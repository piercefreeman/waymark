//! Instance-local runner with lease-based ownership.
//!
//! This runner claims workflow instances and executes them locally. Each instance
//! is owned by a single runner at a time, managed through database leases.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                          InstanceRunner                                  │
//! │                                                                          │
//! │  ┌──────────────────┐     ┌──────────────────┐                          │
//! │  │  Claim Instances │────▶│  ExecutionState  │                          │
//! │  │  (with leases)   │     │  (per instance)  │                          │
//! │  └──────────────────┘     └────────┬─────────┘                          │
//! │                                    │                                     │
//! │  ┌──────────────────┐              │                                     │
//! │  │   Heartbeater    │              ▼                                     │
//! │  │  (lease renewal) │     ┌──────────────────┐                          │
//! │  └──────────────────┘     │ PythonWorkerPool │                          │
//! │                           │ (action dispatch)│                          │
//! │                           └────────┬─────────┘                          │
//! │                                    │                                     │
//! │                                    ▼                                     │
//! │                           ┌──────────────────┐                          │
//! │                           │ Batch Completion │                          │
//! │                           │ (state update)   │                          │
//! │                           └────────┬─────────┘                          │
//! │                                    │                                     │
//! │                                    ▼                                     │
//! │                           ┌──────────────────┐                          │
//! │                           │   DB Persist     │                          │
//! │                           │ (single UPDATE)  │                          │
//! │                           └──────────────────┘                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Lease Management
//!
//! - Default lease duration: 60 seconds
//! - Heartbeat interval: 10 seconds
//! - If a runner crashes, its instances become orphaned after the lease expires
//! - Other runners will pick up orphaned instances automatically

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use chrono::{TimeZone, Utc};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::ast_evaluator::ExpressionEvaluator;
use crate::dag::{DAG, DAGConverter, DAGNode};
use crate::dag_state::DAGHelper;
use crate::db::{
    ClaimedInstance, Database, WorkerStatusUpdate, WorkflowInstanceId, WorkflowVersionId,
};
use crate::execution_graph::{Completion, ExecutionState, SLEEP_WORKER_ID};
use crate::messages::execution::{ExecutionNode, NodeStatus};
use crate::messages::proto;
use crate::messages::proto::WorkflowArguments;
use crate::messages::{MessageError, decode_message, encode_message};
use crate::parser::ast;
use crate::stats::LifecycleStats;
use crate::value::{WorkflowValue, workflow_value_from_proto_bytes, workflow_value_to_proto_bytes};
use crate::worker::{ActionDispatchPayload, PythonWorkerPool};

/// Default lease duration for instance ownership (60 seconds)
pub const DEFAULT_LEASE_SECONDS: i64 = 60;

/// Default heartbeat interval (10 seconds)
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Default batch size for claiming instances
pub const DEFAULT_CLAIM_BATCH_SIZE: i32 = 50;

/// Default completion batch size
pub const DEFAULT_COMPLETION_BATCH_SIZE: usize = 100;

/// Built-in durable sleep action name.
const SLEEP_ACTION_NAME: &str = "sleep";

#[derive(Debug, Error)]
pub enum InstanceRunnerError {
    #[error("Database error: {0}")]
    Database(#[from] crate::db::DbError),

    #[error("Parse error: {0}")]
    Parse(#[from] crate::parser::ParseError),

    #[error("Worker error: {0}")]
    Worker(String),

    #[error("Message error: {0}")]
    Message(#[from] MessageError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] prost::DecodeError),

    #[error("DAG conversion error: {0}")]
    DagConversion(String),

    #[error("Lost lease for instance {0}")]
    LostLease(WorkflowInstanceId),

    #[error("Shutdown requested")]
    Shutdown,
}

impl From<String> for InstanceRunnerError {
    fn from(s: String) -> Self {
        InstanceRunnerError::DagConversion(s)
    }
}

pub type InstanceRunnerResult<T> = Result<T, InstanceRunnerError>;

/// Configuration for the instance runner
#[derive(Debug, Clone)]
pub struct InstanceRunnerConfig {
    /// Unique identifier for this runner (used as owner_id)
    pub runner_id: String,
    /// Unique identifier for the worker pool (used for status reporting)
    pub pool_id: Uuid,
    /// Lease duration in seconds
    pub lease_seconds: i64,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Max instances to claim per DB query (to avoid starving worker pool)
    pub claim_batch_size: i32,
    /// Max instances this runner can hold concurrently
    pub max_concurrent_instances: usize,
    /// Max completions to batch before persisting
    pub completion_batch_size: usize,
    /// How long to wait if no work is available
    pub idle_poll_interval: Duration,
    /// Interval for reporting worker status to DB
    pub status_report_interval: Duration,
}

/// Default max concurrent instances
pub const DEFAULT_MAX_CONCURRENT_INSTANCES: usize = 100;

impl Default for InstanceRunnerConfig {
    fn default() -> Self {
        Self {
            runner_id: Uuid::new_v4().to_string(),
            pool_id: Uuid::new_v4(),
            lease_seconds: DEFAULT_LEASE_SECONDS,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            claim_batch_size: DEFAULT_CLAIM_BATCH_SIZE,
            max_concurrent_instances: DEFAULT_MAX_CONCURRENT_INSTANCES,
            completion_batch_size: DEFAULT_COMPLETION_BATCH_SIZE,
            idle_poll_interval: Duration::from_millis(100),
            status_report_interval: Duration::from_secs(5),
        }
    }
}

/// An active instance being executed by this runner
struct ActiveInstance {
    instance_id: WorkflowInstanceId,
    workflow_name: String,
    dag: Arc<DAG>,
    state: ExecutionState,
    /// Actions currently being executed by workers
    in_flight: HashSet<String>,
    /// Pending completions to be batched
    pending_completions: Vec<Completion>,
}

/// Metrics snapshot for the instance runner
#[derive(Debug, Clone, Default)]
pub struct InstanceRunnerMetrics {
    pub instances_owned: u64,
    pub actions_dispatched: u64,
    pub actions_completed: u64,
    pub instances_completed: u64,
    pub instances_failed: u64,
    pub heartbeats_sent: u64,
    pub orphans_recovered: u64,
}

/// A completion received from a worker
struct WorkerCompletion {
    instance_id: Uuid,
    completion: Completion,
    worker_idx: usize,
}

/// An action queued for dispatch to a worker.
/// Actions are collected from ready nodes and dispatched in FIFO order.
#[derive(Debug)]
struct QueuedAction {
    instance_id: Uuid,
    node_id: String,
    module_name: String,
    action_name: String,
    inputs: proto::WorkflowArguments,
    inputs_bytes: Option<Vec<u8>>,
    timeout_seconds: u32,
    max_retries: u32,
    attempt_number: u32,
}

/// The instance-local runner
pub struct InstanceRunner {
    config: InstanceRunnerConfig,
    db: Database,
    worker_pool: Arc<PythonWorkerPool>,

    /// Active instances owned by this runner
    active_instances: RwLock<HashMap<Uuid, ActiveInstance>>,

    /// Cached DAGs by version ID
    dag_cache: RwLock<HashMap<Uuid, Arc<DAG>>>,

    /// FIFO queue of actions ready to be dispatched to workers.
    /// Actions are enqueued when nodes become ready, and dequeued
    /// based on worker capacity.
    dispatch_queue: Mutex<VecDeque<QueuedAction>>,

    /// Shutdown signal
    shutdown: AtomicBool,

    /// Metrics
    metrics: Mutex<InstanceRunnerMetrics>,

    /// Channel for receiving completions from worker tasks
    completion_tx: mpsc::Sender<WorkerCompletion>,
    completion_rx: Mutex<mpsc::Receiver<WorkerCompletion>>,

    /// Sequence number for action IDs
    action_seq: AtomicU32,

    /// Lifecycle stats
    #[allow(dead_code)]
    stats: Option<Arc<LifecycleStats>>,
}

impl InstanceRunner {
    /// Create a new instance runner
    pub fn new(
        config: InstanceRunnerConfig,
        db: Database,
        worker_pool: Arc<PythonWorkerPool>,
    ) -> Self {
        // Create completion channel with enough capacity for concurrent actions
        let (completion_tx, completion_rx) = mpsc::channel(10000);

        Self {
            config,
            db,
            worker_pool,
            active_instances: RwLock::new(HashMap::new()),
            dag_cache: RwLock::new(HashMap::new()),
            dispatch_queue: Mutex::new(VecDeque::new()),
            shutdown: AtomicBool::new(false),
            metrics: Mutex::new(InstanceRunnerMetrics::default()),
            completion_tx,
            completion_rx: Mutex::new(completion_rx),
            action_seq: AtomicU32::new(0),
            stats: None,
        }
    }

    /// Set lifecycle stats for monitoring
    pub fn with_stats(mut self, stats: Arc<LifecycleStats>) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Request shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Check if shutdown was requested
    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Run the main execution loop
    pub async fn run(&self) -> InstanceRunnerResult<()> {
        info!(runner_id = %self.config.runner_id, "Starting instance runner");

        // Spawn heartbeat task
        let heartbeat_handle = self.spawn_heartbeat_task();

        // Spawn status reporting task
        let status_handle = self.spawn_status_report_task();

        // Main loop
        loop {
            if self.is_shutdown() {
                info!("Shutdown requested, stopping runner");
                break;
            }

            // 1. Claim new instances if we have capacity
            self.claim_instances().await?;

            // 2. Process any completions from workers
            self.process_worker_completions().await?;

            // 3. Check for durable sleep wakeups
            self.process_sleeping_nodes().await?;

            // 4. Collect ready actions from instances → enqueue to dispatch queue
            self.collect_ready_actions().await?;

            // 5. Dispatch from queue: dequeue → mark running → sync DB → send to workers
            self.dispatch_from_queue().await?;

            // 6. Process completed instances
            self.finalize_completed_instances().await?;

            // Small sleep to avoid tight loop when idle
            let active_count = self.active_instances.read().await.len();
            let queue_size = self.dispatch_queue.lock().await.len();
            if active_count == 0 && queue_size == 0 {
                tokio::time::sleep(self.config.idle_poll_interval).await;
            } else {
                // Brief yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
        }

        // Cancel background tasks
        heartbeat_handle.abort();
        status_handle.abort();

        // Release all owned instances
        self.release_all_instances().await?;

        Ok(())
    }

    /// Process completions received from worker tasks
    async fn process_worker_completions(&self) -> InstanceRunnerResult<()> {
        let mut rx = self.completion_rx.lock().await;

        // Drain all available completions without blocking
        loop {
            match rx.try_recv() {
                Ok(worker_completion) => {
                    let mut instances = self.active_instances.write().await;
                    if let Some(instance) = instances.get_mut(&worker_completion.instance_id) {
                        // Remove from in-flight tracking
                        instance
                            .in_flight
                            .remove(&worker_completion.completion.node_id);

                        // Add to pending completions
                        instance
                            .pending_completions
                            .push(worker_completion.completion);

                        // Record completion in worker pool
                        self.worker_pool.record_completion(
                            worker_completion.worker_idx,
                            Arc::clone(&self.worker_pool),
                        );

                        self.metrics.lock().await.actions_completed += 1;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        Ok(())
    }

    async fn process_sleeping_nodes(&self) -> InstanceRunnerResult<()> {
        let now_ms = Utc::now().timestamp_millis();
        let mut instances = self.active_instances.write().await;

        for instance in instances.values_mut() {
            let mut due_completions = Vec::new();

            for (node_id, exec_node) in instance.state.graph.nodes.iter() {
                let status =
                    NodeStatus::try_from(exec_node.status).unwrap_or(NodeStatus::Unspecified);
                if status != NodeStatus::Running {
                    continue;
                }

                let template_id = &exec_node.template_id;
                let is_sleep = instance
                    .dag
                    .nodes
                    .get(template_id)
                    .and_then(|node| node.action_name.as_deref())
                    .map(|name| name == SLEEP_ACTION_NAME)
                    .unwrap_or(false);

                if !is_sleep {
                    continue;
                }

                let Some(wakeup_ms) = Self::sleep_wakeup_time_ms(exec_node) else {
                    continue;
                };
                if now_ms < wakeup_ms {
                    continue;
                }

                let duration_ms = exec_node
                    .started_at_ms
                    .map(|started| (now_ms - started).max(0))
                    .unwrap_or(0);

                due_completions.push(Completion {
                    node_id: node_id.clone(),
                    success: true,
                    result: Some(Self::build_null_result_payload()),
                    error: None,
                    error_type: None,
                    worker_id: SLEEP_WORKER_ID.to_string(),
                    duration_ms,
                    worker_duration_ms: Some(duration_ms), // Sleep duration is the actual execution time
                });
            }

            if !due_completions.is_empty() {
                for completion in due_completions {
                    instance.in_flight.remove(&completion.node_id);
                    instance.pending_completions.push(completion);
                    self.metrics.lock().await.actions_completed += 1;
                }
            }
        }

        Ok(())
    }

    /// Spawn the heartbeat background task
    fn spawn_heartbeat_task(&self) -> tokio::task::JoinHandle<()> {
        let db = self.db.clone();
        let runner_id = self.config.runner_id.clone();
        let lease_seconds = self.config.lease_seconds;
        let heartbeat_interval = self.config.heartbeat_interval;
        let shutdown = &self.shutdown as *const AtomicBool;

        // Safety: We know self lives as long as the returned handle
        let shutdown_ptr = unsafe { &*shutdown };

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if shutdown_ptr.load(Ordering::SeqCst) {
                    break;
                }

                match db.heartbeat_instances(&runner_id, lease_seconds).await {
                    Ok(count) => {
                        if count > 0 {
                            debug!(count = count, "Heartbeat extended leases");
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to send heartbeat");
                    }
                }
            }
        })
    }

    /// Spawn the worker status report background task.
    ///
    /// Periodically reports worker pool metrics to the database including:
    /// - Per-worker throughput and completion counts
    /// - Dispatch queue size (pool-level)
    /// - Total in-flight actions (pool-level)
    fn spawn_status_report_task(&self) -> tokio::task::JoinHandle<()> {
        let db = self.db.clone();
        let pool_id = self.config.pool_id;
        let status_interval = self.config.status_report_interval;
        let worker_pool = Arc::clone(&self.worker_pool);
        let dispatch_queue = &self.dispatch_queue as *const Mutex<VecDeque<QueuedAction>>;
        let shutdown = &self.shutdown as *const AtomicBool;

        // Safety: We know self lives as long as the returned handle
        let shutdown_ptr = unsafe { &*shutdown };
        let dispatch_queue_ptr = unsafe { &*dispatch_queue };

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(status_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if shutdown_ptr.load(Ordering::SeqCst) {
                    break;
                }

                // Get dispatch queue size
                let dispatch_queue_size = dispatch_queue_ptr.lock().await.len() as i64;

                // Get total in-flight from worker pool
                let total_in_flight = worker_pool.total_in_flight() as i64;

                // Get per-worker throughput snapshots
                let snapshots = worker_pool.throughput_snapshots();

                // Convert to WorkerStatusUpdate
                let statuses: Vec<WorkerStatusUpdate> = snapshots
                    .into_iter()
                    .map(|snapshot| WorkerStatusUpdate {
                        worker_id: snapshot.worker_id as i64,
                        throughput_per_min: snapshot.throughput_per_min,
                        total_completed: snapshot.total_completed as i64,
                        last_action_at: snapshot.last_action_at,
                        median_dequeue_ms: None, // TODO: track dequeue latency
                        median_handling_ms: None, // TODO: track handling latency
                        dispatch_queue_size,
                        total_in_flight,
                    })
                    .collect();

                if !statuses.is_empty() {
                    match db.upsert_worker_statuses(pool_id, &statuses).await {
                        Ok(()) => {
                            debug!(
                                worker_count = statuses.len(),
                                dispatch_queue_size, total_in_flight, "Reported worker status"
                            );
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to report worker status");
                        }
                    }
                }
            }
        })
    }

    /// Claim new instances from the database.
    ///
    /// Loops claiming batches until we reach `max_concurrent_instances` or
    /// no more instances are available. Each batch is limited to
    /// `min(spots_available, claim_batch_size)` to avoid long DB queries
    /// that could starve the worker pool.
    async fn claim_instances(&self) -> InstanceRunnerResult<()> {
        loop {
            let current_count = self.active_instances.read().await.len();
            let max_instances = self.config.max_concurrent_instances;

            if current_count >= max_instances {
                break;
            }

            let spots_available = max_instances - current_count;
            let batch_size = self.config.claim_batch_size as usize;
            let to_claim = spots_available.min(batch_size) as i32;

            let claimed = self
                .db
                .claim_instances_batch(&self.config.runner_id, self.config.lease_seconds, to_claim)
                .await?;

            if claimed.is_empty() {
                // No more instances available
                break;
            }

            let claimed_count = claimed.len();
            for instance in claimed {
                if let Err(e) = self.initialize_instance(instance).await {
                    error!(error = %e, "Failed to initialize claimed instance");
                }
            }

            // If we got fewer than requested, no point trying again
            if claimed_count < to_claim as usize {
                break;
            }
        }

        Ok(())
    }

    /// Initialize a claimed instance
    async fn initialize_instance(&self, claimed: ClaimedInstance) -> InstanceRunnerResult<()> {
        let version_id = claimed
            .workflow_version_id
            .ok_or_else(|| InstanceRunnerError::Worker("Instance has no version".into()))?;

        // Load or get cached DAG
        let dag = self.get_or_load_dag(version_id).await?;

        // Initialize or restore execution state
        let state = match claimed.execution_graph {
            Some(bytes) => {
                // Restore from persisted state
                let mut state = ExecutionState::from_bytes(&bytes)?;

                // Recover any nodes that were running when previous owner crashed
                state.recover_running_nodes();

                self.metrics.lock().await.orphans_recovered += 1;
                state
            }
            None => {
                // New instance - initialize from DAG and inputs
                let mut state = ExecutionState::new();

                let inputs: WorkflowArguments = claimed
                    .input_payload
                    .as_ref()
                    .map(|b| decode_message(b))
                    .transpose()?
                    .unwrap_or_default();

                state.initialize_from_dag(&dag, &inputs);
                state
            }
        };

        let active = ActiveInstance {
            instance_id: claimed.id,
            workflow_name: claimed.workflow_name,
            dag,
            state,
            in_flight: HashSet::new(),
            pending_completions: Vec::new(),
        };

        self.active_instances
            .write()
            .await
            .insert(claimed.id.0, active);

        self.metrics.lock().await.instances_owned += 1;

        debug!(
            instance_id = %claimed.id,
            "Initialized instance"
        );

        Ok(())
    }

    /// Get or load a DAG from cache
    async fn get_or_load_dag(
        &self,
        version_id: WorkflowVersionId,
    ) -> InstanceRunnerResult<Arc<DAG>> {
        // Check cache first
        {
            let cache = self.dag_cache.read().await;
            if let Some(dag) = cache.get(&version_id.0) {
                return Ok(Arc::clone(dag));
            }
        }

        // Load from database
        let version = self.db.get_workflow_version(version_id).await?;
        let program: ast::Program = decode_message(&version.program_proto)?;
        let dag = DAGConverter::new().convert(&program)?;

        // Cache it
        let dag = Arc::new(dag);
        self.dag_cache
            .write()
            .await
            .insert(version_id.0, Arc::clone(&dag));

        Ok(dag)
    }

    /// Collect ready actions from all active instances and enqueue them.
    ///
    /// Inline nodes (spreads, sleeps, control flow) are handled immediately and
    /// removed from the ready_queue. Worker actions are added to the dispatch
    /// queue but REMAIN in ready_queue until actually dispatched - this ensures
    /// crash recovery can find them.
    async fn collect_ready_actions(&self) -> InstanceRunnerResult<()> {
        let mut instances = self.active_instances.write().await;
        let mut queue = self.dispatch_queue.lock().await;

        for (_, instance) in instances.iter_mut() {
            // Peek at ready nodes without draining - we'll selectively remove them
            // Worker actions stay in ready_queue until dispatch (for crash recovery)
            let ready_nodes = instance.state.peek_ready_queue();

            for node_id in ready_nodes {
                // Skip if already in flight
                if instance.in_flight.contains(&node_id) {
                    continue;
                }

                // Skip if already in dispatch queue (avoid duplicates)
                if queue.iter().any(|a| a.node_id == node_id) {
                    continue;
                }

                // Resolve the execution node + DAG template node
                let exec_node = match instance.state.graph.nodes.get(&node_id) {
                    Some(n) => n.clone(),
                    None => {
                        warn!(node_id = %node_id, "Ready node not found in execution graph");
                        // Remove invalid node from ready_queue
                        instance.state.remove_from_ready_queue(&node_id);
                        continue;
                    }
                };
                let template_id = exec_node.template_id.clone();
                let dag_node = match instance.dag.nodes.get(&template_id) {
                    Some(n) => n.clone(),
                    None => {
                        warn!(node_id = %node_id, template_id = %template_id, "Ready node not found in DAG");
                        // Remove invalid node from ready_queue
                        instance.state.remove_from_ready_queue(&node_id);
                        continue;
                    }
                };

                // Handle spread nodes inline - remove from ready_queue immediately
                if dag_node.is_spread && exec_node.spread_index.is_none() {
                    // Remove from ready_queue since we're handling it now
                    instance.state.remove_from_ready_queue(&node_id);

                    let mut completion_error = None;
                    let items = match dag_node.spread_collection_expr.as_ref() {
                        Some(expr) => {
                            match ExpressionEvaluator::evaluate(
                                expr,
                                &instance.state.build_scope_for_node(&node_id),
                            ) {
                                Ok(WorkflowValue::List(items))
                                | Ok(WorkflowValue::Tuple(items)) => items,
                                Ok(WorkflowValue::Null) => Vec::new(),
                                Ok(other) => {
                                    completion_error = Some(format!(
                                        "Spread collection must be list or tuple, got {:?}",
                                        other
                                    ));
                                    Vec::new()
                                }
                                Err(e) => {
                                    completion_error =
                                        Some(format!("Spread collection evaluation error: {}", e));
                                    Vec::new()
                                }
                            }
                        }
                        None => {
                            completion_error =
                                Some("Spread node missing collection expression".to_string());
                            Vec::new()
                        }
                    };

                    if completion_error.is_none() {
                        instance
                            .state
                            .expand_spread(&template_id, items, &instance.dag);
                    }

                    let completion = Completion {
                        node_id: node_id.clone(),
                        success: completion_error.is_none(),
                        result: None,
                        error: completion_error.clone(),
                        error_type: completion_error
                            .as_ref()
                            .map(|_| "SpreadEvaluationError".to_string()),
                        worker_id: "inline".to_string(),
                        duration_ms: 0,
                        worker_duration_ms: Some(0),
                    };
                    instance.pending_completions.push(completion);
                    continue;
                }

                // Handle durable sleep actions inline - schedule_sleep_action calls mark_running
                // which removes from ready_queue
                if dag_node.action_name.as_deref() == Some(SLEEP_ACTION_NAME) {
                    if let Err(e) = self
                        .schedule_sleep_action(instance, &node_id, &dag_node)
                        .await
                    {
                        warn!(node_id = %node_id, error = %e, "Failed to schedule sleep action");
                    }
                    continue;
                }

                // Check if this is a worker action - enqueue it
                // NOTE: We do NOT remove from ready_queue here - that happens in
                // dispatch_from_queue when we call mark_running. This ensures crash
                // recovery can find pending actions that were queued but not dispatched.
                if let (Some(module_name), Some(action_name)) =
                    (dag_node.module_name.clone(), dag_node.action_name.clone())
                {
                    let inputs_bytes = instance.state.get_inputs_for_node(&node_id, &instance.dag);
                    let inputs: proto::WorkflowArguments = inputs_bytes
                        .as_ref()
                        .and_then(|bytes| decode_message(bytes).ok())
                        .unwrap_or_default();

                    let timeout_seconds = instance.state.get_timeout_seconds(&node_id);
                    let max_retries = instance.state.get_max_retries(&node_id);
                    let attempt_number = instance.state.get_attempt_number(&node_id);

                    queue.push_back(QueuedAction {
                        instance_id: instance.instance_id.0,
                        node_id: node_id.clone(),
                        module_name,
                        action_name,
                        inputs,
                        inputs_bytes,
                        timeout_seconds,
                        max_retries,
                        attempt_number,
                    });
                } else {
                    // Inline node - execute locally, remove from ready_queue
                    instance.state.remove_from_ready_queue(&node_id);

                    let result = self.execute_inline_node(instance, &node_id, &dag_node);

                    let completion = Completion {
                        node_id: node_id.clone(),
                        success: result.is_ok(),
                        result: result.ok(),
                        error: None,
                        error_type: None,
                        worker_id: "inline".to_string(),
                        duration_ms: 0,
                        worker_duration_ms: Some(0),
                    };
                    instance.pending_completions.push(completion);
                }
            }
        }

        Ok(())
    }

    /// Dispatch actions from the queue to workers.
    ///
    /// Flow:
    /// 1. Check worker capacity
    /// 2. Dequeue actions up to available capacity
    /// 3. Mark as Running in in-memory DAG state
    /// 4. Sync state to DB (so crash = we know action was attempted)
    /// 5. Send to workers
    async fn dispatch_from_queue(&self) -> InstanceRunnerResult<()> {
        let available = self.worker_pool.available_capacity();
        if available == 0 {
            return Ok(());
        }

        // Dequeue actions up to available capacity
        let actions_to_dispatch: Vec<QueuedAction> = {
            let mut queue = self.dispatch_queue.lock().await;
            let count = available.min(queue.len());
            queue.drain(..count).collect()
        };

        if actions_to_dispatch.is_empty() {
            return Ok(());
        }

        // Group actions by instance for efficient updates
        let mut by_instance: HashMap<Uuid, Vec<&QueuedAction>> = HashMap::new();
        for action in &actions_to_dispatch {
            by_instance
                .entry(action.instance_id)
                .or_default()
                .push(action);
        }

        // Phase 1: Mark as Running in in-memory state
        {
            let mut instances = self.active_instances.write().await;
            for (instance_id, actions) in &by_instance {
                if let Some(instance) = instances.get_mut(instance_id) {
                    for action in actions {
                        // Acquire a worker slot
                        let worker_idx = match self.worker_pool.try_acquire_slot() {
                            Some(idx) => idx,
                            None => {
                                // This shouldn't happen since we checked capacity, but handle it
                                warn!(
                                    node_id = %action.node_id,
                                    "No worker slot available during dispatch"
                                );
                                continue;
                            }
                        };
                        let worker_id = format!("worker-{}", worker_idx);

                        instance.state.mark_running(
                            &action.node_id,
                            &worker_id,
                            action.inputs_bytes.clone(),
                        );
                        instance.in_flight.insert(action.node_id.clone());
                    }
                }
            }
        }

        // Phase 2: Sync state to DB (before sending to workers)
        // This ensures if we crash, the DB shows the actions were attempted
        {
            let instances = self.active_instances.read().await;
            for instance_id in by_instance.keys() {
                if let Some(instance) = instances.get(instance_id) {
                    let graph_bytes = instance.state.to_bytes();
                    let next_wakeup = instance
                        .state
                        .graph
                        .next_wakeup_time
                        .map(|ms| Utc.timestamp_millis_opt(ms).unwrap());

                    match self
                        .db
                        .update_execution_graph(
                            instance.instance_id,
                            &self.config.runner_id,
                            &graph_bytes,
                            next_wakeup,
                        )
                        .await
                    {
                        Ok(true) => {
                            debug!(
                                instance_id = %instance.instance_id,
                                "Synced running state to DB before dispatch"
                            );
                        }
                        Ok(false) => {
                            warn!(
                                instance_id = %instance.instance_id,
                                "Lost lease while syncing state before dispatch"
                            );
                            // TODO: Handle lost lease - re-enqueue actions
                        }
                        Err(e) => {
                            error!(
                                instance_id = %instance.instance_id,
                                error = %e,
                                "Failed to sync state to DB before dispatch"
                            );
                        }
                    }
                }
            }
        }

        // Phase 3: Actually send to workers
        for action in actions_to_dispatch {
            let instances = self.active_instances.read().await;
            if !instances.contains_key(&action.instance_id) {
                continue;
            }

            // Get worker (already acquired slot above)
            let worker_idx = self.worker_pool.next_worker_idx() % self.worker_pool.len();
            let worker = self.worker_pool.get_worker(worker_idx).await;
            let worker_id = format!("worker-{}", worker_idx);

            let action_seq = self.action_seq.fetch_add(1, Ordering::SeqCst);
            let dispatch_token = Uuid::new_v4();

            let payload = ActionDispatchPayload {
                action_id: action.node_id.clone(),
                instance_id: action.instance_id.to_string(),
                sequence: action_seq,
                action_name: action.action_name.clone(),
                module_name: action.module_name.clone(),
                kwargs: action.inputs.clone(),
                timeout_seconds: action.timeout_seconds,
                max_retries: action.max_retries,
                attempt_number: action.attempt_number,
                dispatch_token,
            };

            self.metrics.lock().await.actions_dispatched += 1;

            // Spawn task to send and handle completion
            let completion_tx = self.completion_tx.clone();
            let instance_id = action.instance_id;
            let node_id = action.node_id.clone();
            let worker_pool = Arc::clone(&self.worker_pool);
            let worker_id_for_task = worker_id.clone();

            tokio::spawn(async move {
                match worker.send_action(payload).await {
                    Ok(metrics) => {
                        let completion = Completion {
                            node_id: node_id.clone(),
                            success: metrics.success,
                            result: if metrics.response_payload.is_empty() {
                                None
                            } else {
                                Some(metrics.response_payload)
                            },
                            error: metrics.error_message.clone(),
                            error_type: metrics.error_type.clone(),
                            worker_id: worker_id_for_task.clone(),
                            duration_ms: metrics.round_trip.as_millis() as i64,
                            worker_duration_ms: Some(metrics.worker_duration.as_millis() as i64),
                        };

                        if let Err(e) = completion_tx
                            .send(WorkerCompletion {
                                instance_id,
                                completion,
                                worker_idx,
                            })
                            .await
                        {
                            error!(node_id = %node_id, error = %e, "Failed to send completion");
                        }
                    }
                    Err(e) => {
                        error!(node_id = %node_id, error = %e, "Worker action failed");

                        let completion = Completion {
                            node_id: node_id.clone(),
                            success: false,
                            result: None,
                            error: Some(format!("Worker error: {}", e)),
                            error_type: Some("WorkerError".to_string()),
                            worker_id: worker_id_for_task.clone(),
                            duration_ms: 0,
                            worker_duration_ms: None,
                        };

                        let _ = completion_tx
                            .send(WorkerCompletion {
                                instance_id,
                                completion,
                                worker_idx,
                            })
                            .await;
                    }
                }

                // Record completion in worker pool (releases slot, tracks throughput)
                worker_pool.record_completion(worker_idx, Arc::clone(&worker_pool));
            });

            debug!(
                instance_id = %action.instance_id,
                node_id = %action.node_id,
                worker_id = %worker_id,
                "Dispatched action to worker"
            );
        }

        Ok(())
    }

    async fn schedule_sleep_action(
        &self,
        instance: &mut ActiveInstance,
        node_id: &str,
        _dag_node: &DAGNode,
    ) -> InstanceRunnerResult<()> {
        let inputs_bytes = instance.state.get_inputs_for_node(node_id, &instance.dag);
        let inputs = Self::decode_workflow_arguments(inputs_bytes.as_deref());
        let duration_ms = Self::sleep_duration_ms_from_args(&inputs);

        if duration_ms <= 0 {
            let completion = Completion {
                node_id: node_id.to_string(),
                success: true,
                result: Some(Self::build_null_result_payload()),
                error: None,
                error_type: None,
                worker_id: SLEEP_WORKER_ID.to_string(),
                duration_ms: 0,
                worker_duration_ms: Some(0), // Instant sleep (duration <= 0)
            };
            instance.pending_completions.push(completion);
            return Ok(());
        }

        instance
            .state
            .mark_running(node_id, SLEEP_WORKER_ID, inputs_bytes);

        // For sleep nodes, we need to set started_at_ms immediately so the wakeup
        // time can be calculated. Unlike worker actions, sleeps don't report duration.
        let now_ms = Utc::now().timestamp_millis();
        if let Some(node) = instance.state.graph.nodes.get_mut(node_id) {
            node.started_at_ms = Some(now_ms);
        }

        instance.in_flight.insert(node_id.to_string());
        self.metrics.lock().await.actions_dispatched += 1;

        Ok(())
    }

    fn decode_workflow_arguments(bytes: Option<&[u8]>) -> WorkflowArguments {
        bytes
            .and_then(|b| decode_message(b).ok())
            .unwrap_or_default()
    }

    fn sleep_duration_ms_from_args(inputs: &WorkflowArguments) -> i64 {
        let mut duration_secs: Option<f64> = None;
        for arg in &inputs.arguments {
            if arg.key != "duration" && arg.key != "seconds" {
                continue;
            }
            let Some(value) = arg.value.as_ref() else {
                continue;
            };
            match WorkflowValue::from_proto(value) {
                WorkflowValue::Int(i) => {
                    duration_secs = Some(i as f64);
                }
                WorkflowValue::Float(f) => {
                    duration_secs = Some(f);
                }
                WorkflowValue::String(s) => {
                    if let Ok(parsed) = s.parse::<f64>() {
                        duration_secs = Some(parsed);
                    }
                }
                _ => {}
            }
            break;
        }

        let secs = duration_secs.unwrap_or(0.0);
        if secs <= 0.0 {
            0
        } else {
            (secs * 1000.0).ceil() as i64
        }
    }

    fn sleep_wakeup_time_ms(exec_node: &ExecutionNode) -> Option<i64> {
        let started_at_ms = exec_node.started_at_ms?;
        let inputs = Self::decode_workflow_arguments(exec_node.inputs.as_deref());
        let duration_ms = Self::sleep_duration_ms_from_args(&inputs);
        Some(started_at_ms + duration_ms)
    }

    fn sleep_wakeup_datetime_ms(next_wakeup_ms: Option<i64>) -> Option<chrono::DateTime<Utc>> {
        next_wakeup_ms.and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    }

    fn build_null_result_payload() -> Vec<u8> {
        let args = WorkflowArguments {
            arguments: vec![proto::WorkflowArgument {
                key: "result".to_string(),
                value: Some(WorkflowValue::Null.to_proto()),
            }],
        };
        encode_message(&args)
    }

    fn refresh_sleep_state(instance: &mut ActiveInstance) -> bool {
        let has_ready = !instance.state.graph.ready_queue.is_empty();
        let has_pending_completions = !instance.pending_completions.is_empty();
        let mut has_non_sleep_running = false;
        let mut sleep_wakeups: Vec<i64> = Vec::new();

        for exec_node in instance.state.graph.nodes.values() {
            let status = NodeStatus::try_from(exec_node.status).unwrap_or(NodeStatus::Unspecified);
            if status != NodeStatus::Running {
                continue;
            }

            let template_id = &exec_node.template_id;
            let is_sleep = instance
                .dag
                .nodes
                .get(template_id)
                .and_then(|node| node.action_name.as_deref())
                .map(|name| name == SLEEP_ACTION_NAME)
                .unwrap_or(false);

            if is_sleep {
                if let Some(wakeup) = Self::sleep_wakeup_time_ms(exec_node) {
                    sleep_wakeups.push(wakeup);
                }
            } else {
                has_non_sleep_running = true;
            }
        }

        if has_ready || has_pending_completions || has_non_sleep_running || sleep_wakeups.is_empty()
        {
            instance.state.graph.next_wakeup_time = None;
            return false;
        }

        if let Some(next_wakeup) = sleep_wakeups.into_iter().min() {
            instance.state.graph.next_wakeup_time = Some(next_wakeup);
            true
        } else {
            instance.state.graph.next_wakeup_time = None;
            false
        }
    }

    /// Execute an inline node (assignment, branch, join) locally without sending to a worker.
    /// Returns the result bytes to store if successful.
    fn execute_inline_node(
        &self,
        instance: &mut ActiveInstance,
        node_id: &str,
        dag_node: &DAGNode,
    ) -> Result<Vec<u8>, String> {
        // Build scope for expression evaluation
        let scope = instance.state.build_scope_for_node(node_id);

        match dag_node.node_type.as_str() {
            "assignment" | "fn_call" => {
                // Execute the assignment expression
                if let Some(assign_expr) = &dag_node.assign_expr {
                    match ExpressionEvaluator::evaluate(assign_expr, &scope) {
                        Ok(value) => {
                            let result_bytes = workflow_value_to_proto_bytes(&value);

                            // Store result in variables for each target
                            if let Some(targets) = &dag_node.targets {
                                if targets.len() > 1 {
                                    match &value {
                                        WorkflowValue::Tuple(items)
                                        | WorkflowValue::List(items) => {
                                            for (target, item) in targets.iter().zip(items.iter()) {
                                                instance.state.store_variable_for_node(
                                                    node_id,
                                                    &dag_node.node_type,
                                                    target,
                                                    item,
                                                );
                                                debug!(
                                                    node_id = %node_id,
                                                    target = %target,
                                                    "Stored assignment result"
                                                );
                                            }
                                        }
                                        _ => {
                                            warn!(
                                                node_id = %node_id,
                                                targets = ?targets,
                                                value = ?value,
                                                "Assignment value is not iterable for tuple unpacking"
                                            );
                                            for target in targets {
                                                instance.state.store_variable_for_node(
                                                    node_id,
                                                    &dag_node.node_type,
                                                    target,
                                                    &value,
                                                );
                                            }
                                        }
                                    }
                                } else {
                                    for target in targets {
                                        instance.state.store_variable_for_node(
                                            node_id,
                                            &dag_node.node_type,
                                            target,
                                            &value,
                                        );
                                        debug!(
                                            node_id = %node_id,
                                            target = %target,
                                            "Stored assignment result"
                                        );
                                    }
                                }
                            } else if let Some(target) = &dag_node.target {
                                instance.state.store_variable_for_node(
                                    node_id,
                                    &dag_node.node_type,
                                    target,
                                    &value,
                                );
                                debug!(
                                    node_id = %node_id,
                                    target = %target,
                                    "Stored assignment result"
                                );
                            }

                            Ok(result_bytes)
                        }
                        Err(e) => {
                            warn!(
                                node_id = %node_id,
                                error = %e,
                                "Failed to evaluate assignment expression"
                            );
                            Err(format!("Assignment evaluation error: {}", e))
                        }
                    }
                } else {
                    // No expression - just return empty result
                    Ok(vec![])
                }
            }
            "branch" | "if" | "elif" => {
                // Branch nodes don't produce a result - guard evaluation happens in apply_completions_batch
                debug!(node_id = %node_id, "Branch node completed");
                Ok(vec![])
            }
            "join" | "else" => {
                // Join nodes don't produce a result themselves - they just synchronize
                debug!(node_id = %node_id, "Join node completed");
                Ok(vec![])
            }
            "aggregator" => {
                let helper = DAGHelper::new(&instance.dag);
                let exec_node = instance.state.graph.nodes.get(node_id);
                let source_is_spread = dag_node
                    .aggregates_from
                    .as_ref()
                    .and_then(|id| instance.dag.nodes.get(id))
                    .map(|n| n.is_spread)
                    .unwrap_or(false);

                let source_ids: Vec<String> = if source_is_spread {
                    exec_node.map(|n| n.waiting_for.clone()).unwrap_or_default()
                } else if let Some(exec_node) = exec_node
                    && !exec_node.waiting_for.is_empty()
                {
                    exec_node.waiting_for.clone()
                } else {
                    helper
                        .get_incoming_edges(&dag_node.id)
                        .iter()
                        .filter(|edge| {
                            edge.edge_type == crate::dag::EdgeType::StateMachine
                                && edge.exception_types.is_none()
                        })
                        .map(|edge| edge.source.clone())
                        .collect()
                };

                let mut values = Vec::new();
                for source_id in source_ids {
                    if let Some(source_node) = instance.state.graph.nodes.get(&source_id) {
                        if let Some(result_bytes) = &source_node.result {
                            let value =
                                extract_result_value(result_bytes).unwrap_or(WorkflowValue::Null);
                            values.push(value);
                        } else {
                            values.push(WorkflowValue::Null);
                        }
                    }
                }

                let should_store_list = dag_node
                    .targets
                    .as_ref()
                    .map(|targets| targets.len() == 1)
                    .unwrap_or(false);

                if should_store_list {
                    let list_value = WorkflowValue::List(values);
                    let args = WorkflowArguments {
                        arguments: vec![proto::WorkflowArgument {
                            key: "result".to_string(),
                            value: Some(list_value.to_proto()),
                        }],
                    };
                    Ok(encode_message(&args))
                } else {
                    Ok(encode_message(&WorkflowArguments { arguments: vec![] }))
                }
            }
            "input" | "output" => {
                // Input/output boundary nodes
                debug!(node_id = %node_id, node_type = %dag_node.node_type, "Boundary node completed");
                Ok(vec![])
            }
            "return" => {
                // Return nodes evaluate their expression and store in "result" variable
                if let Some(assign_expr) = &dag_node.assign_expr {
                    match ExpressionEvaluator::evaluate(assign_expr, &scope) {
                        Ok(value) => {
                            let result_bytes = workflow_value_to_proto_bytes(&value);

                            // Store in target (should be "result")
                            if let Some(target) = &dag_node.target {
                                instance.state.store_variable_for_node(
                                    node_id,
                                    &dag_node.node_type,
                                    target,
                                    &value,
                                );
                                debug!(
                                    node_id = %node_id,
                                    target = %target,
                                    "Stored return value"
                                );
                            }

                            Ok(result_bytes)
                        }
                        Err(e) => {
                            warn!(
                                node_id = %node_id,
                                error = %e,
                                "Failed to evaluate return expression"
                            );
                            Err(format!("Return evaluation error: {}", e))
                        }
                    }
                } else {
                    // No expression - return None
                    debug!(node_id = %node_id, "Return node with no expression");
                    Ok(vec![])
                }
            }
            _ => {
                // Unknown node type - just mark as completed
                debug!(
                    node_id = %node_id,
                    node_type = %dag_node.node_type,
                    "Unknown inline node type completed"
                );
                Ok(vec![])
            }
        }
    }

    /// Finalize completed instances
    async fn finalize_completed_instances(&self) -> InstanceRunnerResult<()> {
        let mut completed_ids = Vec::new();
        let mut released_ids = Vec::new();
        let mut lost_lease_ids = Vec::new();

        {
            let mut instances = self.active_instances.write().await;

            for (id, instance) in instances.iter_mut() {
                // Apply any remaining completions
                if !instance.pending_completions.is_empty() {
                    let completions = std::mem::take(&mut instance.pending_completions);
                    let result = instance
                        .state
                        .apply_completions_batch(completions, &instance.dag);

                    if result.workflow_completed {
                        // Persist completed state
                        let graph_bytes = instance.state.to_bytes();
                        let success = self
                            .db
                            .complete_instance_with_graph(
                                instance.instance_id,
                                &self.config.runner_id,
                                result.result_payload.as_deref(),
                                &graph_bytes,
                            )
                            .await?;

                        if success {
                            completed_ids.push(*id);
                            self.metrics.lock().await.instances_completed += 1;
                            info!(
                                instance_id = %instance.instance_id,
                                workflow = %instance.workflow_name,
                                "Instance completed successfully"
                            );
                        }
                    } else if result.workflow_failed {
                        // Persist failed state
                        let graph_bytes = instance.state.to_bytes();
                        let success = self
                            .db
                            .fail_instance_with_graph(
                                instance.instance_id,
                                &self.config.runner_id,
                                result.result_payload.as_deref(),
                                &graph_bytes,
                            )
                            .await?;

                        if success {
                            completed_ids.push(*id);
                            self.metrics.lock().await.instances_failed += 1;
                            warn!(
                                instance_id = %instance.instance_id,
                                workflow = %instance.workflow_name,
                                error = ?result.error_message,
                                "Instance failed"
                            );
                        }
                    }
                }

                let previous_wakeup = instance.state.graph.next_wakeup_time;
                let fully_sleeping = Self::refresh_sleep_state(instance);
                let next_wakeup =
                    Self::sleep_wakeup_datetime_ms(instance.state.graph.next_wakeup_time);

                if fully_sleeping {
                    let graph_bytes = instance.state.to_bytes();
                    let released = self
                        .db
                        .release_instance(
                            instance.instance_id,
                            &self.config.runner_id,
                            &graph_bytes,
                            next_wakeup,
                        )
                        .await?;
                    if released {
                        released_ids.push(*id);
                    }
                    continue;
                }

                // Check if instance is done (no pending work)
                if !instance.state.has_pending_work() && instance.in_flight.is_empty() {
                    // Persist current state
                    let graph_bytes = instance.state.to_bytes();
                    match self
                        .db
                        .update_execution_graph(
                            instance.instance_id,
                            &self.config.runner_id,
                            &graph_bytes,
                            next_wakeup,
                        )
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            warn!(
                                instance_id = %instance.instance_id,
                                "Lost lease while persisting execution graph"
                            );
                            lost_lease_ids.push(*id);
                        }
                        Err(err) => {
                            error!(
                                instance_id = %instance.instance_id,
                                error = %err,
                                "Failed to persist execution graph"
                            );
                        }
                    }
                } else if instance.state.graph.next_wakeup_time != previous_wakeup {
                    let graph_bytes = instance.state.to_bytes();
                    match self
                        .db
                        .update_execution_graph(
                            instance.instance_id,
                            &self.config.runner_id,
                            &graph_bytes,
                            next_wakeup,
                        )
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            warn!(
                                instance_id = %instance.instance_id,
                                "Lost lease while updating wakeup time"
                            );
                            lost_lease_ids.push(*id);
                        }
                        Err(err) => {
                            error!(
                                instance_id = %instance.instance_id,
                                error = %err,
                                "Failed to update wakeup time"
                            );
                        }
                    }
                }
            }
        }

        // Remove completed instances
        let mut instances = self.active_instances.write().await;
        for id in completed_ids {
            instances.remove(&id);
        }
        for id in released_ids {
            instances.remove(&id);
        }
        for id in lost_lease_ids {
            instances.remove(&id);
        }

        Ok(())
    }

    /// Release all owned instances (on shutdown)
    async fn release_all_instances(&self) -> InstanceRunnerResult<()> {
        let instances = std::mem::take(&mut *self.active_instances.write().await);

        for (_, mut instance) in instances {
            Self::refresh_sleep_state(&mut instance);
            let next_wakeup = Self::sleep_wakeup_datetime_ms(instance.state.graph.next_wakeup_time);
            let graph_bytes = instance.state.to_bytes();
            if let Err(e) = self
                .db
                .release_instance(
                    instance.instance_id,
                    &self.config.runner_id,
                    &graph_bytes,
                    next_wakeup,
                )
                .await
            {
                error!(
                    instance_id = %instance.instance_id,
                    error = %e,
                    "Failed to release instance"
                );
            } else {
                debug!(instance_id = %instance.instance_id, "Released instance");
            }
        }

        Ok(())
    }

    /// Get current metrics
    pub async fn metrics(&self) -> InstanceRunnerMetrics {
        self.metrics.lock().await.clone()
    }
}

fn extract_result_value(result_bytes: &[u8]) -> Option<WorkflowValue> {
    if let Ok(args) = decode_message::<WorkflowArguments>(result_bytes) {
        for arg in args.arguments {
            if arg.key == "result" {
                if let Some(value) = arg.value {
                    return Some(WorkflowValue::from_proto(&value));
                }
                return None;
            }
        }
    }

    workflow_value_from_proto_bytes(result_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = InstanceRunnerConfig::default();
        assert_eq!(config.lease_seconds, DEFAULT_LEASE_SECONDS);
        assert_eq!(config.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL);
        assert_eq!(config.claim_batch_size, DEFAULT_CLAIM_BATCH_SIZE);
        assert_eq!(config.completion_batch_size, DEFAULT_COMPLETION_BATCH_SIZE);
        assert_eq!(config.idle_poll_interval, Duration::from_millis(100));
        // runner_id should be a valid UUID
        assert!(!config.runner_id.is_empty());
    }

    #[test]
    fn test_config_custom_values() {
        let config = InstanceRunnerConfig {
            runner_id: "custom-runner-123".to_string(),
            pool_id: Uuid::new_v4(),
            lease_seconds: 120,
            heartbeat_interval: Duration::from_secs(20),
            claim_batch_size: 50,
            max_concurrent_instances: 200,
            completion_batch_size: 200,
            idle_poll_interval: Duration::from_millis(500),
            status_report_interval: Duration::from_secs(10),
        };
        assert_eq!(config.runner_id, "custom-runner-123");
        assert_eq!(config.lease_seconds, 120);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(20));
        assert_eq!(config.claim_batch_size, 50);
        assert_eq!(config.max_concurrent_instances, 200);
        assert_eq!(config.completion_batch_size, 200);
    }

    #[test]
    fn test_metrics_default() {
        let metrics = InstanceRunnerMetrics::default();
        assert_eq!(metrics.instances_owned, 0);
        assert_eq!(metrics.actions_dispatched, 0);
        assert_eq!(metrics.actions_completed, 0);
        assert_eq!(metrics.instances_completed, 0);
        assert_eq!(metrics.instances_failed, 0);
        assert_eq!(metrics.heartbeats_sent, 0);
        assert_eq!(metrics.orphans_recovered, 0);
    }

    #[test]
    fn test_error_from_string() {
        let error: InstanceRunnerError = "dag conversion failed".to_string().into();
        match error {
            InstanceRunnerError::DagConversion(msg) => {
                assert_eq!(msg, "dag conversion failed");
            }
            _ => panic!("Expected DagConversion error"),
        }
    }

    #[test]
    fn test_error_display() {
        let error = InstanceRunnerError::Worker("worker crashed".to_string());
        assert_eq!(error.to_string(), "Worker error: worker crashed");

        let error = InstanceRunnerError::DagConversion("invalid dag".to_string());
        assert_eq!(error.to_string(), "DAG conversion error: invalid dag");

        let error = InstanceRunnerError::Shutdown;
        assert_eq!(error.to_string(), "Shutdown requested");

        let error = InstanceRunnerError::LostLease(WorkflowInstanceId::new());
        assert!(error.to_string().starts_with("Lost lease for instance"));
    }

    #[test]
    fn test_constants() {
        // Verify constants are reasonable values
        assert_eq!(DEFAULT_LEASE_SECONDS, 60);
        assert_eq!(DEFAULT_HEARTBEAT_INTERVAL, Duration::from_secs(10));
        assert_eq!(DEFAULT_CLAIM_BATCH_SIZE, 50);
        assert_eq!(DEFAULT_MAX_CONCURRENT_INSTANCES, 100);
        assert_eq!(DEFAULT_COMPLETION_BATCH_SIZE, 100);

        // Heartbeat should be significantly less than lease to prevent expiration
        assert!(DEFAULT_HEARTBEAT_INTERVAL.as_secs() < DEFAULT_LEASE_SECONDS as u64 / 2);
    }
}
