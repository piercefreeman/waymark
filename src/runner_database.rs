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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::ast_evaluator::ExpressionEvaluator;
use crate::dag::{DAG, DAGConverter, DAGNode};
use crate::db::{
    ClaimedInstance, Database, ScheduleId, WorkerStatusUpdate, WorkflowInstanceId,
    WorkflowSchedule, WorkflowVersionId,
};
use crate::execution_graph::{Completion, ExecutionState, SLEEP_WORKER_ID};
use crate::messages::execution::{ExecutionNode, NodeStatus};
use crate::messages::proto;
use crate::messages::proto::WorkflowArguments;
use crate::messages::{MessageError, decode_message, encode_message};
use crate::parser::ast;
use crate::pool_status::{PoolTimeSeries, TimeSeriesEntry};
use crate::schedule::{apply_jitter, next_cron_run, next_interval_run};
use crate::stats::LifecycleStats;
use crate::value::WorkflowValue;
use crate::worker::{ActionDispatchPayload, PythonWorkerPool};

/// Default lease duration for instance ownership (60 seconds)
pub const DEFAULT_LEASE_SECONDS: i64 = 60;

/// Default heartbeat interval (10 seconds)
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Default batch size for claiming instances
pub const DEFAULT_CLAIM_BATCH_SIZE: i32 = 50;

/// Default completion batch size
pub const DEFAULT_COMPLETION_BATCH_SIZE: usize = 100;

/// Default schedule check interval (10 seconds)
pub const DEFAULT_SCHEDULE_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Default schedule check batch size
pub const DEFAULT_SCHEDULE_CHECK_BATCH_SIZE: i32 = 100;

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
    /// Interval for checking due schedules
    pub schedule_check_interval: Duration,
    /// Max schedules to process per check cycle
    pub schedule_check_batch_size: i32,
    /// Garbage collection interval. If None, GC is disabled.
    pub gc_interval: Option<Duration>,
    /// Minimum age in seconds for completed/failed instances before GC cleanup.
    pub gc_retention_seconds: i64,
    /// Batch size for GC operations.
    pub gc_batch_size: i32,
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
            schedule_check_interval: DEFAULT_SCHEDULE_CHECK_INTERVAL,
            schedule_check_batch_size: DEFAULT_SCHEDULE_CHECK_BATCH_SIZE,
            gc_interval: None,           // GC disabled by default
            gc_retention_seconds: 86400, // 24 hours
            gc_batch_size: 100,
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
    /// Count of ready_queue nodes already queued for dispatch.
    queued_ready_count: usize,
    /// Pending completions to be batched
    pending_completions: Vec<Completion>,
    /// When this instance was claimed by this runner
    claimed_at: Instant,
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
#[derive(Debug, Clone)]
struct QueuedAction {
    instance_id: Uuid,
    node_id: String,
    module_name: String,
    action_name: String,
    timeout_seconds: u32,
    max_retries: u32,
    attempt_number: u32,
}

type DispatchQueueKey = (Uuid, String);

struct PreparedAction {
    action: QueuedAction,
    inputs: proto::WorkflowArguments,
    worker_idx: usize,
}

/// An action currently in-flight to a worker.
/// Used for Rust-side timeout tracking.
#[derive(Debug, Clone)]
struct InFlightAction {
    instance_id: Uuid,
    node_id: String,
    timeout_at_ms: i64,
    worker_idx: usize,
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
    dispatch_queue_keys: Mutex<HashSet<DispatchQueueKey>>,

    /// In-flight actions indexed by timeout time for efficient timeout checking.
    /// Key is timeout_at_ms, value is list of actions timing out at that time.
    timeout_queue: Mutex<std::collections::BTreeMap<i64, Vec<InFlightAction>>>,

    /// Reverse lookup from (instance_id, node_id) to timeout_at_ms.
    /// Used to remove entries when actions complete normally.
    timeout_lookup: Mutex<HashMap<(Uuid, String), i64>>,

    /// Shutdown signal
    shutdown: AtomicBool,

    /// Metrics
    metrics: Mutex<InstanceRunnerMetrics>,

    /// Channel for receiving completions from worker tasks
    completion_tx: mpsc::Sender<WorkerCompletion>,
    completion_rx: Mutex<mpsc::Receiver<WorkerCompletion>>,

    /// Sequence number for action IDs
    action_seq: AtomicU32,

    /// Pool-level time-series ring buffer (24h at 1-minute resolution)
    pool_time_series: std::sync::Mutex<PoolTimeSeries>,

    /// Rolling metric for instance completion duration (p50 over 5-minute window)
    instance_duration_metric: std::sync::Mutex<crate::stats::RollingMetric>,

    /// Total instances completed (completed + failed) since runner start
    instances_completed_total: AtomicU64,

    /// Lifecycle stats
    #[allow(dead_code)]
    stats: Option<Arc<LifecycleStats>>,
}

#[derive(Default)]
struct RunnerProfile {
    loops: u64,
    claim_instances: Duration,
    process_completions: Duration,
    check_timeouts: Duration,
    process_sleeping: Duration,
    collect_ready: Duration,
    dispatch_queue: Duration,
    finalize_instances: Duration,
}

impl RunnerProfile {
    fn reset(&mut self) {
        *self = Self::default();
    }
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
            dispatch_queue_keys: Mutex::new(HashSet::new()),
            timeout_queue: Mutex::new(std::collections::BTreeMap::new()),
            timeout_lookup: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            metrics: Mutex::new(InstanceRunnerMetrics::default()),
            completion_tx,
            completion_rx: Mutex::new(completion_rx),
            action_seq: AtomicU32::new(0),
            pool_time_series: std::sync::Mutex::new(PoolTimeSeries::new()),
            instance_duration_metric: std::sync::Mutex::new(crate::stats::RollingMetric::new(
                Duration::from_secs(300),
            )),
            instances_completed_total: AtomicU64::new(0),
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

        // Spawn schedule check task
        let schedule_handle = self.spawn_schedule_task();

        // Spawn GC task if enabled
        let gc_handle = self.spawn_gc_task();

        let profile_interval = std::env::var("RAPPEL_RUNNER_PROFILE_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis);
        let mut profile = RunnerProfile::default();
        let mut last_profile = Instant::now();

        // Main loop
        loop {
            if self.is_shutdown() {
                info!("Shutdown requested, stopping runner");
                break;
            }

            // 1. Claim new instances if we have capacity
            if profile_interval.is_some() {
                let started_at = Instant::now();
                self.claim_instances().await?;
                profile.claim_instances += started_at.elapsed();
            } else {
                self.claim_instances().await?;
            }

            // 2. Process any completions from workers
            if profile_interval.is_some() {
                let started_at = Instant::now();
                self.process_worker_completions().await?;
                profile.process_completions += started_at.elapsed();
            } else {
                self.process_worker_completions().await?;
            }

            // 3. Check for action timeouts (Rust-side enforcement)
            if profile_interval.is_some() {
                let started_at = Instant::now();
                self.check_action_timeouts().await?;
                profile.check_timeouts += started_at.elapsed();
            } else {
                self.check_action_timeouts().await?;
            }

            // 4. Check for durable sleep wakeups
            if profile_interval.is_some() {
                let started_at = Instant::now();
                self.process_sleeping_nodes().await?;
                profile.process_sleeping += started_at.elapsed();
            } else {
                self.process_sleeping_nodes().await?;
            }

            // 5. Collect ready actions from instances → enqueue to dispatch queue
            if profile_interval.is_some() {
                let started_at = Instant::now();
                self.collect_ready_actions().await?;
                profile.collect_ready += started_at.elapsed();
            } else {
                self.collect_ready_actions().await?;
            }

            // 6. Dispatch from queue: dequeue → mark running → sync DB → send to workers
            if profile_interval.is_some() {
                let started_at = Instant::now();
                self.dispatch_from_queue().await?;
                profile.dispatch_queue += started_at.elapsed();
            } else {
                self.dispatch_from_queue().await?;
            }

            // 7. Process completed instances
            if profile_interval.is_some() {
                let started_at = Instant::now();
                self.finalize_completed_instances().await?;
                profile.finalize_instances += started_at.elapsed();
            } else {
                self.finalize_completed_instances().await?;
            }

            // Small sleep to avoid tight loop when idle
            let active_count = self.active_instances.read().await.len();
            let queue_size = self.dispatch_queue.lock().await.len();
            if active_count == 0 && queue_size == 0 {
                tokio::time::sleep(self.config.idle_poll_interval).await;
            } else {
                // Brief yield to allow other tasks to run
                tokio::task::yield_now().await;
            }

            if let Some(interval) = profile_interval {
                profile.loops += 1;
                if last_profile.elapsed() >= interval {
                    info!(
                        runner_id = %self.config.runner_id,
                        loops = profile.loops,
                        active_instances = active_count,
                        queue_size = queue_size,
                        claim_ms = profile.claim_instances.as_millis() as u64,
                        completions_ms = profile.process_completions.as_millis() as u64,
                        timeouts_ms = profile.check_timeouts.as_millis() as u64,
                        sleeping_ms = profile.process_sleeping.as_millis() as u64,
                        ready_ms = profile.collect_ready.as_millis() as u64,
                        dispatch_ms = profile.dispatch_queue.as_millis() as u64,
                        finalize_ms = profile.finalize_instances.as_millis() as u64,
                        "runner profile"
                    );
                    profile.reset();
                    last_profile = Instant::now();
                }
            }
        }

        // Cancel background tasks
        heartbeat_handle.abort();
        status_handle.abort();
        schedule_handle.abort();
        if let Some(handle) = gc_handle {
            handle.abort();
        }

        // Release all owned instances
        self.release_all_instances().await?;

        Ok(())
    }

    /// Process completions received from worker tasks
    async fn process_worker_completions(&self) -> InstanceRunnerResult<()> {
        let mut rx = self.completion_rx.lock().await;
        let mut completed_actions: Vec<(Uuid, String)> = Vec::new();

        // Drain all available completions without blocking
        loop {
            match rx.try_recv() {
                Ok(worker_completion) => {
                    let mut instances = self.active_instances.write().await;
                    if let Some(instance) = instances.get_mut(&worker_completion.instance_id) {
                        // Only process if action is still in-flight
                        // (Rust-side timeout may have already handled it)
                        if !instance
                            .in_flight
                            .remove(&worker_completion.completion.node_id)
                        {
                            // Action already completed (e.g., via timeout), ignore this response
                            debug!(
                                instance_id = %worker_completion.instance_id,
                                node_id = %worker_completion.completion.node_id,
                                "Ignoring late completion for action no longer in-flight"
                            );
                            // Still release worker slot
                            self.worker_pool.release_slot(worker_completion.worker_idx);
                            continue;
                        }

                        // Track for timeout removal
                        completed_actions.push((
                            worker_completion.instance_id,
                            worker_completion.completion.node_id.clone(),
                        ));

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

        // Remove completed actions from timeout tracking
        if !completed_actions.is_empty() {
            self.remove_from_timeout_tracking(&completed_actions).await;
        }

        Ok(())
    }

    /// Remove actions from timeout tracking (called when they complete normally)
    async fn remove_from_timeout_tracking(&self, actions: &[(Uuid, String)]) {
        let mut timeout_queue = self.timeout_queue.lock().await;
        let mut timeout_lookup = self.timeout_lookup.lock().await;

        for (instance_id, node_id) in actions {
            if let Some(timeout_at_ms) = timeout_lookup.remove(&(*instance_id, node_id.clone()))
                && let Some(entries) = timeout_queue.get_mut(&timeout_at_ms)
            {
                entries.retain(|e| !(e.instance_id == *instance_id && e.node_id == *node_id));
                if entries.is_empty() {
                    timeout_queue.remove(&timeout_at_ms);
                }
            }
        }
    }

    /// Check for timed-out actions and create synthetic failure completions.
    ///
    /// This is the Rust-side timeout enforcement - we don't rely on Python
    /// to report timeouts. Any action that exceeds its timeout_seconds gets
    /// a synthetic failure completion which will trigger retry logic.
    async fn check_action_timeouts(&self) -> InstanceRunnerResult<()> {
        let now_ms = Utc::now().timestamp_millis();
        let mut timed_out: Vec<InFlightAction> = Vec::new();

        // Find all timed-out actions
        {
            let mut timeout_queue = self.timeout_queue.lock().await;
            let mut timeout_lookup = self.timeout_lookup.lock().await;

            // Collect all entries with timeout_at_ms <= now
            let expired_keys: Vec<i64> = timeout_queue.range(..=now_ms).map(|(k, _)| *k).collect();

            for key in expired_keys {
                if let Some(entries) = timeout_queue.remove(&key) {
                    for entry in entries {
                        timeout_lookup.remove(&(entry.instance_id, entry.node_id.clone()));
                        timed_out.push(entry);
                    }
                }
            }
        }

        if timed_out.is_empty() {
            return Ok(());
        }

        // Create synthetic completions for timed-out actions
        let mut instances = self.active_instances.write().await;

        for entry in timed_out {
            if let Some(instance) = instances.get_mut(&entry.instance_id) {
                // Only process if still in-flight (might have completed just before timeout)
                if !instance.in_flight.remove(&entry.node_id) {
                    continue;
                }

                // Get timeout duration for error message
                let timeout_seconds = instance
                    .state
                    .graph
                    .nodes
                    .get(&entry.node_id)
                    .map(|n| n.timeout_seconds)
                    .unwrap_or(0);

                let completion = Completion {
                    node_id: entry.node_id.clone(),
                    success: false,
                    result: None,
                    error: Some(format!(
                        "Action timed out after {} seconds (Rust-side enforcement)",
                        timeout_seconds
                    )),
                    error_type: Some("TimeoutError".to_string()),
                    worker_id: format!("worker-{}", entry.worker_idx),
                    duration_ms: (now_ms - entry.timeout_at_ms + (timeout_seconds as i64 * 1000)),
                    worker_duration_ms: None, // Unknown - worker may still be running
                };

                instance.pending_completions.push(completion);

                // Release worker slot
                self.worker_pool.release_slot(entry.worker_idx);

                self.metrics.lock().await.actions_completed += 1;

                warn!(
                    instance_id = %entry.instance_id,
                    node_id = %entry.node_id,
                    timeout_seconds,
                    "Action timed out (Rust-side enforcement)"
                );
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

    /// Spawn the pool-level status report background task.
    ///
    /// Every 60 seconds, aggregates pool-level metrics and writes a single row
    /// to worker_status keyed by pool_id. Also pushes a TimeSeriesEntry into
    /// the in-memory ring buffer and encodes it as BYTEA.
    fn spawn_status_report_task(&self) -> tokio::task::JoinHandle<()> {
        let db = self.db.clone();
        let pool_id = self.config.pool_id;
        let status_interval = self.config.status_report_interval;
        let worker_pool = Arc::clone(&self.worker_pool);
        let dispatch_queue = &self.dispatch_queue as *const Mutex<VecDeque<QueuedAction>>;
        let shutdown = &self.shutdown as *const AtomicBool;
        let active_instances =
            &self.active_instances as *const RwLock<HashMap<Uuid, ActiveInstance>>;
        let pool_time_series = &self.pool_time_series as *const std::sync::Mutex<PoolTimeSeries>;
        let instance_duration_metric =
            &self.instance_duration_metric as *const std::sync::Mutex<crate::stats::RollingMetric>;
        let instances_completed_total = &self.instances_completed_total as *const AtomicU64;

        // Safety: We know self lives as long as the returned handle
        let shutdown_ptr = unsafe { &*shutdown };
        let dispatch_queue_ptr = unsafe { &*dispatch_queue };
        let active_instances_ptr = unsafe { &*active_instances };
        let pool_time_series_ptr = unsafe { &*pool_time_series };
        let instance_duration_metric_ptr = unsafe { &*instance_duration_metric };
        let instances_completed_total_ptr = unsafe { &*instances_completed_total };

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(status_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut prev_total_completed: i64 = 0;
            let mut prev_tick: Option<Instant> = None;

            loop {
                interval.tick().await;

                if shutdown_ptr.load(Ordering::SeqCst) {
                    break;
                }

                let tick_now = Instant::now();

                // 1. Per-worker throughput snapshots → aggregate
                let snapshots = worker_pool.throughput_snapshots();
                let throughput_per_min: f64 = snapshots.iter().map(|s| s.throughput_per_min).sum();
                let total_completed: i64 = snapshots.iter().map(|s| s.total_completed as i64).sum();

                // Instantaneous actions/sec from delta since last tick
                let actions_per_sec = match prev_tick {
                    Some(prev) => {
                        let elapsed = tick_now.duration_since(prev).as_secs_f64();
                        let delta = total_completed - prev_total_completed;
                        if elapsed > 0.0 {
                            delta as f64 / elapsed
                        } else {
                            0.0
                        }
                    }
                    None => 0.0,
                };
                prev_total_completed = total_completed;
                prev_tick = Some(tick_now);
                let last_action_at = snapshots.iter().filter_map(|s| s.last_action_at).max();

                // 2. Active workers count
                let active_workers = worker_pool.len() as i32;

                // 3. Median instance duration (p50 of recently completed) and active count
                let (median_instance_duration_secs, active_instance_count) = {
                    let instances = active_instances_ptr.read().await;
                    let count = instances.len() as i32;
                    let median = instance_duration_metric_ptr.lock().ok().and_then(|mut m| {
                        let stats = m.stats();
                        if stats.count > 0 {
                            Some(stats.p50_us as f64 / 1_000_000.0)
                        } else {
                            None
                        }
                    });
                    (median, count)
                };

                // 4. Dispatch queue size
                let dispatch_queue_size = dispatch_queue_ptr.lock().await.len() as i64;

                // 5. Total in-flight from worker pool
                let total_in_flight = worker_pool.total_in_flight() as i64;

                // 6. Push time-series entry and encode
                let now_secs = Utc::now().timestamp();
                let time_series_bytes = {
                    let mut ts = pool_time_series_ptr
                        .lock()
                        .expect("pool_time_series lock poisoned");
                    ts.push(TimeSeriesEntry {
                        timestamp_secs: now_secs,
                        actions_per_sec: actions_per_sec as f32,
                        active_workers: active_workers as u16,
                        median_instance_duration_secs: median_instance_duration_secs.unwrap_or(0.0)
                            as f32,
                        active_instances: active_instance_count as u32,
                        queue_depth: dispatch_queue_size as u32,
                        in_flight_actions: total_in_flight as u32,
                    });
                    ts.encode()
                };

                // 7. Upsert single pool-level row
                let total_instances_completed =
                    instances_completed_total_ptr.load(Ordering::Relaxed) as i64;
                let update = WorkerStatusUpdate {
                    throughput_per_min,
                    total_completed,
                    last_action_at,
                    median_dequeue_ms: None,
                    median_handling_ms: None,
                    dispatch_queue_size,
                    total_in_flight,
                    active_workers,
                    actions_per_sec,
                    median_instance_duration_secs,
                    active_instance_count,
                    total_instances_completed,
                    time_series: Some(time_series_bytes),
                };

                match db.upsert_worker_status(pool_id, &update).await {
                    Ok(()) => {
                        debug!(
                            active_workers,
                            actions_per_sec,
                            dispatch_queue_size,
                            total_in_flight,
                            "Reported pool status"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to report pool status");
                    }
                }
            }
        })
    }

    /// Spawn the schedule check background task.
    ///
    /// Periodically checks for due schedules and creates workflow instances.
    fn spawn_schedule_task(&self) -> tokio::task::JoinHandle<()> {
        let db = self.db.clone();
        let schedule_interval = self.config.schedule_check_interval;
        let batch_size = self.config.schedule_check_batch_size;
        let shutdown = &self.shutdown as *const AtomicBool;

        // Safety: We know self lives as long as the returned handle
        let shutdown_ptr = unsafe { &*shutdown };

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(schedule_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if shutdown_ptr.load(Ordering::SeqCst) {
                    break;
                }

                // Keep processing until we've drained all due schedules
                loop {
                    match Self::process_due_schedules(&db, batch_size).await {
                        Ok(count) => {
                            if count < batch_size as usize {
                                // No more due schedules
                                break;
                            }
                            // There might be more, continue processing
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to process due schedules");
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Process due scheduled workflows by creating new instances.
    ///
    /// Returns the number of schedules that were processed.
    async fn process_due_schedules(db: &Database, batch_size: i32) -> InstanceRunnerResult<usize> {
        // Find due schedules (uses SKIP LOCKED for multi-runner safety)
        let schedules = db
            .find_due_schedules(batch_size)
            .await
            .map_err(InstanceRunnerError::Database)?;

        let count = schedules.len();

        for schedule in schedules {
            // Get the latest version for this workflow
            let version_id = match db
                .get_latest_workflow_version(&schedule.workflow_name)
                .await
            {
                Ok(Some(id)) => id,
                Ok(None) => {
                    error!(
                        workflow_name = %schedule.workflow_name,
                        schedule_id = %schedule.id,
                        "SCHEDULE SKIPPED: No registered workflow version found. \
                         The workflow DAG must be registered before the schedule can execute. \
                         Re-register the schedule using schedule_workflow() to fix this."
                    );
                    // Still update next_run_at to avoid infinite retries
                    if let Ok(next_run) = Self::compute_next_run(&schedule)
                        && let Err(e) = db
                            .update_schedule_next_run(ScheduleId(schedule.id), next_run)
                            .await
                    {
                        error!(error = %e, "Failed to update schedule next_run");
                    }
                    continue;
                }
                Err(e) => {
                    error!(error = %e, "Failed to get workflow version for schedule");
                    continue;
                }
            };

            // Skip if allow_duplicate is false and there's already a running instance
            if !schedule.allow_duplicate {
                match db
                    .has_running_instance_for_schedule(ScheduleId(schedule.id))
                    .await
                {
                    Ok(true) => {
                        info!(
                            schedule_id = %schedule.id,
                            workflow_name = %schedule.workflow_name,
                            "Skipping scheduled run: instance already running (allow_duplicate=false)"
                        );
                        // Still advance next_run_at to avoid re-checking on next tick
                        if let Ok(next_run) = Self::compute_next_run(&schedule)
                            && let Err(e) = db
                                .update_schedule_next_run(ScheduleId(schedule.id), next_run)
                                .await
                        {
                            error!(error = %e, "Failed to update schedule next_run");
                        }
                        continue;
                    }
                    Ok(false) => {} // No running instance, proceed normally
                    Err(e) => {
                        error!(error = %e, "Failed to check for running instances");
                        continue;
                    }
                }
            }

            // Create workflow instance with scheduled inputs and priority
            let instance_id = match db
                .create_instance_with_priority(
                    &schedule.workflow_name,
                    version_id,
                    schedule.input_payload.as_deref(),
                    Some(ScheduleId(schedule.id)),
                    schedule.priority,
                )
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    error!(
                        error = %e,
                        workflow_name = %schedule.workflow_name,
                        "Failed to create scheduled instance"
                    );
                    continue;
                }
            };

            info!(
                schedule_id = %schedule.id,
                workflow_name = %schedule.workflow_name,
                instance_id = %instance_id,
                "Created scheduled workflow instance"
            );

            // Compute next run and update schedule
            match Self::compute_next_run(&schedule) {
                Ok(next_run) => {
                    if let Err(e) = db
                        .mark_schedule_executed(ScheduleId(schedule.id), instance_id, next_run)
                        .await
                    {
                        error!(error = %e, "Failed to mark schedule executed");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to compute next run time");
                }
            }
        }

        Ok(count)
    }

    /// Compute the next run time based on schedule type.
    fn compute_next_run(schedule: &WorkflowSchedule) -> InstanceRunnerResult<DateTime<Utc>> {
        let base = match schedule.schedule_type.as_str() {
            "cron" => {
                let expr = schedule
                    .cron_expression
                    .as_ref()
                    .ok_or_else(|| InstanceRunnerError::Worker("Missing cron expression".into()))?;
                next_cron_run(expr)
                    .map_err(|e| InstanceRunnerError::Worker(format!("Invalid cron: {}", e)))?
            }
            "interval" => {
                let secs = schedule.interval_seconds.ok_or_else(|| {
                    InstanceRunnerError::Worker("Missing interval_seconds".into())
                })?;
                next_interval_run(secs, Some(Utc::now()))
            }
            _ => {
                return Err(InstanceRunnerError::Worker(format!(
                    "Unknown schedule type: {}",
                    schedule.schedule_type
                )));
            }
        };

        apply_jitter(base, schedule.jitter_seconds)
            .map_err(|e| InstanceRunnerError::Worker(format!("Jitter error: {}", e)))
    }

    /// Spawn the garbage collection background task (if enabled).
    ///
    /// Periodically cleans up old completed/failed workflow instances.
    fn spawn_gc_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        let gc_interval = self.config.gc_interval?;
        let db = self.db.clone();
        let retention_seconds = self.config.gc_retention_seconds;
        let batch_size = self.config.gc_batch_size;
        let shutdown = &self.shutdown as *const AtomicBool;

        // Safety: We know self lives as long as the returned handle
        let shutdown_ptr = unsafe { &*shutdown };

        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(gc_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if shutdown_ptr.load(Ordering::SeqCst) {
                    break;
                }

                // Keep processing until we've drained all eligible instances
                loop {
                    match db
                        .garbage_collect_instances(retention_seconds, batch_size)
                        .await
                    {
                        Ok(count) => {
                            if count > 0 {
                                info!(deleted = count, "Garbage collected old instances");
                            }
                            if count < batch_size as i64 {
                                // No more eligible instances
                                break;
                            }
                            // There might be more, continue processing
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to garbage collect instances");
                            break;
                        }
                    }
                }
            }
        }))
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
        let (state, stalled_completions) = match claimed.execution_graph {
            Some(bytes) => {
                // Restore from persisted state
                let mut state = ExecutionState::from_bytes(&bytes)?;

                // Recover any nodes that were running when previous owner crashed
                state.recover_running_nodes();

                // Recover completed nodes whose successors weren't advanced
                // (e.g. runner crashed after action completed but before next
                // nodes were determined)
                let stalled = state.find_stalled_completions(&dag);

                self.metrics.lock().await.orphans_recovered += 1;
                (state, stalled)
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
                (state, Vec::new())
            }
        };

        let active = ActiveInstance {
            instance_id: claimed.id,
            workflow_name: claimed.workflow_name,
            dag,
            state,
            in_flight: HashSet::new(),
            queued_ready_count: 0,
            pending_completions: stalled_completions,
            claimed_at: Instant::now(),
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
        let mut queue_keys = self.dispatch_queue_keys.lock().await;

        for (_, instance) in instances.iter_mut() {
            let ready_len = instance.state.graph.ready_queue.len();
            if ready_len == 0 {
                continue;
            }
            if instance.queued_ready_count > ready_len {
                warn!(
                    instance_id = %instance.instance_id,
                    queued_ready_count = instance.queued_ready_count,
                    ready_len,
                    "Queued ready count exceeds ready queue length; correcting"
                );
                instance.queued_ready_count = ready_len;
            }
            if instance.queued_ready_count == ready_len {
                continue;
            }

            // Peek at ready nodes without draining - we'll selectively remove them
            // Worker actions stay in ready_queue until dispatch (for crash recovery)
            let ready_nodes = instance.state.peek_ready_queue();

            for node_id in ready_nodes {
                // Skip if already in flight
                if instance.in_flight.contains(&node_id) {
                    continue;
                }

                // Skip if already in dispatch queue (avoid duplicates)
                let queue_key = (instance.instance_id.0, node_id.clone());
                if queue_keys.contains(&queue_key) {
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
                    let timeout_seconds = instance.state.get_timeout_seconds(&node_id);
                    let max_retries = instance.state.get_max_retries(&node_id);
                    let attempt_number = instance.state.get_attempt_number(&node_id);

                    queue.push_back(QueuedAction {
                        instance_id: instance.instance_id.0,
                        node_id: node_id.clone(),
                        module_name,
                        action_name,
                        timeout_seconds,
                        max_retries,
                        attempt_number,
                    });
                    queue_keys.insert(queue_key);
                    instance.queued_ready_count += 1;
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
            let mut queue_keys = self.dispatch_queue_keys.lock().await;
            let count = available.min(queue.len());
            let actions: Vec<QueuedAction> = queue.drain(..count).collect();
            for action in &actions {
                queue_keys.remove(&(action.instance_id, action.node_id.clone()));
            }
            actions
        };

        if actions_to_dispatch.is_empty() {
            return Ok(());
        }

        let mut queued_removals_by_instance: HashMap<Uuid, usize> = HashMap::new();
        for action in &actions_to_dispatch {
            *queued_removals_by_instance
                .entry(action.instance_id)
                .or_insert(0) += 1;
        }

        // Group actions by instance for efficient updates
        let mut by_instance: HashMap<Uuid, Vec<&QueuedAction>> = HashMap::new();
        for action in &actions_to_dispatch {
            by_instance
                .entry(action.instance_id)
                .or_default()
                .push(action);
        }

        // Phase 1: Mark as Running in in-memory state and track timeouts
        let now_ms = Utc::now().timestamp_millis();
        let mut timeout_entries: Vec<(i64, InFlightAction)> = Vec::new();
        let mut prepared_actions: Vec<PreparedAction> = Vec::new();

        {
            let mut instances = self.active_instances.write().await;
            for (instance_id, actions) in &by_instance {
                if let Some(instance) = instances.get_mut(instance_id) {
                    let mut dispatched_nodes: Vec<String> = Vec::new();

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

                        let inputs_bytes = instance
                            .state
                            .get_inputs_for_node(&action.node_id, &instance.dag);
                        let inputs: proto::WorkflowArguments = inputs_bytes
                            .as_ref()
                            .and_then(|bytes| decode_message(bytes).ok())
                            .unwrap_or_default();

                        instance.state.mark_running_no_queue_removal(
                            &action.node_id,
                            &worker_id,
                            inputs_bytes.clone(),
                        );
                        dispatched_nodes.push(action.node_id.clone());

                        // Set started_at_ms for timeout tracking
                        if let Some(node) = instance.state.graph.nodes.get_mut(&action.node_id) {
                            node.started_at_ms = Some(now_ms);
                        }

                        instance.in_flight.insert(action.node_id.clone());

                        // Track timeout (only if timeout is set)
                        if action.timeout_seconds > 0 {
                            let timeout_at_ms = now_ms + (action.timeout_seconds as i64 * 1000);
                            timeout_entries.push((
                                timeout_at_ms,
                                InFlightAction {
                                    instance_id: *instance_id,
                                    node_id: action.node_id.clone(),
                                    timeout_at_ms,
                                    worker_idx,
                                },
                            ));
                        }

                        prepared_actions.push(PreparedAction {
                            action: (*action).clone(),
                            inputs,
                            worker_idx,
                        });
                    }

                    if !dispatched_nodes.is_empty() {
                        instance
                            .state
                            .remove_from_ready_queue_batch(&dispatched_nodes);
                    }

                    if let Some(removed) = queued_removals_by_instance.get(instance_id) {
                        instance.queued_ready_count =
                            instance.queued_ready_count.saturating_sub(*removed);
                    }
                }
            }
        }

        // Add timeout entries to tracking structures
        if !timeout_entries.is_empty() {
            let mut timeout_queue = self.timeout_queue.lock().await;
            let mut timeout_lookup = self.timeout_lookup.lock().await;

            for (timeout_at_ms, entry) in timeout_entries {
                timeout_lookup.insert((entry.instance_id, entry.node_id.clone()), timeout_at_ms);
                timeout_queue
                    .entry(timeout_at_ms)
                    .or_insert_with(Vec::new)
                    .push(entry);
            }
        }

        // Phase 2: Sync state to DB (before sending to workers)
        // This ensures if we crash, the DB shows the actions were attempted
        {
            let instances = self.active_instances.read().await;
            let updates: Vec<_> = by_instance
                .keys()
                .filter_map(|instance_id| {
                    instances.get(instance_id).map(|instance| {
                        let graph_bytes = instance.state.to_bytes();
                        let next_wakeup = instance
                            .state
                            .graph
                            .next_wakeup_time
                            .map(|ms| Utc.timestamp_millis_opt(ms).unwrap());
                        (instance.instance_id, graph_bytes, next_wakeup)
                    })
                })
                .collect();

            if !updates.is_empty() {
                match self
                    .db
                    .update_execution_graphs_batch(&self.config.runner_id, &updates)
                    .await
                {
                    Ok(updated_ids) => {
                        for (id, _, _) in &updates {
                            if updated_ids.contains(id) {
                                debug!(instance_id = %id, "Synced running state to DB before dispatch");
                            } else {
                                warn!(instance_id = %id, "Lost lease while syncing state before dispatch");
                                // TODO: Handle lost lease - re-enqueue actions
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to sync state to DB before dispatch");
                    }
                }
            }
        }

        // Phase 3: Actually send to workers
        for prepared in prepared_actions {
            let instances = self.active_instances.read().await;
            if !instances.contains_key(&prepared.action.instance_id) {
                continue;
            }

            // Get worker (already acquired slot above)
            let worker_idx = prepared.worker_idx;
            let worker = self.worker_pool.get_worker(worker_idx).await;
            let worker_id = format!("worker-{}", worker_idx);

            let action_seq = self.action_seq.fetch_add(1, Ordering::SeqCst);
            let dispatch_token = Uuid::new_v4();

            let payload = ActionDispatchPayload {
                action_id: prepared.action.node_id.clone(),
                instance_id: prepared.action.instance_id.to_string(),
                sequence: action_seq,
                action_name: prepared.action.action_name.clone(),
                module_name: prepared.action.module_name.clone(),
                kwargs: prepared.inputs.clone(),
                timeout_seconds: prepared.action.timeout_seconds,
                max_retries: prepared.action.max_retries,
                attempt_number: prepared.action.attempt_number,
                dispatch_token,
            };

            self.metrics.lock().await.actions_dispatched += 1;

            // Spawn task to send and handle completion
            let completion_tx = self.completion_tx.clone();
            let instance_id = prepared.action.instance_id;
            let node_id = prepared.action.node_id.clone();
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
            });

            trace!(
                instance_id = %prepared.action.instance_id,
                node_id = %prepared.action.node_id,
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
        crate::executor::execute_inline_node(&mut instance.state, &instance.dag, node_id, dag_node)
    }

    /// Finalize completed instances
    async fn finalize_completed_instances(&self) -> InstanceRunnerResult<()> {
        // Collect batches for different operation types
        // (instance_id, result_payload, graph_bytes)
        let mut to_complete: Vec<(WorkflowInstanceId, Option<Vec<u8>>, Vec<u8>)> = Vec::new();
        let mut to_fail: Vec<(WorkflowInstanceId, Option<Vec<u8>>, Vec<u8>)> = Vec::new();
        // (instance_id, graph_bytes, next_wakeup)
        let mut to_release: Vec<(WorkflowInstanceId, Vec<u8>, Option<DateTime<Utc>>)> = Vec::new();
        let mut to_update: Vec<(WorkflowInstanceId, Vec<u8>, Option<DateTime<Utc>>)> = Vec::new();

        // Track metadata for logging after batch operations complete
        let mut complete_meta: HashMap<WorkflowInstanceId, String> = HashMap::new();
        let mut fail_meta: HashMap<WorkflowInstanceId, (String, Option<String>)> = HashMap::new();

        // Phase 1: Apply completions and categorize instances
        {
            let mut instances = self.active_instances.write().await;

            for (_, instance) in instances.iter_mut() {
                // Recover stalled completions: if no work is pending and nothing
                // is in flight, check for nodes that completed but whose
                // successors were never advanced (e.g. the runner crashed after
                // an action finished but before the next nodes were determined).
                // Inject them as pending completions so the normal flow handles
                // them.
                if instance.pending_completions.is_empty()
                    && !instance.state.has_pending_work()
                    && instance.in_flight.is_empty()
                {
                    let stalled = instance.state.find_stalled_completions(&instance.dag);
                    if !stalled.is_empty() {
                        debug!(
                            instance_id = %instance.instance_id,
                            stalled_count = stalled.len(),
                            "Recovering stalled completions for active instance"
                        );
                        instance.pending_completions = stalled;
                    }
                }

                // Apply any remaining completions
                let completion_result = if !instance.pending_completions.is_empty() {
                    let completions = std::mem::take(&mut instance.pending_completions);
                    Some(
                        instance
                            .state
                            .apply_completions_batch(completions, &instance.dag),
                    )
                } else {
                    None
                };

                // Check for workflow completion/failure
                if let Some(ref result) = completion_result {
                    if result.workflow_completed {
                        let graph_bytes = instance.state.to_bytes();
                        complete_meta.insert(instance.instance_id, instance.workflow_name.clone());
                        to_complete.push((
                            instance.instance_id,
                            result.result_payload.clone(),
                            graph_bytes,
                        ));
                        continue;
                    } else if result.workflow_failed {
                        let graph_bytes = instance.state.to_bytes();
                        fail_meta.insert(
                            instance.instance_id,
                            (instance.workflow_name.clone(), result.error_message.clone()),
                        );
                        to_fail.push((
                            instance.instance_id,
                            result.result_payload.clone(),
                            graph_bytes,
                        ));
                        continue;
                    }
                }

                let previous_wakeup = instance.state.graph.next_wakeup_time;
                let fully_sleeping = Self::refresh_sleep_state(instance);
                let next_wakeup =
                    Self::sleep_wakeup_datetime_ms(instance.state.graph.next_wakeup_time);

                if fully_sleeping {
                    let graph_bytes = instance.state.to_bytes();
                    to_release.push((instance.instance_id, graph_bytes, next_wakeup));
                } else if !instance.state.has_pending_work() && instance.in_flight.is_empty() {
                    let graph_bytes = instance.state.to_bytes();
                    to_update.push((instance.instance_id, graph_bytes, next_wakeup));
                } else if instance.state.graph.next_wakeup_time != previous_wakeup {
                    // Wakeup time changed
                    let graph_bytes = instance.state.to_bytes();
                    to_update.push((instance.instance_id, graph_bytes, next_wakeup));
                }
            }
        }

        // Phase 2: Execute batch operations
        let mut completed_ids: HashSet<WorkflowInstanceId> = HashSet::new();
        let mut released_ids: HashSet<WorkflowInstanceId> = HashSet::new();
        let mut lost_lease_ids: HashSet<WorkflowInstanceId> = HashSet::new();

        // Batch complete
        if !to_complete.is_empty() {
            match self
                .db
                .complete_instances_batch(&self.config.runner_id, &to_complete)
                .await
            {
                Ok(succeeded) => {
                    let mut metrics = self.metrics.lock().await;
                    for id in &succeeded {
                        completed_ids.insert(*id);
                        metrics.instances_completed += 1;
                        if let Some(workflow_name) = complete_meta.get(id) {
                            info!(
                                instance_id = %id,
                                workflow = %workflow_name,
                                "Instance completed successfully"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to complete instances batch");
                }
            }
        }

        // Batch fail
        if !to_fail.is_empty() {
            match self
                .db
                .fail_instances_batch(&self.config.runner_id, &to_fail)
                .await
            {
                Ok(succeeded) => {
                    let mut metrics = self.metrics.lock().await;
                    for id in &succeeded {
                        completed_ids.insert(*id);
                        metrics.instances_failed += 1;
                        if let Some((workflow_name, error_msg)) = fail_meta.get(id) {
                            warn!(
                                instance_id = %id,
                                workflow = %workflow_name,
                                error = ?error_msg,
                                "Instance failed"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to fail instances batch");
                }
            }
        }

        // Batch release
        if !to_release.is_empty() {
            match self
                .db
                .release_instances_batch(&self.config.runner_id, &to_release)
                .await
            {
                Ok(succeeded) => {
                    released_ids = succeeded;
                }
                Err(e) => {
                    error!(error = %e, "Failed to release instances batch");
                }
            }
        }

        // Batch update
        if !to_update.is_empty() {
            match self
                .db
                .update_execution_graphs_batch(&self.config.runner_id, &to_update)
                .await
            {
                Ok(succeeded) => {
                    // Track instances that lost their lease
                    for (id, _, _) in &to_update {
                        if !succeeded.contains(id) {
                            warn!(instance_id = %id, "Lost lease while persisting execution graph");
                            lost_lease_ids.insert(*id);
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to update execution graphs batch");
                }
            }
        }

        // Record duration for completed/failed instances into the rolling metric
        if !completed_ids.is_empty() {
            self.instances_completed_total
                .fetch_add(completed_ids.len() as u64, Ordering::Relaxed);
            let instances = self.active_instances.read().await;
            let now = Instant::now();
            if let Ok(mut metric) = self.instance_duration_metric.lock() {
                for id in &completed_ids {
                    if let Some(inst) = instances.get(&id.0) {
                        let duration_us = now.duration_since(inst.claimed_at).as_micros() as u64;
                        metric.record(duration_us);
                    }
                }
            }
        }

        // Phase 3: Remove finalized instances from active set
        let mut instances = self.active_instances.write().await;
        for id in completed_ids {
            instances.remove(&id.0);
        }
        for id in released_ids {
            instances.remove(&id.0);
        }
        for id in lost_lease_ids {
            instances.remove(&id.0);
        }

        Ok(())
    }

    /// Release all owned instances (on shutdown)
    async fn release_all_instances(&self) -> InstanceRunnerResult<()> {
        let instances = std::mem::take(&mut *self.active_instances.write().await);

        let releases: Vec<_> = instances
            .into_values()
            .map(|mut instance| {
                Self::refresh_sleep_state(&mut instance);
                let next_wakeup =
                    Self::sleep_wakeup_datetime_ms(instance.state.graph.next_wakeup_time);
                let graph_bytes = instance.state.to_bytes();
                (instance.instance_id, graph_bytes, next_wakeup)
            })
            .collect();

        if !releases.is_empty() {
            match self
                .db
                .release_instances_batch(&self.config.runner_id, &releases)
                .await
            {
                Ok(released) => {
                    for (id, _, _) in &releases {
                        if released.contains(id) {
                            debug!(instance_id = %id, "Released instance");
                        } else {
                            warn!(instance_id = %id, "Failed to release instance (lost lease?)");
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to release instances batch");
                }
            }
        }

        Ok(())
    }

    /// Get current metrics
    pub async fn metrics(&self) -> InstanceRunnerMetrics {
        self.metrics.lock().await.clone()
    }
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
        assert_eq!(
            config.schedule_check_interval,
            DEFAULT_SCHEDULE_CHECK_INTERVAL
        );
        assert_eq!(
            config.schedule_check_batch_size,
            DEFAULT_SCHEDULE_CHECK_BATCH_SIZE
        );
        assert!(config.gc_interval.is_none()); // GC disabled by default
        assert_eq!(config.gc_retention_seconds, 86400);
        assert_eq!(config.gc_batch_size, 100);
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
            schedule_check_interval: Duration::from_secs(5),
            schedule_check_batch_size: 50,
            gc_interval: Some(Duration::from_secs(3600)),
            gc_retention_seconds: 43200,
            gc_batch_size: 50,
        };
        assert_eq!(config.runner_id, "custom-runner-123");
        assert_eq!(config.lease_seconds, 120);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(20));
        assert_eq!(config.claim_batch_size, 50);
        assert_eq!(config.max_concurrent_instances, 200);
        assert_eq!(config.completion_batch_size, 200);
        assert_eq!(config.schedule_check_interval, Duration::from_secs(5));
        assert_eq!(config.schedule_check_batch_size, 50);
        assert_eq!(config.gc_interval, Some(Duration::from_secs(3600)));
        assert_eq!(config.gc_retention_seconds, 43200);
        assert_eq!(config.gc_batch_size, 50);
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

    #[test]
    fn test_in_flight_action_construction() {
        let instance_id = Uuid::new_v4();
        let action = InFlightAction {
            instance_id,
            node_id: "action_0".to_string(),
            timeout_at_ms: 1000,
            worker_idx: 0,
        };
        assert_eq!(action.instance_id, instance_id);
        assert_eq!(action.node_id, "action_0");
        assert_eq!(action.timeout_at_ms, 1000);
        assert_eq!(action.worker_idx, 0);
    }

    #[test]
    fn test_timeout_queue_ordering() {
        // BTreeMap should maintain entries sorted by timeout_at_ms
        use std::collections::BTreeMap;

        let mut timeout_queue: BTreeMap<i64, Vec<InFlightAction>> = BTreeMap::new();
        let instance_id = Uuid::new_v4();

        // Insert in non-sorted order
        timeout_queue.insert(
            3000,
            vec![InFlightAction {
                instance_id,
                node_id: "action_c".to_string(),
                timeout_at_ms: 3000,
                worker_idx: 2,
            }],
        );
        timeout_queue.insert(
            1000,
            vec![InFlightAction {
                instance_id,
                node_id: "action_a".to_string(),
                timeout_at_ms: 1000,
                worker_idx: 0,
            }],
        );
        timeout_queue.insert(
            2000,
            vec![InFlightAction {
                instance_id,
                node_id: "action_b".to_string(),
                timeout_at_ms: 2000,
                worker_idx: 1,
            }],
        );

        // Iteration should be in sorted order
        let keys: Vec<i64> = timeout_queue.keys().cloned().collect();
        assert_eq!(keys, vec![1000, 2000, 3000]);

        // First entry should be the earliest timeout
        let (first_key, first_actions) = timeout_queue.iter().next().unwrap();
        assert_eq!(*first_key, 1000);
        assert_eq!(first_actions[0].node_id, "action_a");
    }

    #[test]
    fn test_timeout_queue_multiple_actions_same_deadline() {
        // Multiple actions can timeout at the same time
        use std::collections::BTreeMap;

        let mut timeout_queue: BTreeMap<i64, Vec<InFlightAction>> = BTreeMap::new();
        let instance_id = Uuid::new_v4();

        let deadline = 5000_i64;

        // Add first action at deadline
        timeout_queue.insert(
            deadline,
            vec![InFlightAction {
                instance_id,
                node_id: "action_a".to_string(),
                timeout_at_ms: deadline,
                worker_idx: 0,
            }],
        );

        // Add second action at same deadline
        timeout_queue
            .entry(deadline)
            .or_default()
            .push(InFlightAction {
                instance_id,
                node_id: "action_b".to_string(),
                timeout_at_ms: deadline,
                worker_idx: 1,
            });

        // Should have both actions under same key
        let actions = timeout_queue.get(&deadline).unwrap();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].node_id, "action_a");
        assert_eq!(actions[1].node_id, "action_b");
    }

    #[test]
    fn test_timeout_lookup_and_removal() {
        // Test the reverse lookup pattern used in remove_from_timeout_tracking
        use std::collections::BTreeMap;

        let mut timeout_queue: BTreeMap<i64, Vec<InFlightAction>> = BTreeMap::new();
        let mut timeout_lookup: HashMap<(Uuid, String), i64> = HashMap::new();

        let instance_id = Uuid::new_v4();
        let timeout_at_ms = 5000_i64;

        // Add action to both structures (simulating dispatch)
        let action = InFlightAction {
            instance_id,
            node_id: "action_0".to_string(),
            timeout_at_ms,
            worker_idx: 0,
        };

        timeout_lookup.insert((instance_id, "action_0".to_string()), timeout_at_ms);
        timeout_queue.entry(timeout_at_ms).or_default().push(action);

        // Verify it exists
        assert!(timeout_lookup.contains_key(&(instance_id, "action_0".to_string())));
        assert_eq!(timeout_queue.get(&timeout_at_ms).unwrap().len(), 1);

        // Simulate removal (when action completes normally)
        if let Some(deadline) = timeout_lookup.remove(&(instance_id, "action_0".to_string()))
            && let Some(entries) = timeout_queue.get_mut(&deadline)
        {
            entries.retain(|e| !(e.instance_id == instance_id && e.node_id == "action_0"));
            if entries.is_empty() {
                timeout_queue.remove(&deadline);
            }
        }

        // Verify it's removed from both structures
        assert!(!timeout_lookup.contains_key(&(instance_id, "action_0".to_string())));
        assert!(!timeout_queue.contains_key(&timeout_at_ms));
    }

    #[test]
    fn test_timeout_expired_detection() {
        // Test the pattern used in check_action_timeouts to find expired entries
        use std::collections::BTreeMap;

        let mut timeout_queue: BTreeMap<i64, Vec<InFlightAction>> = BTreeMap::new();
        let instance_id = Uuid::new_v4();

        // Add actions with different deadlines
        for (i, deadline) in [1000_i64, 2000, 3000, 4000, 5000].iter().enumerate() {
            timeout_queue.insert(
                *deadline,
                vec![InFlightAction {
                    instance_id,
                    node_id: format!("action_{}", i),
                    timeout_at_ms: *deadline,
                    worker_idx: i,
                }],
            );
        }

        // Current time is 2500ms - actions at 1000 and 2000 should be expired
        let now_ms = 2500_i64;
        let expired_keys: Vec<i64> = timeout_queue.range(..=now_ms).map(|(k, _)| *k).collect();

        assert_eq!(expired_keys, vec![1000, 2000]);

        // Collect expired actions
        let mut expired_actions = Vec::new();
        for key in expired_keys {
            if let Some(entries) = timeout_queue.remove(&key) {
                expired_actions.extend(entries);
            }
        }

        assert_eq!(expired_actions.len(), 2);
        assert_eq!(expired_actions[0].node_id, "action_0");
        assert_eq!(expired_actions[1].node_id, "action_1");

        // Remaining queue should have actions at 3000, 4000, 5000
        assert_eq!(timeout_queue.len(), 3);
        assert!(timeout_queue.contains_key(&3000));
        assert!(timeout_queue.contains_key(&4000));
        assert!(timeout_queue.contains_key(&5000));
    }
}
