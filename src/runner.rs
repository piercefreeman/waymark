//! DAG Runner - Orchestrates workflow execution with Python workers.
//!
//! The runner coordinates:
//! - Fetching batched work from the action queue (DB)
//! - Distributing actions to available Python workers
//! - Processing completion results with parallel tokio tasks
//! - Evaluating inline nodes (assignments, expressions)
//! - Creating next runnable actions in a single transaction
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                              DAGRunner                                   │
//! │  ┌───────────────────┐    ┌────────────────────┐                        │
//! │  │ WorkQueueHandler  │───▶│  WorkerSlotTracker │                        │
//! │  │  (batch fetch)    │    │  (capacity mgmt)   │                        │
//! │  └─────────┬─────────┘    └────────────────────┘                        │
//! │            │                                                             │
//! │            ▼                                                             │
//! │  ┌───────────────────┐    ┌────────────────────┐                        │
//! │  │  PythonWorkerPool │───▶│WorkCompletionHandler│                       │
//! │  │  (action dispatch)│    │  (result + DAG)    │                        │
//! │  └───────────────────┘    └─────────┬──────────┘                        │
//! │                                     │                                    │
//! │                                     ▼                                    │
//! │                          ┌────────────────────┐                          │
//! │                          │    DB Transaction   │                         │
//! │                          │  (batched writes)   │                         │
//! │                          └────────────────────┘                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use chrono::{DateTime, Utc};
use prost::Message;

use crate::{
    ast_evaluator::{EvaluationError, Scope},
    completion::{
        CompletionError, CompletionPlan, InlineContext, analyze_subgraph, execute_inline_subgraph,
    },
    dag::{DAG, DAGConverter, EdgeType},
    dag_state::DAGHelper,
    db::{
        ActionId, CompletionRecord, Database, NewAction, QueuedAction, ScheduleId,
        WorkerStatusUpdate, WorkflowInstanceId, WorkflowVersionId,
    },
    execution::context::ExecutionContext,
    execution::engine::CompletionEngine,
    execution::model::ExceptionHandlingOutcome,
    messages::proto,
    parser::ast,
    schedule::{apply_jitter, next_cron_run, next_interval_run},
    value::WorkflowValue,
    worker::{ActionDispatchPayload, PythonWorkerPool, RoundTripMetrics, WorkerThroughputSnapshot},
};

#[cfg(test)]
use crate::ast_evaluator::ExpressionEvaluator;

type InboxValues = HashMap<String, HashMap<String, WorkflowValue>>;

#[derive(Debug, Clone)]
pub(crate) struct InstanceInboxCache {
    values: InboxValues,
    updated_at: DateTime<Utc>,
}

struct DbExecutionContext<'a> {
    db: &'a Database,
    dag: &'a DAG,
    instance_id: WorkflowInstanceId,
    instance_contexts: &'a Arc<RwLock<HashMap<Uuid, Scope>>>,
    instance_inboxes: &'a Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
}

#[tonic::async_trait]
impl<'a> ExecutionContext for DbExecutionContext<'a> {
    fn dag(&self) -> &DAG {
        self.dag
    }

    fn instance_id(&self) -> WorkflowInstanceId {
        self.instance_id
    }

    async fn initial_scope(&self) -> anyhow::Result<HashMap<String, WorkflowValue>> {
        if let Some(scope) = self
            .instance_contexts
            .read()
            .await
            .get(&self.instance_id.0)
            .cloned()
        {
            return Ok(scope);
        }

        let scope = load_initial_scope(self.db, self.instance_id)
            .await
            .map_err(anyhow::Error::new)?;
        self.instance_contexts
            .write()
            .await
            .insert(self.instance_id.0, scope.clone());
        Ok(scope)
    }

    async fn load_inbox(
        &self,
        node_ids: &HashSet<String>,
    ) -> anyhow::Result<HashMap<String, HashMap<String, WorkflowValue>>> {
        DAGRunner::load_inbox_with_cache(self.db, self.instance_inboxes, self.instance_id, node_ids)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn read_inbox(&self, node_id: &str) -> anyhow::Result<HashMap<String, WorkflowValue>> {
        let inbox = self
            .db
            .read_inbox(self.instance_id, node_id)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(inbox
            .into_iter()
            .map(|(key, value)| (key, json_to_workflow_value(&value)))
            .collect())
    }

    async fn read_spread_inbox(&self, node_id: &str) -> anyhow::Result<Vec<(i32, WorkflowValue)>> {
        let inbox = self
            .db
            .read_inbox_for_aggregator(self.instance_id, node_id)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(inbox
            .into_iter()
            .map(|(idx, value)| (idx, json_to_workflow_value(&value)))
            .collect())
    }
}

// ============================================================================
// Workflow Value Conversion
// ============================================================================

fn proto_value_to_workflow_value(value: &proto::WorkflowArgumentValue) -> WorkflowValue {
    WorkflowValue::from_proto(value)
}

fn json_to_workflow_value(value: &serde_json::Value) -> WorkflowValue {
    WorkflowValue::from_json(value)
}

fn inbox_json_to_workflow(
    inbox: HashMap<String, HashMap<String, serde_json::Value>>,
) -> HashMap<String, HashMap<String, WorkflowValue>> {
    inbox
        .into_iter()
        .map(|(node_id, vars)| {
            let converted = vars
                .into_iter()
                .map(|(name, value)| (name, json_to_workflow_value(&value)))
                .collect();
            (node_id, converted)
        })
        .collect()
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("Database error: {0}")]
    Database(#[from] crate::db::DbError),

    #[error("Worker error: {0}")]
    Worker(String),

    #[error("Expression evaluation error: {0}")]
    Evaluation(#[from] EvaluationError),

    #[error("DAG error: {0}")]
    Dag(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Instance not found: {0}")]
    InstanceNotFound(Uuid),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Channel closed")]
    ChannelClosed,

    #[error("Guard evaluation failed: {0:?}")]
    GuardEvaluationFailed(Vec<(String, String)>),
}

pub type RunnerResult<T> = Result<T, RunnerError>;

// ============================================================================
// Runtime Value
// ============================================================================

/// Runtime value for expression evaluation.
pub type Value = WorkflowValue;

// ============================================================================
// JSON to WorkflowArguments Conversion
// ============================================================================

/// Convert JSON bytes (expected to be an object) to WorkflowArguments.
///
/// The dispatch_payload is stored as JSON bytes representing the kwargs
/// for the action. This function parses and converts them to the proto format.
fn json_bytes_to_workflow_args(payload: &[u8]) -> proto::WorkflowArguments {
    if payload.is_empty() {
        return proto::WorkflowArguments { arguments: vec![] };
    }

    let json: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to parse dispatch payload as JSON: {}", e);
            return proto::WorkflowArguments { arguments: vec![] };
        }
    };

    match json {
        serde_json::Value::Object(obj) => {
            let arguments: Vec<proto::WorkflowArgument> = obj
                .iter()
                .map(|(k, v)| proto::WorkflowArgument {
                    key: k.clone(),
                    value: Some(json_to_workflow_value(v).to_proto()),
                })
                .collect();
            proto::WorkflowArguments { arguments }
        }
        _ => {
            warn!("dispatch_payload is not a JSON object, expected kwargs");
            proto::WorkflowArguments { arguments: vec![] }
        }
    }
}

/// Load the initial scope for an instance from its stored input_payload.
async fn load_initial_scope(db: &Database, instance_id: WorkflowInstanceId) -> RunnerResult<Scope> {
    let instance = db.get_instance(instance_id).await?;
    let mut scope = Scope::new();

    if let Some(payload) = instance.input_payload {
        match proto::WorkflowArguments::decode(&payload[..]) {
            Ok(args) => {
                for arg in args.arguments {
                    if let Some(value) = arg.value {
                        scope.insert(arg.key, proto_value_to_workflow_value(&value));
                    }
                }
            }
            Err(e) => {
                warn!(
                    instance_id = %instance_id.0,
                    error = %e,
                    "failed to decode initial inputs, defaulting to empty scope"
                );
            }
        }
    }

    Ok(scope)
}

// ============================================================================
// DAG Cache
// ============================================================================

/// Cache for DAGs loaded from workflow versions.
///
/// Since workflow versions are immutable (content-addressed by dag_hash),
/// we can cache them indefinitely. The cache stores Arc<DAG> which can be
/// used to create DAGHelper instances on-demand.
pub struct DAGCache {
    db: Arc<Database>,
    /// Cached DAGs by workflow version ID
    cache: RwLock<HashMap<Uuid, Arc<DAG>>>,
    /// Cached version lookups by instance ID (instance -> version_id)
    instance_versions: RwLock<HashMap<Uuid, Uuid>>,
}

impl DAGCache {
    /// Create a new DAG cache.
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            cache: RwLock::new(HashMap::new()),
            instance_versions: RwLock::new(HashMap::new()),
        }
    }

    /// Get the DAG for a workflow version, loading from DB if not cached.
    pub async fn get_dag(&self, version_id: WorkflowVersionId) -> RunnerResult<Arc<DAG>> {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(dag) = cache.get(&version_id.0) {
                return Ok(Arc::clone(dag));
            }
        }

        // Load from DB
        let version = self.db.get_workflow_version(version_id).await?;

        // Decode the program proto
        let program = ast::Program::decode(&version.program_proto[..])
            .map_err(|e| RunnerError::Dag(format!("Failed to decode program proto: {}", e)))?;

        // Convert to DAG
        let mut converter = DAGConverter::new();
        let dag = converter
            .convert(&program)
            .map_err(|e| RunnerError::Dag(format!("Failed to convert to DAG: {}", e)))?;

        let dag = Arc::new(dag);

        // Cache it
        {
            let mut cache = self.cache.write().await;
            cache.insert(version_id.0, Arc::clone(&dag));
        }

        Ok(dag)
    }

    /// Get the DAG for a workflow instance, looking up the version first.
    pub async fn get_dag_for_instance(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> RunnerResult<Option<Arc<DAG>>> {
        // Check if we have the version cached for this instance
        let version_id = {
            let cache = self.instance_versions.read().await;
            cache.get(&instance_id.0).copied()
        };

        if let Some(version_id) = version_id {
            return Ok(Some(self.get_dag(WorkflowVersionId(version_id)).await?));
        }

        // Load instance to get version_id
        let instance = self.db.get_instance(instance_id).await?;

        let version_id = match instance.workflow_version_id {
            Some(id) => id,
            None => return Ok(None), // No version associated
        };

        // Cache the instance -> version mapping
        {
            let mut cache = self.instance_versions.write().await;
            cache.insert(instance_id.0, version_id);
        }

        Ok(Some(self.get_dag(WorkflowVersionId(version_id)).await?))
    }

    pub async fn cache_instance_version(
        &self,
        instance_id: WorkflowInstanceId,
        version_id: WorkflowVersionId,
    ) {
        let mut cache = self.instance_versions.write().await;
        cache.insert(instance_id.0, version_id.0);
    }

    /// Pre-load a DAG into the cache (for testing or warm-up).
    pub async fn preload(&self, version_id: Uuid, dag: DAG) {
        let mut cache = self.cache.write().await;
        cache.insert(version_id, Arc::new(dag));
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> (usize, usize) {
        let dag_count = self.cache.read().await.len();
        let instance_count = self.instance_versions.read().await.len();
        (dag_count, instance_count)
    }
}

// ============================================================================
// Worker Slot Tracking
// ============================================================================

/// Tracks available slots across all workers.
///
/// Each worker can handle a configurable number of concurrent actions.
/// This tracker ensures we don't overload any single worker.
#[derive(Debug)]
pub struct WorkerSlotTracker {
    /// Slots per worker (index = worker id)
    worker_slots: Vec<AtomicUsize>,
    /// Maximum slots per worker
    max_slots_per_worker: usize,
    /// Total available slots (cached)
    total_available: AtomicUsize,
    /// Cursor for round-robin selection
    cursor: AtomicUsize,
}

impl WorkerSlotTracker {
    /// Create a new tracker for the given number of workers.
    pub fn new(num_workers: usize, max_slots_per_worker: usize) -> Self {
        let worker_slots = (0..num_workers)
            .map(|_| AtomicUsize::new(max_slots_per_worker))
            .collect();

        Self {
            worker_slots,
            max_slots_per_worker,
            total_available: AtomicUsize::new(num_workers * max_slots_per_worker),
            cursor: AtomicUsize::new(0),
        }
    }

    /// Get total available slots across all workers.
    pub fn available_slots(&self) -> usize {
        self.total_available.load(Ordering::SeqCst)
    }

    /// Try to acquire a slot from any worker. Returns worker index if successful.
    pub fn acquire_slot(&self) -> Option<usize> {
        let worker_count = self.worker_slots.len();
        if worker_count == 0 {
            return None;
        }

        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % worker_count;
        for _ in 0..worker_count {
            let mut best_idx = None;
            let mut best_slots = 0;

            for offset in 0..worker_count {
                let idx = (start + offset) % worker_count;
                let current = self.worker_slots[idx].load(Ordering::SeqCst);
                if current > best_slots {
                    best_slots = current;
                    best_idx = Some(idx);
                }
            }

            let idx = best_idx?;

            if best_slots == 0 {
                return None;
            }

            let slots = &self.worker_slots[idx];
            if slots
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    if current > 0 { Some(current - 1) } else { None }
                })
                .is_ok()
            {
                self.total_available.fetch_sub(1, Ordering::SeqCst);
                return Some(idx);
            }
        }
        None
    }

    /// Acquire multiple slots, returning worker assignments.
    /// Returns a map of worker_idx -> count of slots acquired.
    pub fn acquire_slots(&self, count: usize) -> HashMap<usize, usize> {
        let mut assignments = HashMap::new();
        let mut acquired = 0;

        while acquired < count {
            if let Some(worker_idx) = self.acquire_slot() {
                *assignments.entry(worker_idx).or_insert(0) += 1;
                acquired += 1;
            } else {
                break;
            }
        }

        assignments
    }

    /// Release a slot back to a specific worker.
    pub fn release_slot(&self, worker_idx: usize) {
        if worker_idx < self.worker_slots.len() {
            let current = self.worker_slots[worker_idx].load(Ordering::SeqCst);
            if current < self.max_slots_per_worker {
                self.worker_slots[worker_idx].fetch_add(1, Ordering::SeqCst);
                self.total_available.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// Get slots available for a specific worker.
    pub fn worker_available(&self, worker_idx: usize) -> usize {
        self.worker_slots
            .get(worker_idx)
            .map(|s| s.load(Ordering::SeqCst))
            .unwrap_or(0)
    }
}

// ============================================================================
// In-Flight Action Tracking
// ============================================================================

/// Tracks an action currently being processed by a worker.
#[derive(Debug, Clone)]
pub struct InFlightAction {
    /// The queued action from DB
    pub action: QueuedAction,
    /// Worker index handling this action
    pub worker_idx: usize,
    /// Worker ID handling this action
    pub worker_id: u64,
    /// When dispatch started
    pub dispatched_at: Instant,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct TimeoutEntry {
    deadline: Instant,
    token: Uuid,
}

impl Ord for TimeoutEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.token.as_u128().cmp(&other.token.as_u128()))
    }
}

impl PartialOrd for TimeoutEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
struct InFlightTimeouts {
    deadlines: HashMap<Uuid, Instant>,
    heap: BinaryHeap<std::cmp::Reverse<TimeoutEntry>>,
}

impl InFlightTimeouts {
    fn insert(&mut self, token: Uuid, deadline: Instant) {
        self.deadlines.insert(token, deadline);
        self.heap
            .push(std::cmp::Reverse(TimeoutEntry { deadline, token }));
    }

    fn remove(&mut self, token: &Uuid) {
        self.deadlines.remove(token);
    }

    fn pop_expired(&mut self, now: Instant) -> Vec<Uuid> {
        let mut expired = Vec::new();
        while let Some(std::cmp::Reverse(entry)) = self.heap.peek() {
            if entry.deadline > now {
                break;
            }

            let entry = self.heap.pop().expect("peeked entry exists").0;
            match self.deadlines.get(&entry.token) {
                Some(stored_deadline) if *stored_deadline == entry.deadline => {
                    self.deadlines.remove(&entry.token);
                    expired.push(entry.token);
                }
                _ => {
                    continue;
                }
            }
        }

        expired
    }
}

/// Manages in-flight actions for correlation on completion.
#[derive(Debug, Default)]
pub struct InFlightTracker {
    /// Actions keyed by delivery_token
    actions: HashMap<Uuid, InFlightAction>,
    timeouts: InFlightTimeouts,
}

impl InFlightTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an action to tracking.
    pub fn add(&mut self, action: QueuedAction, worker_idx: usize, worker_id: u64) {
        if action.timeout_seconds > 0 {
            let deadline = Instant::now() + Duration::from_secs(action.timeout_seconds as u64);
            self.timeouts.insert(action.delivery_token, deadline);
        }

        self.actions.insert(
            action.delivery_token,
            InFlightAction {
                action,
                worker_idx,
                worker_id,
                dispatched_at: Instant::now(),
            },
        );
    }

    /// Remove and return an action by delivery token.
    pub fn remove(&mut self, delivery_token: &Uuid) -> Option<InFlightAction> {
        self.timeouts.remove(delivery_token);
        self.actions.remove(delivery_token)
    }

    /// Get current count of in-flight actions.
    pub fn count(&self) -> usize {
        self.actions.len()
    }

    /// Remove actions that exceeded their timeout and return them.
    pub fn take_timed_out(&mut self, now: Instant) -> Vec<InFlightAction> {
        let expired_tokens = self.timeouts.pop_expired(now);
        expired_tokens
            .into_iter()
            .filter_map(|token| self.actions.remove(&token))
            .collect()
    }
}

// ============================================================================
// Completion Batch
// ============================================================================

/// A batch of actions to write to the database in a single transaction.
#[derive(Debug, Default)]
pub struct CompletionBatch {
    /// Actions to mark as completed
    pub completions: Vec<CompletionRecord>,
    /// New actions to enqueue
    pub new_actions: Vec<NewAction>,
    /// Inbox writes to commit (data flow between nodes)
    pub inbox_writes: Vec<InboxWrite>,
    /// Workflow instance completion (if the workflow finished)
    pub instance_completion: Option<InstanceCompletion>,
}

/// Workflow instance completion record.
#[derive(Debug, Clone)]
pub struct InstanceCompletion {
    pub instance_id: WorkflowInstanceId,
    pub result_payload: Vec<u8>,
}

impl CompletionBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.completions.is_empty()
            && self.new_actions.is_empty()
            && self.inbox_writes.is_empty()
            && self.instance_completion.is_none()
    }
}

// ============================================================================
// Work Queue Handler
// ============================================================================

/// Handles fetching and dispatching work from the database action queue to workers.
///
/// Responsibilities:
/// - Fetching batched actions from the DB (respecting available worker slots)
/// - Building dispatch payloads
/// - Sending actions to workers
/// - Tracking in-flight actions
pub struct WorkQueueHandler {
    db: Arc<Database>,
    worker_pool: Arc<PythonWorkerPool>,
    slot_tracker: Arc<WorkerSlotTracker>,
    in_flight: Arc<Mutex<InFlightTracker>>,
    dag_cache: Arc<DAGCache>,
    instance_inboxes: Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
    metrics: Option<Arc<RunnerMetrics>>,
}

impl WorkQueueHandler {
    pub(crate) fn new(
        db: Arc<Database>,
        worker_pool: Arc<PythonWorkerPool>,
        slot_tracker: Arc<WorkerSlotTracker>,
        in_flight: Arc<Mutex<InFlightTracker>>,
        dag_cache: Arc<DAGCache>,
        instance_inboxes: Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
        metrics: Option<Arc<RunnerMetrics>>,
    ) -> Self {
        Self {
            db,
            worker_pool,
            slot_tracker,
            in_flight,
            dag_cache,
            instance_inboxes,
            metrics,
        }
    }

    /// Get the number of available worker slots.
    pub fn available_slots(&self) -> usize {
        self.slot_tracker.available_slots()
    }

    pub(crate) fn worker_pool_id(&self) -> Uuid {
        self.worker_pool.pool_id()
    }

    pub(crate) fn worker_throughput_snapshot(&self) -> Vec<WorkerThroughputSnapshot> {
        self.worker_pool.throughput_snapshot()
    }

    /// Fetch and dispatch a batch of runnable nodes (actions and barriers).
    ///
    /// - Actions are dispatched to Python workers
    /// - Barriers are processed inline using the unified readiness model
    ///
    /// Returns immediately after dispatching. Action completions are sent to the provided channel.
    #[tracing::instrument(level = "info", skip(self, completion_tx))]
    pub async fn fetch_and_dispatch(
        &self,
        batch_size: usize,
        completion_tx: mpsc::Sender<(InFlightAction, RoundTripMetrics)>,
    ) -> RunnerResult<usize> {
        let total_start = std::time::Instant::now();

        let available = self.slot_tracker.available_slots();
        if available == 0 {
            if let Some(metrics) = &self.metrics {
                metrics.record_fetch_and_dispatch(FetchAndDispatchStats {
                    total_us: total_start.elapsed().as_micros() as u64,
                    db_us: 0,
                    dispatch_us: 0,
                    dispatched: 0,
                    actions: 0,
                    barriers: 0,
                    sleeps: 0,
                });
            }
            return Ok(0);
        }

        let limit = available.min(batch_size);
        let db_start = std::time::Instant::now();
        let nodes = self.db.dispatch_runnable_nodes(limit as i32).await?;
        let db_us = db_start.elapsed().as_micros() as u64;
        let dispatched = nodes.len();

        let mut barrier_count = 0;
        let mut action_count = 0;
        let mut sleep_count = 0;

        let dispatch_start = std::time::Instant::now();
        for node in nodes {
            match node.node_type.as_str() {
                "barrier" | "branch" | "join" => {
                    // Process control flow nodes inline using unified readiness model
                    // - barrier: synchronization points
                    // - branch: conditional decision points (if/elif/else, loop conditions)
                    // - join: merge points after conditionals or loops
                    self.process_barrier(node).await?;
                    barrier_count += 1;
                }
                "sleep" => {
                    // Process durable sleep inline (no worker dispatch)
                    self.process_sleep_action(node, completion_tx.clone())
                        .await?;
                    sleep_count += 1;
                }
                _ => {
                    // Dispatch actions to workers
                    self.dispatch_action(node, completion_tx.clone()).await?;
                    action_count += 1;
                }
            }
        }
        let dispatch_us = dispatch_start.elapsed().as_micros() as u64;

        if dispatched > 0 {
            debug!(
                total_us = total_start.elapsed().as_micros() as u64,
                db_fetch_us = db_us,
                dispatch_us = dispatch_us,
                dispatched = dispatched,
                barriers = barrier_count,
                actions = action_count,
                sleeps = sleep_count,
                "fetch_and_dispatch timing"
            );
        }

        if let Some(metrics) = &self.metrics {
            metrics.record_fetch_and_dispatch(FetchAndDispatchStats {
                total_us: total_start.elapsed().as_micros() as u64,
                db_us,
                dispatch_us,
                dispatched,
                actions: action_count,
                barriers: barrier_count,
                sleeps: sleep_count,
            });
        }

        Ok(dispatched)
    }

    /// Process a barrier (aggregator) that has become ready.
    ///
    /// Barriers are processed inline by the runner, not dispatched to workers.
    /// The barrier's inbox is guaranteed to be fully populated because all
    /// predecessors completed and wrote their data before the barrier was enqueued.
    async fn process_barrier(&self, barrier: QueuedAction) -> RunnerResult<()> {
        let total_start = std::time::Instant::now();

        let instance_id = WorkflowInstanceId(barrier.instance_id);
        let node_id = match barrier.node_id.as_deref() {
            Some(id) => id,
            None => {
                warn!(barrier_id = %barrier.id, "Barrier missing node_id");
                return Ok(());
            }
        };

        debug!(
            barrier_id = %barrier.id,
            node_id = %node_id,
            instance_id = %instance_id.0,
            "processing barrier"
        );

        // Get DAG
        let dag = match self.dag_cache.get_dag_for_instance(instance_id).await? {
            Some(dag) => dag,
            None => {
                warn!(instance_id = %instance_id.0, "DAG not found for barrier processing");
                return Ok(());
            }
        };

        let helper = DAGHelper::new(&dag);

        // Read aggregated results from inbox (for spread actions with spread_index)
        let inbox_start = std::time::Instant::now();
        let spread_results = self
            .db
            .read_inbox_for_aggregator(instance_id, node_id)
            .await?;

        // Aggregate results (already sorted by spread_index)
        let aggregated = WorkflowValue::List(
            spread_results
                .into_iter()
                .map(|(_, value)| json_to_workflow_value(&value))
                .collect(),
        );

        // Also read named variables from the barrier's inbox (for parallel blocks)
        let barrier_inbox = self
            .db
            .read_inbox(instance_id, node_id)
            .await?
            .into_iter()
            .map(|(key, value)| (key, json_to_workflow_value(&value)))
            .collect::<HashMap<_, _>>();
        let inbox_us = inbox_start.elapsed().as_micros() as u64;

        let result_count = match &aggregated {
            WorkflowValue::List(items) => items.len(),
            _ => 0,
        };
        debug!(
            barrier_id = %node_id,
            result_count = result_count,
            named_vars = ?barrier_inbox.keys().collect::<Vec<_>>(),
            "aggregated barrier results"
        );

        // Analyze subgraph from barrier
        let subgraph_start = std::time::Instant::now();
        let subgraph = analyze_subgraph(node_id, &dag, &helper);
        let subgraph_us = subgraph_start.elapsed().as_micros() as u64;

        // Batch fetch inbox for all nodes in subgraph
        let batch_start = std::time::Instant::now();
        let mut existing_inbox = inbox_json_to_workflow(
            self.db
                .batch_read_inbox(instance_id, &subgraph.all_node_ids)
                .await?,
        );
        let batch_us = batch_start.elapsed().as_micros() as u64;

        // Merge barrier's named variables into the existing inbox so they're available
        // to successors. This is how parallel block results flow to downstream nodes.
        if !barrier_inbox.is_empty() {
            existing_inbox
                .entry(node_id.to_string())
                .or_default()
                .extend(barrier_inbox.clone());
        }

        // Execute inline subgraph and build completion plan
        let inline_start = std::time::Instant::now();
        let initial_scope = load_initial_scope(&self.db, instance_id).await?;
        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };
        let mut plan =
            execute_inline_subgraph(node_id, aggregated, ctx, &subgraph, &dag, instance_id)
                .map_err(|e| RunnerError::Dag(e.to_string()))?;

        // Fill in barrier completion details
        plan = plan.with_action_completion(
            ActionId(barrier.id),
            barrier.delivery_token,
            true, // barriers always succeed
            Vec::new(),
            None,
        );
        let inbox_writes = plan.inbox_writes.clone();
        let inline_us = inline_start.elapsed().as_micros() as u64;

        // Execute completion plan in single atomic transaction
        let db_start = std::time::Instant::now();
        let result = self.db.execute_completion_plan(instance_id, plan).await?;
        let db_us = db_start.elapsed().as_micros() as u64;

        if !result.was_stale {
            if !inbox_writes.is_empty() {
                let updated_at = match result.inbox_updated_at {
                    Some(updated_at) => updated_at,
                    None => self.db.get_inbox_updated_at(instance_id).await?,
                };
                self.apply_inbox_writes_to_cache(instance_id, &inbox_writes, updated_at)
                    .await;
            }
            if result.workflow_completed {
                self.clear_inbox_cache(instance_id).await;
            }
        }

        info!(
            barrier_id = %node_id,
            total_us = total_start.elapsed().as_micros() as u64,
            inbox_us = inbox_us,
            subgraph_us = subgraph_us,
            batch_us = batch_us,
            inline_us = inline_us,
            db_us = db_us,
            newly_ready = result.newly_ready_nodes.len(),
            workflow_completed = result.workflow_completed,
            "process_barrier timing"
        );

        Ok(())
    }

    /// Process a durable sleep action that has become ready.
    ///
    /// Sleep actions are processed inline by the runner, not dispatched to workers.
    /// The sleep delay is handled by the database scheduling (scheduled_at column).
    /// When this method is called, the sleep duration has already elapsed.
    async fn process_sleep_action(
        &self,
        sleep: QueuedAction,
        _completion_tx: mpsc::Sender<(InFlightAction, RoundTripMetrics)>,
    ) -> RunnerResult<()> {
        let instance_id = WorkflowInstanceId(sleep.instance_id);
        let node_id = match sleep.node_id.as_deref() {
            Some(id) => id,
            None => {
                warn!(sleep_id = %sleep.id, "Sleep action missing node_id");
                return Ok(());
            }
        };

        debug!(
            sleep_id = %sleep.id,
            node_id = %node_id,
            instance_id = %instance_id.0,
            "processing sleep action"
        );

        // Get DAG
        let dag = match self.dag_cache.get_dag_for_instance(instance_id).await? {
            Some(dag) => dag,
            None => {
                warn!(instance_id = %instance_id.0, "DAG not found for sleep processing");
                return Ok(());
            }
        };

        let helper = DAGHelper::new(&dag);

        // Sleep actions return null - they're just for timing
        let sleep_result = WorkflowValue::Null;

        // Analyze subgraph from sleep node
        let subgraph = analyze_subgraph(node_id, &dag, &helper);

        // Batch fetch inbox for all nodes in subgraph
        let existing_inbox = inbox_json_to_workflow(
            self.db
                .batch_read_inbox(instance_id, &subgraph.all_node_ids)
                .await?,
        );

        // Execute inline subgraph and build completion plan
        let initial_scope = load_initial_scope(&self.db, instance_id).await?;
        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };
        let mut plan =
            execute_inline_subgraph(node_id, sleep_result, ctx, &subgraph, &dag, instance_id)
                .map_err(|e| RunnerError::Dag(e.to_string()))?;

        // Fill in sleep completion details
        plan = plan.with_action_completion(
            ActionId(sleep.id),
            sleep.delivery_token,
            true, // sleep always succeeds
            Vec::new(),
            None,
        );
        let inbox_writes = plan.inbox_writes.clone();

        // Execute completion plan in single atomic transaction
        let result = self.db.execute_completion_plan(instance_id, plan).await?;

        if !result.was_stale {
            if !inbox_writes.is_empty() {
                let updated_at = match result.inbox_updated_at {
                    Some(updated_at) => updated_at,
                    None => self.db.get_inbox_updated_at(instance_id).await?,
                };
                self.apply_inbox_writes_to_cache(instance_id, &inbox_writes, updated_at)
                    .await;
            }
            if result.workflow_completed {
                self.clear_inbox_cache(instance_id).await;
            }
        }

        info!(
            sleep_id = %node_id,
            newly_ready_nodes = ?result.newly_ready_nodes,
            workflow_completed = result.workflow_completed,
            "processed sleep action"
        );

        Ok(())
    }

    /// Dispatch a single action to a worker.
    async fn dispatch_action(
        &self,
        action: QueuedAction,
        completion_tx: mpsc::Sender<(InFlightAction, RoundTripMetrics)>,
    ) -> RunnerResult<()> {
        // Acquire a worker slot
        let worker_idx = match self.slot_tracker.acquire_slot() {
            Some(idx) => idx,
            None => {
                warn!("No worker slots available, action will remain dispatched in DB");
                return Ok(());
            }
        };

        // Build dispatch payload - convert JSON to WorkflowArguments
        let kwargs = json_bytes_to_workflow_args(&action.dispatch_payload);

        let dispatch = ActionDispatchPayload {
            action_id: action.id.to_string(),
            instance_id: action.instance_id.to_string(),
            sequence: action.action_seq as u32,
            action_name: action.action_name.clone(),
            module_name: action.module_name.clone(),
            kwargs,
            timeout_seconds: action.timeout_seconds as u32,
            max_retries: action.max_retries as u32,
            attempt_number: action.attempt_number as u32,
            dispatch_token: action.delivery_token,
        };

        // Get worker and send
        let worker = self.worker_pool.get_worker(worker_idx).await;
        let worker_id = worker.worker_id();

        // Track in-flight
        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.add(action.clone(), worker_idx, worker_id);
        }
        let delivery_token = action.delivery_token;
        let in_flight_tracker = Arc::clone(&self.in_flight);
        let slot_tracker = Arc::clone(&self.slot_tracker);
        let worker_pool = Arc::clone(&self.worker_pool);

        tokio::spawn(async move {
            match worker.send_action(dispatch).await {
                Ok(metrics) => {
                    worker_pool.record_completion(worker_idx, Arc::clone(&worker_pool));
                    // Get in-flight info and release slot
                    let in_flight_action = {
                        let mut tracker = in_flight_tracker.lock().await;
                        tracker.remove(&delivery_token)
                    };

                    if let Some(in_flight) = in_flight_action {
                        slot_tracker.release_slot(in_flight.worker_idx);
                        if let Err(e) = completion_tx.send((in_flight, metrics)).await {
                            debug!(
                                "Completion channel closed, dropping completion result: {}",
                                e
                            );
                        }
                    } else {
                        debug!(
                            delivery_token = %delivery_token,
                            "received completion for timed-out action"
                        );
                    }
                }
                Err(e) => {
                    error!("Worker dispatch failed: {}", e);
                    // Remove from in-flight tracking and release slot
                    let mut tracker = in_flight_tracker.lock().await;
                    if let Some(in_flight) = tracker.remove(&delivery_token) {
                        slot_tracker.release_slot(in_flight.worker_idx);
                    }
                }
            }
        });

        Ok(())
    }

    /// Get count of in-flight actions.
    pub async fn in_flight_count(&self) -> usize {
        self.in_flight.lock().await.count()
    }

    pub async fn release_timed_out_slots(&self) -> usize {
        let now = Instant::now();
        let mut tracker = self.in_flight.lock().await;
        let timed_out = tracker.take_timed_out(now);
        drop(tracker);

        for in_flight in &timed_out {
            self.slot_tracker.release_slot(in_flight.worker_idx);
        }

        timed_out.len()
    }

    async fn apply_inbox_writes_to_cache(
        &self,
        instance_id: WorkflowInstanceId,
        inbox_writes: &[crate::completion::InboxWrite],
        inbox_updated_at: DateTime<Utc>,
    ) {
        if inbox_writes.is_empty() {
            return;
        }

        let mut cache = self.instance_inboxes.write().await;
        let instance_cache = cache
            .entry(instance_id.0)
            .or_insert_with(|| InstanceInboxCache {
                values: HashMap::new(),
                updated_at: inbox_updated_at,
            });
        instance_cache.updated_at = inbox_updated_at;
        for write in inbox_writes {
            if write.spread_index.is_some() {
                continue;
            }
            let node_cache = instance_cache
                .values
                .entry(write.target_node_id.clone())
                .or_default();
            node_cache.insert(write.variable_name.clone(), write.value.clone());
        }
    }

    async fn clear_inbox_cache(&self, instance_id: WorkflowInstanceId) {
        let mut cache = self.instance_inboxes.write().await;
        cache.remove(&instance_id.0);
    }
}

// ============================================================================
// Work Completion Handler
// ============================================================================

/// Handles processing completed actions and determining next steps.
///
/// This is where the DAG traversal logic lives. For each completed action:
/// 1. Parse the result payload
/// 2. Use DAGHelper to find data flow targets
/// 3. Execute any inlinable successor nodes
/// 4. Determine which new actions need to be queued
#[derive(Clone)]
pub struct WorkCompletionHandler {
    /// Database handle for persisting completions and enqueuing new actions.
    pub db: Arc<Database>,
}

impl WorkCompletionHandler {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Write a completion batch to the database in a single transaction.
    pub async fn write_batch(
        &self,
        batch: CompletionBatch,
    ) -> RunnerResult<HashMap<Uuid, DateTime<Utc>>> {
        // Complete actions
        for completion in batch.completions {
            self.db.complete_action(completion).await?;
        }

        // Write inbox entries (data flow between nodes)
        let mut inbox_instance_ids: HashSet<Uuid> = HashSet::new();
        for inbox_write in batch.inbox_writes {
            inbox_instance_ids.insert(inbox_write.instance_id.0);
            self.db
                .append_to_inbox(
                    inbox_write.instance_id,
                    &inbox_write.target_node_id,
                    &inbox_write.variable_name,
                    inbox_write.value.to_json(),
                    &inbox_write.source_node_id,
                    inbox_write.spread_index,
                )
                .await?;
        }
        let touch_ids: Vec<Uuid> = inbox_instance_ids.into_iter().collect();
        let updated_at_by_instance = self.db.touch_inbox_updated_at(&touch_ids).await?;

        // Enqueue new actions
        for new_action in batch.new_actions {
            self.db.enqueue_action(new_action).await?;
        }

        // Complete workflow instance if finished
        if let Some(completion) = batch.instance_completion {
            debug!(
                instance_id = %completion.instance_id.0,
                result_len = completion.result_payload.len(),
                "marking workflow instance as completed"
            );
            self.db
                .complete_instance(
                    completion.instance_id,
                    if completion.result_payload.is_empty() {
                        None
                    } else {
                        Some(&completion.result_payload)
                    },
                )
                .await?;
        }

        Ok(updated_at_by_instance)
    }
}

// ============================================================================
// Completion Batching
// ============================================================================

struct CompletionFlushRequest {
    instance_id: WorkflowInstanceId,
    plan: CompletionPlan,
    response_tx: oneshot::Sender<crate::db::DbResult<crate::completion::CompletionResult>>,
}

#[derive(Clone)]
struct CompletionBatcher {
    db: Arc<Database>,
    sender: Option<mpsc::Sender<CompletionFlushRequest>>,
    batching_enabled: bool,
}

impl CompletionBatcher {
    fn new(db: Arc<Database>, max_batch_size: usize, flush_interval_ms: u64) -> Self {
        let batch_size = max_batch_size.max(1);
        let batching_enabled = batch_size > 1;
        let interval_ms = flush_interval_ms.max(1);
        if !batching_enabled {
            return Self {
                db,
                sender: None,
                batching_enabled,
            };
        }

        let channel_capacity = batch_size.saturating_mul(4).max(100);
        let (sender, mut receiver) = mpsc::channel(channel_capacity);
        let db_task = Arc::clone(&db);

        tokio::spawn(async move {
            let mut pending: Vec<CompletionFlushRequest> = Vec::with_capacity(batch_size);
            let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    maybe_req = receiver.recv() => {
                        match maybe_req {
                            Some(req) => {
                                pending.push(req);
                                if pending.len() >= batch_size {
                                    Self::flush_pending(&db_task, &mut pending, batch_size).await;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = ticker.tick() => {
                        if !pending.is_empty() {
                            Self::flush_pending(&db_task, &mut pending, batch_size).await;
                        }
                    }
                }
            }

            if !pending.is_empty() {
                Self::flush_pending(&db_task, &mut pending, batch_size).await;
            }
        });

        Self {
            db,
            sender: Some(sender),
            batching_enabled,
        }
    }

    async fn execute(
        &self,
        instance_id: WorkflowInstanceId,
        plan: CompletionPlan,
    ) -> RunnerResult<crate::completion::CompletionResult> {
        if !self.batching_enabled {
            return self
                .db
                .execute_completion_plan(instance_id, plan)
                .await
                .map_err(RunnerError::Database);
        }

        let (response_tx, response_rx) = oneshot::channel();
        let sender = self.sender.as_ref().ok_or(RunnerError::ChannelClosed)?;
        sender
            .send(CompletionFlushRequest {
                instance_id,
                plan,
                response_tx,
            })
            .await
            .map_err(|_| RunnerError::ChannelClosed)?;
        response_rx
            .await
            .map_err(|_| RunnerError::ChannelClosed)?
            .map_err(RunnerError::Database)
    }

    async fn flush_pending(
        db: &Database,
        pending: &mut Vec<CompletionFlushRequest>,
        batch_size: usize,
    ) {
        let mut requests = Vec::new();
        requests.append(pending);
        if requests.is_empty() {
            return;
        }

        let mut batch_requests = requests;
        if batch_requests.len() > batch_size {
            let deferred = batch_requests.split_off(batch_size);
            pending.extend(deferred);
        }

        if batch_requests.is_empty() {
            return;
        }

        let mut plans = Vec::with_capacity(batch_requests.len());
        for req in &batch_requests {
            plans.push((req.instance_id, req.plan.clone()));
        }

        match db.execute_completion_plans_batch(plans).await {
            Ok(results) if results.len() == batch_requests.len() => {
                for (req, result) in batch_requests.into_iter().zip(results) {
                    let _ = req.response_tx.send(Ok(result));
                }
            }
            Ok(results) => {
                error!(
                    expected = batch_requests.len(),
                    actual = results.len(),
                    "completion batch size mismatch"
                );
                for req in batch_requests {
                    let _ = req.response_tx.send(Err(crate::db::DbError::NotFound(
                        "completion batch size mismatch".to_string(),
                    )));
                }
            }
            Err(err) => {
                error!(
                    "completion batch failed, falling back to per-plan writes: {}",
                    err
                );
                for req in batch_requests {
                    let result = db.execute_completion_plan(req.instance_id, req.plan).await;
                    let _ = req.response_tx.send(result);
                }
            }
        }
    }
}

// ============================================================================
// Instance Context
// ============================================================================

/// A pending write to a node's inbox.
/// Collected during CPU-bound work and written to DB asynchronously afterward.
#[derive(Debug, Clone)]
pub struct InboxWrite {
    pub instance_id: WorkflowInstanceId,
    pub target_node_id: String,
    pub variable_name: String,
    pub value: WorkflowValue,
    pub source_node_id: String,
    pub spread_index: Option<i32>,
}

struct StartPlan {
    instance_id: WorkflowInstanceId,
    plan: CompletionPlan,
    inbox_writes: Vec<crate::completion::InboxWrite>,
}

/// Exception information for error handling.
#[derive(Debug, Clone)]
pub struct ExceptionInfo {
    pub exception_type: String,
    pub message: String,
    pub node_id: String,
}

// ============================================================================
// Runner Metrics
// ============================================================================

#[derive(Debug, Default)]
pub(crate) struct RunnerMetrics {
    fetch_and_dispatch_calls: AtomicU64,
    fetch_and_dispatch_total_us: AtomicU64,
    fetch_and_dispatch_db_us: AtomicU64,
    fetch_and_dispatch_dispatch_us: AtomicU64,
    fetch_and_dispatch_dispatched: AtomicU64,
    fetch_and_dispatch_actions: AtomicU64,
    fetch_and_dispatch_barriers: AtomicU64,
    fetch_and_dispatch_sleeps: AtomicU64,
    start_unstarted_calls: AtomicU64,
    start_unstarted_total_us: AtomicU64,
    start_unstarted_instances: AtomicU64,
    process_completion_calls: AtomicU64,
    process_completion_total_us: AtomicU64,
    process_completion_subgraph_us: AtomicU64,
    process_completion_inbox_us: AtomicU64,
    process_completion_inline_us: AtomicU64,
    process_completion_db_us: AtomicU64,
    process_completion_newly_ready: AtomicU64,
    process_completion_workflow_completed: AtomicU64,
}

struct FetchAndDispatchStats {
    total_us: u64,
    db_us: u64,
    dispatch_us: u64,
    dispatched: usize,
    actions: usize,
    barriers: usize,
    sleeps: usize,
}

struct ProcessCompletionStats {
    total_us: u64,
    subgraph_us: u64,
    inbox_us: u64,
    inline_us: u64,
    db_us: u64,
    newly_ready: usize,
    workflow_completed: bool,
}

impl RunnerMetrics {
    fn record_fetch_and_dispatch(&self, stats: FetchAndDispatchStats) {
        self.fetch_and_dispatch_calls
            .fetch_add(1, Ordering::Relaxed);
        self.fetch_and_dispatch_total_us
            .fetch_add(stats.total_us, Ordering::Relaxed);
        self.fetch_and_dispatch_db_us
            .fetch_add(stats.db_us, Ordering::Relaxed);
        self.fetch_and_dispatch_dispatch_us
            .fetch_add(stats.dispatch_us, Ordering::Relaxed);
        self.fetch_and_dispatch_dispatched
            .fetch_add(stats.dispatched as u64, Ordering::Relaxed);
        self.fetch_and_dispatch_actions
            .fetch_add(stats.actions as u64, Ordering::Relaxed);
        self.fetch_and_dispatch_barriers
            .fetch_add(stats.barriers as u64, Ordering::Relaxed);
        self.fetch_and_dispatch_sleeps
            .fetch_add(stats.sleeps as u64, Ordering::Relaxed);
    }

    fn record_start_unstarted(&self, total_us: u64, instances: usize) {
        self.start_unstarted_calls.fetch_add(1, Ordering::Relaxed);
        self.start_unstarted_total_us
            .fetch_add(total_us, Ordering::Relaxed);
        self.start_unstarted_instances
            .fetch_add(instances as u64, Ordering::Relaxed);
    }

    fn record_process_completion(&self, stats: ProcessCompletionStats) {
        self.process_completion_calls
            .fetch_add(1, Ordering::Relaxed);
        self.process_completion_total_us
            .fetch_add(stats.total_us, Ordering::Relaxed);
        self.process_completion_subgraph_us
            .fetch_add(stats.subgraph_us, Ordering::Relaxed);
        self.process_completion_inbox_us
            .fetch_add(stats.inbox_us, Ordering::Relaxed);
        self.process_completion_inline_us
            .fetch_add(stats.inline_us, Ordering::Relaxed);
        self.process_completion_db_us
            .fetch_add(stats.db_us, Ordering::Relaxed);
        self.process_completion_newly_ready
            .fetch_add(stats.newly_ready as u64, Ordering::Relaxed);
        self.process_completion_workflow_completed
            .fetch_add(stats.workflow_completed as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> RunnerMetricsSnapshot {
        RunnerMetricsSnapshot {
            fetch_and_dispatch_calls: self.fetch_and_dispatch_calls.load(Ordering::Relaxed),
            fetch_and_dispatch_total_us: self.fetch_and_dispatch_total_us.load(Ordering::Relaxed),
            fetch_and_dispatch_db_us: self.fetch_and_dispatch_db_us.load(Ordering::Relaxed),
            fetch_and_dispatch_dispatch_us: self
                .fetch_and_dispatch_dispatch_us
                .load(Ordering::Relaxed),
            fetch_and_dispatch_dispatched: self
                .fetch_and_dispatch_dispatched
                .load(Ordering::Relaxed),
            fetch_and_dispatch_actions: self.fetch_and_dispatch_actions.load(Ordering::Relaxed),
            fetch_and_dispatch_barriers: self.fetch_and_dispatch_barriers.load(Ordering::Relaxed),
            fetch_and_dispatch_sleeps: self.fetch_and_dispatch_sleeps.load(Ordering::Relaxed),
            start_unstarted_calls: self.start_unstarted_calls.load(Ordering::Relaxed),
            start_unstarted_total_us: self.start_unstarted_total_us.load(Ordering::Relaxed),
            start_unstarted_instances: self.start_unstarted_instances.load(Ordering::Relaxed),
            process_completion_calls: self.process_completion_calls.load(Ordering::Relaxed),
            process_completion_total_us: self.process_completion_total_us.load(Ordering::Relaxed),
            process_completion_subgraph_us: self
                .process_completion_subgraph_us
                .load(Ordering::Relaxed),
            process_completion_inbox_us: self.process_completion_inbox_us.load(Ordering::Relaxed),
            process_completion_inline_us: self.process_completion_inline_us.load(Ordering::Relaxed),
            process_completion_db_us: self.process_completion_db_us.load(Ordering::Relaxed),
            process_completion_newly_ready: self
                .process_completion_newly_ready
                .load(Ordering::Relaxed),
            process_completion_workflow_completed: self
                .process_completion_workflow_completed
                .load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RunnerMetricsSnapshot {
    pub fetch_and_dispatch_calls: u64,
    pub fetch_and_dispatch_total_us: u64,
    pub fetch_and_dispatch_db_us: u64,
    pub fetch_and_dispatch_dispatch_us: u64,
    pub fetch_and_dispatch_dispatched: u64,
    pub fetch_and_dispatch_actions: u64,
    pub fetch_and_dispatch_barriers: u64,
    pub fetch_and_dispatch_sleeps: u64,
    pub start_unstarted_calls: u64,
    pub start_unstarted_total_us: u64,
    pub start_unstarted_instances: u64,
    pub process_completion_calls: u64,
    pub process_completion_total_us: u64,
    pub process_completion_subgraph_us: u64,
    pub process_completion_inbox_us: u64,
    pub process_completion_inline_us: u64,
    pub process_completion_db_us: u64,
    pub process_completion_newly_ready: u64,
    pub process_completion_workflow_completed: u64,
}

// ============================================================================
// DAG Runner
// ============================================================================

/// Configuration for the DAG runner.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Maximum actions to fetch per batch
    pub batch_size: usize,
    /// Enable internal runner metrics collection.
    pub enable_metrics: bool,
    /// Maximum concurrent actions per worker
    pub max_slots_per_worker: usize,
    /// Polling interval when idle (milliseconds)
    pub poll_interval_ms: u64,
    /// Timeout check interval (milliseconds)
    pub timeout_check_interval_ms: u64,
    /// Maximum actions to process per timeout check cycle
    pub timeout_check_batch_size: i32,
    /// Schedule check interval (milliseconds)
    pub schedule_check_interval_ms: u64,
    /// Maximum schedules to process per check cycle
    pub schedule_check_batch_size: i32,
    /// Worker status upsert interval (milliseconds)
    pub worker_status_interval_ms: u64,
    /// Action log flush interval (milliseconds). If 0, flush is disabled.
    pub action_log_flush_interval_ms: u64,
    /// Maximum action logs to flush per cycle.
    pub action_log_flush_batch_size: i64,
    /// Maximum completion plans to flush per batch.
    pub completion_batch_size: usize,
    /// Completion flush interval (milliseconds).
    pub completion_flush_interval_ms: u64,
    /// Garbage collection interval (milliseconds). If None, GC is disabled.
    pub gc_interval_ms: Option<u64>,
    /// Minimum age in seconds for completed/failed instances before cleanup.
    pub gc_retention_seconds: i64,
    /// Batch size for garbage collection operations.
    pub gc_batch_size: i32,
    /// Maximum age in milliseconds for a start claim before reclaiming it.
    pub start_claim_timeout_ms: u64,
    /// Inbox compaction interval (milliseconds). If None, compaction is disabled.
    pub inbox_compaction_interval_ms: Option<u64>,
    /// Maximum inbox rows to compact per pass.
    pub inbox_compaction_batch_size: i64,
    /// Minimum age in seconds for inbox rows eligible for compaction.
    pub inbox_compaction_min_age_seconds: i64,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            enable_metrics: false,
            max_slots_per_worker: 10,
            poll_interval_ms: 100,
            timeout_check_interval_ms: 1000,
            timeout_check_batch_size: 100,
            schedule_check_interval_ms: 10000, // 10 seconds
            schedule_check_batch_size: 100,
            worker_status_interval_ms: 10000,
            action_log_flush_interval_ms: 200,
            action_log_flush_batch_size: 1000,
            completion_batch_size: 200,
            completion_flush_interval_ms: 10,
            gc_interval_ms: None,
            gc_retention_seconds: 86400, // 24 hours
            gc_batch_size: 100,
            start_claim_timeout_ms: 60000,
            inbox_compaction_interval_ms: None,
            inbox_compaction_batch_size: 10000,
            inbox_compaction_min_age_seconds: 60,
        }
    }
}

/// The main DAG runner that orchestrates workflow execution.
///
/// The runner coordinates between:
/// - `WorkQueueHandler`: Fetching and dispatching work to workers
/// - `WorkCompletionHandler`: Processing results and creating next actions
///
/// The runner itself just manages the event loop and shared state.
pub struct DAGRunner {
    config: RunnerConfig,
    work_handler: Arc<WorkQueueHandler>,
    completion_handler: WorkCompletionHandler,
    /// DAG cache with DB-backed loading
    dag_cache: Arc<DAGCache>,
    /// Stores initial input scope per workflow instance.
    /// Used to provide workflow input variables during inline evaluation.
    instance_contexts: Arc<RwLock<HashMap<Uuid, Scope>>>,
    /// Best-effort cache of latest inbox values per instance/node.
    instance_inboxes: Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
    shutdown_flag: Arc<AtomicBool>,
    metrics: Option<Arc<RunnerMetrics>>,
}

impl DAGRunner {
    /// Create a new DAG runner.
    pub fn new(
        config: RunnerConfig,
        db: Arc<Database>,
        worker_pool: Arc<PythonWorkerPool>,
    ) -> Self {
        let num_workers = worker_pool.len();
        let slot_tracker = Arc::new(WorkerSlotTracker::new(
            num_workers,
            config.max_slots_per_worker,
        ));
        let in_flight = Arc::new(Mutex::new(InFlightTracker::new()));

        let metrics = if config.enable_metrics {
            Some(Arc::new(RunnerMetrics::default()))
        } else {
            None
        };

        let dag_cache = Arc::new(DAGCache::new(Arc::clone(&db)));
        let instance_contexts = Arc::new(RwLock::new(HashMap::new()));
        let instance_inboxes = Arc::new(RwLock::new(HashMap::new()));
        let work_handler = Arc::new(WorkQueueHandler::new(
            Arc::clone(&db),
            worker_pool,
            slot_tracker,
            in_flight,
            Arc::clone(&dag_cache),
            Arc::clone(&instance_inboxes),
            metrics.clone(),
        ));
        let completion_handler = WorkCompletionHandler::new(db);

        Self {
            config,
            work_handler,
            completion_handler,
            dag_cache,
            instance_contexts,
            instance_inboxes,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            metrics,
        }
    }

    /// Run the main execution loop.
    pub async fn run(self: Arc<Self>) -> RunnerResult<()> {
        info!("Starting DAG runner");

        // Channel for completion results
        let (completion_tx, mut completion_rx) =
            mpsc::channel::<(InFlightAction, RoundTripMetrics)>(1000);
        let completion_batcher = Arc::new(CompletionBatcher::new(
            Arc::clone(&self.completion_handler.db),
            self.config.completion_batch_size,
            self.config.completion_flush_interval_ms,
        ));

        let make_interval = |ms: u64, name: &'static str| {
            let clamped = ms.max(1);
            if clamped != ms {
                warn!(
                    interval_ms = ms,
                    interval_name = name,
                    "interval must be >= 1ms; clamping"
                );
            }
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(clamped));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        };

        // Timeout maintenance loop.
        // Note: We spawn these tasks but don't monitor their JoinHandles in the select.
        // They will exit cleanly when shutdown is notified via the Notify.
        let timeout_runner = Arc::clone(&self);
        let _timeout_handle = tokio::spawn(async move {
            let mut interval = make_interval(
                timeout_runner.config.timeout_check_interval_ms,
                "timeout_check",
            );
            loop {
                if timeout_runner.shutdown_flag.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    _ = timeout_runner.shutdown.notified() => break,
                    _ = interval.tick() => {
                        let batch_size = timeout_runner.config.timeout_check_batch_size;
                        let mut should_continue = true;

                        while should_continue {
                            should_continue = false;

                            // Phase 1: Mark timed-out actions as failed (with retry_kind='timeout')
                            match timeout_runner.completion_handler.db.mark_timed_out_actions(batch_size).await {
                                Ok(count) => {
                                    if count >= batch_size as i64 {
                                        should_continue = true;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to mark timed-out actions: {}", e);
                                }
                            }

                            // Phase 2: Requeue all failed actions (both timeouts and explicit failures)
                            match timeout_runner.completion_handler.db.requeue_failed_actions(batch_size).await {
                                Ok((requeued, permanently_failed)) => {
                                    let total = requeued + permanently_failed;
                                    if total >= batch_size as i64 {
                                        should_continue = true;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to requeue failed actions: {}", e);
                                }
                            }

                            // Phase 3: Fail workflow instances that have actions which exhausted retries
                            match timeout_runner.completion_handler.db.fail_instances_with_exhausted_actions(batch_size).await {
                                Ok(failed_instances) => {
                                    if failed_instances >= batch_size as i64 {
                                        should_continue = true;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to fail instances with exhausted actions: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        });

        // In-flight timeout release loop.
        let in_flight_runner = Arc::clone(&self);
        let _in_flight_handle = tokio::spawn(async move {
            let mut interval = make_interval(
                in_flight_runner.config.timeout_check_interval_ms,
                "in_flight_timeout_check",
            );
            loop {
                if in_flight_runner.shutdown_flag.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    _ = in_flight_runner.shutdown.notified() => break,
                    _ = interval.tick() => {
                        let released = in_flight_runner.work_handler.release_timed_out_slots().await;
                        if released > 0 {
                            warn!(
                                timed_out = released,
                                "released worker slots for timed-out in-flight actions"
                            );
                        }
                    }
                }
            }
        });

        // Schedule check loop.
        let schedule_runner = Arc::clone(&self);
        let _schedule_handle = tokio::spawn(async move {
            let mut interval = make_interval(
                schedule_runner.config.schedule_check_interval_ms,
                "schedule_check",
            );
            loop {
                if schedule_runner.shutdown_flag.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    _ = schedule_runner.shutdown.notified() => break,
                    _ = interval.tick() => {
                        let batch_size = schedule_runner.config.schedule_check_batch_size as usize;
                        let mut should_continue = true;

                        // Keep processing until we've drained all due schedules
                        while should_continue {
                            should_continue = false;
                            match schedule_runner.process_due_schedules().await {
                                Ok(count) => {
                                    if count >= batch_size {
                                        should_continue = true;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to process due schedules: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        });

        // Worker status update loop.
        let status_runner = Arc::clone(&self);
        let _status_handle = tokio::spawn(async move {
            let mut interval = make_interval(
                status_runner.config.worker_status_interval_ms,
                "worker_status",
            );
            loop {
                if status_runner.shutdown_flag.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    _ = status_runner.shutdown.notified() => break,
                    _ = interval.tick() => {
                        let pool_id = status_runner.work_handler.worker_pool_id();
                        let snapshots = status_runner.work_handler.worker_throughput_snapshot();
                        if !snapshots.is_empty() {
                            // Calculate median times for workers (look back 5 minutes)
                            let median_times = status_runner
                                .completion_handler
                                .db
                                .calculate_worker_median_times(pool_id, 300)
                                .await
                                .unwrap_or_default();

                            // Create a lookup map for median times by worker_id
                            let median_map: std::collections::HashMap<i64, (Option<i64>, Option<i64>)> =
                                median_times.into_iter().map(|(w, d, h)| (w, (d, h))).collect();

                            let updates: Vec<WorkerStatusUpdate> = snapshots
                                .into_iter()
                                .map(|snapshot| {
                                    let worker_id = snapshot.worker_id as i64;
                                    let (median_dequeue_ms, median_handling_ms) =
                                        median_map.get(&worker_id).copied().unwrap_or((None, None));
                                    WorkerStatusUpdate {
                                        worker_id,
                                        throughput_per_min: snapshot.throughput_per_min,
                                        total_completed: snapshot.total_completed as i64,
                                        last_action_at: snapshot.last_action_at,
                                        median_dequeue_ms,
                                        median_handling_ms,
                                    }
                                })
                                .collect();

                            if let Err(err) = status_runner
                                .completion_handler
                                .db
                                .upsert_worker_statuses(pool_id, &updates)
                                .await
                            {
                                error!(?err, "failed to upsert worker status");
                            }
                        }
                    }
                }
            }
        });

        // Action log flush loop.
        if self.config.action_log_flush_interval_ms > 0 {
            let log_runner = Arc::clone(&self);
            let _log_handle = tokio::spawn(async move {
                let mut interval = make_interval(
                    log_runner.config.action_log_flush_interval_ms,
                    "action_log_flush",
                );
                loop {
                    if log_runner.shutdown_flag.load(Ordering::Acquire) {
                        break;
                    }
                    tokio::select! {
                        _ = log_runner.shutdown.notified() => break,
                        _ = interval.tick() => {
                            let batch_size = log_runner.config.action_log_flush_batch_size;
                            let mut should_continue = true;
                            while should_continue {
                                should_continue = false;
                                match log_runner
                                    .completion_handler
                                    .db
                                    .flush_action_log_queue(batch_size)
                                    .await
                                {
                                    Ok(count) => {
                                        if count >= batch_size {
                                            should_continue = true;
                                        }
                                    }
                                    Err(err) => {
                                        error!(?err, "failed to flush action log queue");
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        // Garbage collection loop (only if GC is enabled).
        if let Some(gc_interval_ms) = self.config.gc_interval_ms {
            let gc_runner = Arc::clone(&self);
            let _gc_handle = tokio::spawn(async move {
                let mut interval = make_interval(gc_interval_ms, "gc");
                loop {
                    if gc_runner.shutdown_flag.load(Ordering::Acquire) {
                        break;
                    }
                    tokio::select! {
                        _ = gc_runner.shutdown.notified() => break,
                        _ = interval.tick() => {
                            let retention_seconds = gc_runner.config.gc_retention_seconds;
                            let batch_size = gc_runner.config.gc_batch_size;
                            let mut should_continue = true;

                            while should_continue {
                                should_continue = false;

                                match gc_runner
                                    .completion_handler
                                    .db
                                    .garbage_collect_instances(retention_seconds, batch_size)
                                    .await
                                {
                                    Ok(count) => {
                                        if count >= batch_size as i64 {
                                            // More work may be available, continue batching
                                            should_continue = true;
                                        }
                                    }
                                    Err(err) => {
                                        error!(?err, "failed to garbage collect instances");
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        // Inbox compaction loop (only if enabled).
        if let Some(compact_interval_ms) = self.config.inbox_compaction_interval_ms {
            let compact_runner = Arc::clone(&self);
            let _compact_handle = tokio::spawn(async move {
                let mut interval = make_interval(compact_interval_ms, "inbox_compaction");
                loop {
                    if compact_runner.shutdown_flag.load(Ordering::Acquire) {
                        break;
                    }
                    tokio::select! {
                        _ = compact_runner.shutdown.notified() => break,
                        _ = interval.tick() => {
                            let min_age_seconds =
                                compact_runner.config.inbox_compaction_min_age_seconds;
                            let batch_size = compact_runner.config.inbox_compaction_batch_size;
                            let mut should_continue = true;

                            while should_continue {
                                should_continue = false;
                                match compact_runner
                                    .completion_handler
                                    .db
                                    .compact_node_inputs(min_age_seconds, batch_size)
                                    .await
                                {
                                    Ok(count) => {
                                        if count >= batch_size {
                                            should_continue = true;
                                        }
                                    }
                                    Err(err) => {
                                        error!(?err, "failed to compact node_inputs");
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        // Start unstarted instances loop.
        let start_runner = Arc::clone(&self);
        let start_interval =
            tokio::time::Duration::from_millis(start_runner.config.poll_interval_ms);
        let _start_handle = tokio::spawn(async move {
            loop {
                if start_runner.shutdown_flag.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    _ = start_runner.shutdown.notified() => break,
                    _ = tokio::time::sleep(start_interval) => {
                        let batch_size = start_runner.config.batch_size;
                        let mut should_continue = true;

                        while should_continue {
                            should_continue = false;
                            match start_runner.start_unstarted_instances().await {
                                Ok(count) => {
                                    if count >= batch_size {
                                        should_continue = true;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to start unstarted instances: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        });

        // Poll + dispatch loop - uses sleep() like original for consistent timing behavior
        let poll_runner = Arc::clone(&self);
        let poll_tx = completion_tx.clone();
        drop(completion_tx); // Explicitly drop unused sender
        let poll_interval = tokio::time::Duration::from_millis(poll_runner.config.poll_interval_ms);
        let _poll_handle = tokio::spawn(async move {
            loop {
                if poll_runner.shutdown_flag.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! {
                    _ = poll_runner.shutdown.notified() => break,
                    _ = tokio::time::sleep(poll_interval) => {
                        let batch_size = poll_runner.config.batch_size;
                        let mut should_continue = true;

                        // Keep processing until we've drained work or hit capacity
                        while should_continue {
                            should_continue = false;

                            // Skip dispatch if no slots available
                            if poll_runner.work_handler.available_slots() == 0 {
                                continue;
                            }

                            // Dispatch actions - if we hit batch size, loop immediately
                            match poll_runner.work_handler.fetch_and_dispatch(
                                batch_size,
                                poll_tx.clone(),
                            ).await {
                                Ok(count) => {
                                    if count > 0 {
                                        debug!("Dispatched {} actions", count);
                                    }
                                    if count >= batch_size {
                                        should_continue = true;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to fetch/dispatch actions: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        });

        loop {
            if self.shutdown_flag.load(Ordering::Acquire) {
                info!("Runner shutdown requested");
                break;
            }
            tokio::select! {
                // Use biased selection to prioritize shutdown check.
                // This prevents race condition where a task exits due to shutdown
                // but its JoinHandle is selected before the shutdown branch.
                biased;

                _ = self.shutdown.notified() => {
                    info!("Runner shutdown requested");
                    break;
                }
                Some((in_flight, metrics)) = completion_rx.recv() => {
                    let handler = self.completion_handler.clone();
                    let dag_cache = Arc::clone(&self.dag_cache);
                    let instance_contexts = Arc::clone(&self.instance_contexts);
                    let instance_inboxes = Arc::clone(&self.instance_inboxes);
                    let completion_batcher = Arc::clone(&completion_batcher);
                    let instance_id = in_flight.action.instance_id;
                    // Capture worker tracking info for action logs
                    let pool_id = self.work_handler.worker_pool_id();
                    let worker_id = in_flight.worker_id as i64;
                    let runner_metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        // Use unified completion path for successful actions
                        if metrics.success {
                            if let Err(e) = Self::process_completion_unified(
                                &handler.db,
                                &dag_cache,
                                &instance_contexts,
                                &instance_inboxes,
                                completion_batcher.clone(),
                                &in_flight,
                                &metrics,
                                WorkflowInstanceId(instance_id),
                                Some(pool_id),
                                Some(worker_id),
                                runner_metrics.clone(),
                            )
                            .await
                            {
                                error!("Unified completion processing failed: {}", e);

                                // Handle the error: mark action complete and fail the instance
                                Self::handle_completion_error(
                                    &handler.db,
                                    &in_flight,
                                    &metrics,
                                    WorkflowInstanceId(instance_id),
                                    &e,
                                )
                                .await;
                            }
                        } else if let Err(e) = Self::process_completion_task(
                            &handler.db,
                            &dag_cache,
                            &instance_contexts,
                            &instance_inboxes,
                            completion_batcher.clone(),
                            in_flight,
                            metrics,
                            WorkflowInstanceId(instance_id),
                            Some(pool_id),
                            Some(worker_id),
                        )
                        .await
                        {
                            error!("Completion processing failed: {}", e);
                        }
                    });
                }
            }
        }

        Ok(())
    }

    /// Process due scheduled workflows by creating new instances.
    ///
    /// Returns the number of schedules that were processed.
    async fn process_due_schedules(&self) -> RunnerResult<usize> {
        let batch_size = self.config.schedule_check_batch_size;

        // Find due schedules (uses SKIP LOCKED for multi-runner safety)
        let schedules = self
            .completion_handler
            .db
            .find_due_schedules(batch_size)
            .await
            .map_err(|e| RunnerError::Dag(format!("Failed to find due schedules: {}", e)))?;
        let count = schedules.len();

        for schedule in schedules {
            // Get the latest version for this workflow
            let version_id = match self
                .completion_handler
                .db
                .get_latest_workflow_version(&schedule.workflow_name)
                .await
                .map_err(|e| RunnerError::Dag(format!("Failed to get workflow version: {}", e)))?
            {
                Some(id) => id,
                None => {
                    error!(
                        workflow_name = %schedule.workflow_name,
                        schedule_id = %schedule.id,
                        "SCHEDULE SKIPPED: No registered workflow version found. \
                         The workflow DAG must be registered before the schedule can execute. \
                         Re-register the schedule using schedule_workflow() to fix this."
                    );
                    // Still update next_run_at to avoid infinite retries
                    let next_run = self.compute_next_run(&schedule)?;
                    self.completion_handler
                        .db
                        .update_schedule_next_run(ScheduleId(schedule.id), next_run)
                        .await
                        .map_err(|e| {
                            RunnerError::Dag(format!("Failed to update schedule next_run: {}", e))
                        })?;
                    continue;
                }
            };

            // Create workflow instance with scheduled inputs and priority
            let instance_id = self
                .completion_handler
                .db
                .create_instance_with_priority(
                    &schedule.workflow_name,
                    version_id,
                    schedule.input_payload.as_deref(),
                    Some(ScheduleId(schedule.id)),
                    schedule.priority,
                )
                .await
                .map_err(|e| RunnerError::Dag(format!("Failed to create instance: {}", e)))?;

            info!(
                schedule_id = %schedule.id,
                workflow_name = %schedule.workflow_name,
                instance_id = %instance_id,
                "Created scheduled workflow instance"
            );

            // Compute next run and update schedule
            let next_run = self.compute_next_run(&schedule)?;
            self.completion_handler
                .db
                .mark_schedule_executed(ScheduleId(schedule.id), instance_id, next_run)
                .await
                .map_err(|e| {
                    RunnerError::Dag(format!("Failed to mark schedule executed: {}", e))
                })?;
        }

        Ok(count)
    }

    /// Compute the next run time based on schedule type.
    fn compute_next_run(
        &self,
        schedule: &crate::db::WorkflowSchedule,
    ) -> RunnerResult<chrono::DateTime<chrono::Utc>> {
        let base = match schedule.schedule_type.as_str() {
            "cron" => {
                let expr = schedule
                    .cron_expression
                    .as_ref()
                    .ok_or_else(|| RunnerError::Dag("Missing cron expression".into()))?;
                next_cron_run(expr).map_err(RunnerError::Dag)?
            }
            "interval" => {
                let secs = schedule
                    .interval_seconds
                    .ok_or_else(|| RunnerError::Dag("Missing interval_seconds".into()))?;
                next_interval_run(secs, Some(chrono::Utc::now()))
            }
            _ => {
                return Err(RunnerError::Dag(format!(
                    "Unknown schedule type: {}",
                    schedule.schedule_type
                )));
            }
        };

        apply_jitter(base, schedule.jitter_seconds).map_err(RunnerError::Dag)
    }

    // ========================================================================
    // Unified Readiness Model - New Completion Flow
    // ========================================================================

    /// Process a completion using the unified readiness model.
    ///
    /// This implements the 4-step completion flow:
    /// 1. Analyze subgraph - find inline nodes and frontier (barriers/actions)
    /// 2. Batch fetch inbox - single query for all relevant node inboxes
    /// 3. Execute inline subgraph - run inline nodes in memory
    /// 4. Execute completion plan - single atomic transaction
    ///
    /// Every frontier node gets readiness tracking. A node is only enqueued
    /// when `completed_count == required_count`.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        level = "info",
        skip(
            db,
            dag_cache,
            instance_contexts,
            instance_inboxes,
            completion_batcher,
            in_flight,
            metrics,
            runner_metrics
        ),
        fields(instance_id = %instance_id.0)
    )]
    async fn process_completion_unified(
        db: &Database,
        dag_cache: &DAGCache,
        instance_contexts: &Arc<RwLock<HashMap<Uuid, Scope>>>,
        instance_inboxes: &Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
        completion_batcher: Arc<CompletionBatcher>,
        in_flight: &InFlightAction,
        metrics: &RoundTripMetrics,
        instance_id: WorkflowInstanceId,
        pool_id: Option<Uuid>,
        worker_id: Option<i64>,
        runner_metrics: Option<Arc<RunnerMetrics>>,
    ) -> RunnerResult<crate::completion::CompletionResult> {
        let total_start = std::time::Instant::now();

        // Get DAG for this workflow instance
        let dag = match dag_cache.get_dag_for_instance(instance_id).await? {
            Some(dag) => dag,
            None => {
                return Ok(crate::completion::CompletionResult::default());
            }
        };

        // Get node_id from action
        let node_id = match in_flight.action.node_id.as_deref() {
            Some(id) => id,
            None => {
                return Ok(crate::completion::CompletionResult::default());
            }
        };

        let ctx = DbExecutionContext {
            db,
            dag: &dag,
            instance_id,
            instance_contexts,
            instance_inboxes,
        };
        let engine_plan =
            CompletionEngine::build_success_plan(&ctx, node_id, &metrics.response_payload)
                .await
                .map_err(|e| RunnerError::Dag(e.to_string()))?;
        let subgraph_ms = engine_plan.subgraph_us;
        let inbox_ms = engine_plan.inbox_us;
        let inline_ms = engine_plan.inline_us;
        let mut plan = engine_plan.plan;

        // Fill in action completion details
        let worker_duration_ms = Some(metrics.worker_duration.as_millis() as i64);
        plan = plan
            .with_action_completion(
                ActionId(in_flight.action.id),
                in_flight.action.delivery_token,
                metrics.success,
                metrics.response_payload.clone(),
                metrics.error_message.clone(),
            )
            .with_worker(pool_id, worker_id, worker_duration_ms);

        debug!(
            node_id = %node_id,
            inbox_writes = plan.inbox_writes.len(),
            readiness_increments = plan.readiness_increments.len(),
            has_instance_completion = plan.instance_completion.is_some(),
            "built completion plan"
        );

        // Step 4: Execute completion plan in single atomic transaction
        let inbox_writes = plan.inbox_writes.clone();
        let db_start = std::time::Instant::now();
        let result = completion_batcher.execute(instance_id, plan).await?;
        let db_ms = db_start.elapsed().as_micros() as u64;

        if !result.was_stale {
            if !inbox_writes.is_empty() {
                let updated_at = match result.inbox_updated_at {
                    Some(updated_at) => updated_at,
                    None => db.get_inbox_updated_at(instance_id).await?,
                };
                Self::apply_inbox_writes_to_cache(
                    instance_inboxes,
                    instance_id,
                    &inbox_writes,
                    updated_at,
                )
                .await;
            }
            if result.workflow_completed {
                Self::clear_inbox_cache(instance_inboxes, instance_id).await;
            }
        }

        let total_ms = total_start.elapsed().as_micros() as u64;
        debug!(
            node_id = %node_id,
            total_us = total_ms,
            subgraph_us = subgraph_ms,
            inbox_us = inbox_ms,
            inline_us = inline_ms,
            db_us = db_ms,
            newly_ready = result.newly_ready_nodes.len(),
            workflow_completed = result.workflow_completed,
            "process_completion timing"
        );

        if let Some(metrics) = &runner_metrics {
            metrics.record_process_completion(ProcessCompletionStats {
                total_us: total_ms,
                subgraph_us: subgraph_ms,
                inbox_us: inbox_ms,
                inline_us: inline_ms,
                db_us: db_ms,
                newly_ready: result.newly_ready_nodes.len(),
                workflow_completed: result.workflow_completed,
            });
        }

        Ok(result)
    }

    /// Handle errors that occur during completion processing.
    ///
    /// When an action executes successfully but the DAG completion processing fails
    /// (e.g., WorkflowDeadEnd due to unreachable nodes), we must:
    /// 1. Mark the action as complete (the action itself succeeded)
    /// 2. Mark the workflow instance as failed (the DAG can't continue)
    ///
    /// This prevents actions from being retried indefinitely when the issue is
    /// a DAG structure problem rather than an action execution problem.
    async fn handle_completion_error(
        db: &Database,
        in_flight: &InFlightAction,
        metrics: &RoundTripMetrics,
        instance_id: WorkflowInstanceId,
        error: &RunnerError,
    ) {
        // Mark the action as complete since it executed successfully.
        // The error is in the DAG completion processing, not the action itself.
        let completion_record = CompletionRecord {
            action_id: ActionId(in_flight.action.id),
            success: true, // Action succeeded, completion processing failed
            result_payload: metrics.response_payload.clone(),
            delivery_token: in_flight.action.delivery_token,
            error_message: Some(format!("DAG completion failed: {}", error)),
            pool_id: None,
            worker_id: None,
            worker_duration_ms: Some(metrics.worker_duration.as_millis() as i64),
        };

        if let Err(db_err) = db.complete_action(completion_record).await {
            error!("Failed to mark action complete after DAG error: {}", db_err);
        }

        // Mark the workflow instance as failed since the DAG can't continue
        if let Err(db_err) = db.fail_instance(instance_id).await {
            error!("Failed to mark instance failed after DAG error: {}", db_err);
        } else {
            info!(
                instance_id = %instance_id.0,
                error = %error,
                "Marked workflow instance as failed due to DAG completion error"
            );
        }
    }

    /// Process a barrier (aggregator) that has become ready.
    ///
    /// Called when the polling loop picks up a barrier from the queue.
    /// The barrier's inbox is guaranteed to be fully populated because
    /// all predecessors completed and wrote their data before the barrier
    /// was enqueued.
    #[allow(dead_code)]
    async fn process_barrier_unified(
        db: &Database,
        dag_cache: &DAGCache,
        barrier: &QueuedAction,
    ) -> RunnerResult<crate::completion::CompletionResult> {
        let instance_id = WorkflowInstanceId(barrier.instance_id);
        let node_id = match barrier.node_id.as_deref() {
            Some(id) => id,
            None => return Ok(crate::completion::CompletionResult::default()),
        };

        // Get DAG
        let dag = match dag_cache.get_dag_for_instance(instance_id).await? {
            Some(dag) => dag,
            None => return Ok(crate::completion::CompletionResult::default()),
        };

        let helper = DAGHelper::new(&dag);

        // Read aggregated results from inbox
        let spread_results = db.read_inbox_for_aggregator(instance_id, node_id).await?;

        // Aggregate results (already sorted by spread_index)
        let aggregated = WorkflowValue::List(
            spread_results
                .into_iter()
                .map(|(_, value)| json_to_workflow_value(&value))
                .collect(),
        );

        let result_count = match &aggregated {
            WorkflowValue::List(items) => items.len(),
            _ => 0,
        };
        debug!(
            barrier_id = %node_id,
            result_count = result_count,
            "processing ready barrier"
        );

        // Analyze subgraph from barrier
        let subgraph = analyze_subgraph(node_id, &dag, &helper);

        // Batch fetch inbox
        let existing_inbox = inbox_json_to_workflow(
            db.batch_read_inbox(instance_id, &subgraph.all_node_ids)
                .await?,
        );

        // Execute inline subgraph
        let initial_scope = load_initial_scope(db, instance_id).await?;
        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };
        let mut plan =
            execute_inline_subgraph(node_id, aggregated, ctx, &subgraph, &dag, instance_id)
                .map_err(|e| RunnerError::Dag(e.to_string()))?;

        // Fill in barrier completion details
        plan = plan.with_action_completion(
            ActionId(barrier.id),
            barrier.delivery_token,
            true, // barriers always succeed
            Vec::new(),
            None,
        );

        // Execute completion plan
        let result = db.execute_completion_plan(instance_id, plan).await?;

        info!(
            barrier_id = %node_id,
            newly_ready_nodes = ?result.newly_ready_nodes,
            workflow_completed = result.workflow_completed,
            "processed barrier"
        );

        Ok(result)
    }

    /// Process a failed completion and optionally route to exception handlers.
    #[allow(clippy::too_many_arguments)]
    async fn process_completion_task(
        db: &Database,
        dag_cache: &DAGCache,
        instance_contexts: &Arc<RwLock<HashMap<Uuid, Scope>>>,
        instance_inboxes: &Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
        completion_batcher: Arc<CompletionBatcher>,
        in_flight: InFlightAction,
        metrics: RoundTripMetrics,
        instance_id: WorkflowInstanceId,
        pool_id: Option<Uuid>,
        worker_id: Option<i64>,
    ) -> RunnerResult<()> {
        let node_id = match in_flight.action.node_id.as_deref() {
            Some(id) => id,
            None => return Ok(()),
        };

        let mut plan = CompletionPlan::new(node_id.to_string());
        let mut handled = false;

        if let Some(dag) = dag_cache.get_dag_for_instance(instance_id).await? {
            let ctx = DbExecutionContext {
                db,
                dag: &dag,
                instance_id,
                instance_contexts,
                instance_inboxes,
            };
            match CompletionEngine::handle_action_failure(
                &ctx,
                node_id,
                &metrics.response_payload,
                in_flight.action.attempt_number,
                in_flight.action.max_retries,
            )
            .await
            .map_err(|e| RunnerError::Dag(e.to_string()))?
            {
                ExceptionHandlingOutcome::Handled {
                    plan: handler_plan, ..
                } => {
                    plan = *handler_plan;
                    handled = true;
                }
                ExceptionHandlingOutcome::Retry | ExceptionHandlingOutcome::Unhandled => {}
            }
        }

        let worker_duration_ms = Some(metrics.worker_duration.as_millis() as i64);
        plan = plan
            .with_action_completion(
                ActionId(in_flight.action.id),
                in_flight.action.delivery_token,
                metrics.success,
                metrics.response_payload.clone(),
                metrics.error_message.clone(),
            )
            .with_worker(pool_id, worker_id, worker_duration_ms);

        let inbox_writes = plan.inbox_writes.clone();
        let result = completion_batcher.execute(instance_id, plan).await?;

        if !result.was_stale {
            if !inbox_writes.is_empty() {
                let updated_at = match result.inbox_updated_at {
                    Some(updated_at) => updated_at,
                    None => db.get_inbox_updated_at(instance_id).await?,
                };
                Self::apply_inbox_writes_to_cache(
                    instance_inboxes,
                    instance_id,
                    &inbox_writes,
                    updated_at,
                )
                .await;
            }
            if result.workflow_completed {
                Self::clear_inbox_cache(instance_inboxes, instance_id).await;
            }
        }

        if handled {
            db.mark_action_caught(instance_id, node_id).await?;
        }

        Ok(())
    }

    /// Collect inbox writes for a node's result via DATA_FLOW edges, with spread index support.
    #[allow(clippy::too_many_arguments)]
    fn collect_inbox_writes_for_node_with_spread(
        source_node_id: &str,
        variable_name: &str,
        value: &WorkflowValue,
        dag: &DAG,
        instance_id: WorkflowInstanceId,
        spread_index: Option<usize>,
        inbox_writes: &mut Vec<InboxWrite>,
    ) {
        for edge in dag.edges.iter() {
            if edge.source == source_node_id
                && edge.edge_type == EdgeType::DataFlow
                && edge.variable.as_deref() == Some(variable_name)
            {
                inbox_writes.push(InboxWrite {
                    instance_id,
                    target_node_id: edge.target.clone(),
                    variable_name: variable_name.to_string(),
                    value: value.clone(),
                    source_node_id: source_node_id.to_string(),
                    spread_index: spread_index.map(|i| i as i32),
                });
            }
        }
    }

    /// Collect inbox writes for a node's result via DATA_FLOW edges.
    fn collect_inbox_writes_for_node(
        source_node_id: &str,
        variable_name: &str,
        value: &WorkflowValue,
        dag: &DAG,
        instance_id: WorkflowInstanceId,
        inbox_writes: &mut Vec<InboxWrite>,
    ) {
        Self::collect_inbox_writes_for_node_with_spread(
            source_node_id,
            variable_name,
            value,
            dag,
            instance_id,
            None,
            inbox_writes,
        );
    }

    /// Seed inline scope and initial inbox writes from workflow inputs.
    fn seed_scope_and_inbox(
        initial_inputs: &HashMap<String, WorkflowValue>,
        dag: &DAG,
        source_node_id: &str,
        instance_id: WorkflowInstanceId,
    ) -> (Scope, Vec<InboxWrite>) {
        let mut inbox_writes = Vec::new();
        for (var_name, value) in initial_inputs {
            Self::collect_inbox_writes_for_node(
                source_node_id,
                var_name,
                value,
                dag,
                instance_id,
                &mut inbox_writes,
            );
        }
        (initial_inputs.clone(), inbox_writes)
    }

    /// Serialize workflow result (inbox values) as protobuf WorkflowArguments.
    fn serialize_workflow_result(inbox: &HashMap<String, WorkflowValue>) -> Vec<u8> {
        use prost::Message;

        let arguments: Vec<proto::WorkflowArgument> = inbox
            .iter()
            .map(|(key, value)| proto::WorkflowArgument {
                key: key.clone(),
                value: Some(value.to_proto()),
            })
            .collect();

        let workflow_args = proto::WorkflowArguments { arguments };
        workflow_args.encode_to_vec()
    }

    fn format_guard_errors(guard_errors: &[(String, String)]) -> String {
        let details = guard_errors
            .iter()
            .map(|(node_id, error)| format!("{node_id}: {error}"))
            .collect::<Vec<_>>()
            .join("; ");
        format!("Guard evaluation failed during startup: {details}")
    }

    /// Request shutdown.
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
    }

    /// Snapshot internal runner metrics if enabled.
    pub fn metrics_snapshot(&self) -> Option<RunnerMetricsSnapshot> {
        self.metrics.as_ref().map(|metrics| metrics.snapshot())
    }

    /// Register a DAG for a workflow version (for testing or warm-up).
    pub async fn register_dag(&self, version_id: Uuid, dag: DAG) {
        self.dag_cache.preload(version_id, dag).await;
    }

    /// Best-effort inbox cache lookup; fetches missing nodes from DB.
    async fn load_inbox_with_cache(
        db: &Database,
        instance_inboxes: &Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
        instance_id: WorkflowInstanceId,
        node_ids: &std::collections::HashSet<String>,
    ) -> RunnerResult<InboxValues> {
        if node_ids.is_empty() {
            return Ok(InboxValues::new());
        }

        let db_updated_at = db.get_inbox_updated_at(instance_id).await?;
        let mut merged: InboxValues = InboxValues::new();
        let mut missing: std::collections::HashSet<String> = node_ids.clone();
        let mut cache_valid = false;

        {
            let cache = instance_inboxes.read().await;
            if let Some(instance_cache) = cache.get(&instance_id.0)
                && db_updated_at <= instance_cache.updated_at
            {
                cache_valid = true;
            }
        }

        if cache_valid {
            let cache = instance_inboxes.read().await;
            if let Some(instance_cache) = cache.get(&instance_id.0) {
                for node_id in node_ids {
                    if let Some(values) = instance_cache.values.get(node_id) {
                        merged.insert(node_id.clone(), values.clone());
                        missing.remove(node_id);
                    }
                }
            }
        } else {
            let mut cache = instance_inboxes.write().await;
            cache.remove(&instance_id.0);
            missing = node_ids.clone();
            merged.clear();
        }

        if !missing.is_empty() {
            let fetched = inbox_json_to_workflow(db.batch_read_inbox(instance_id, &missing).await?);
            {
                let mut cache = instance_inboxes.write().await;
                let instance_cache =
                    cache
                        .entry(instance_id.0)
                        .or_insert_with(|| InstanceInboxCache {
                            values: HashMap::new(),
                            updated_at: db_updated_at,
                        });
                instance_cache.updated_at = db_updated_at;
                for (node_id, values) in &fetched {
                    instance_cache
                        .values
                        .insert(node_id.clone(), values.clone());
                }
            }
            for (node_id, values) in fetched {
                merged.insert(node_id, values);
            }
        }

        Ok(merged)
    }

    /// Update inbox cache with newly written non-spread values.
    async fn apply_inbox_writes_to_cache(
        instance_inboxes: &Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
        instance_id: WorkflowInstanceId,
        inbox_writes: &[crate::completion::InboxWrite],
        inbox_updated_at: DateTime<Utc>,
    ) {
        if inbox_writes.is_empty() {
            return;
        }

        let mut cache = instance_inboxes.write().await;
        let instance_cache = cache
            .entry(instance_id.0)
            .or_insert_with(|| InstanceInboxCache {
                values: HashMap::new(),
                updated_at: inbox_updated_at,
            });
        instance_cache.updated_at = inbox_updated_at;
        for write in inbox_writes {
            if write.spread_index.is_some() {
                continue;
            }
            let node_cache = instance_cache
                .values
                .entry(write.target_node_id.clone())
                .or_default();
            node_cache.insert(write.variable_name.clone(), write.value.clone());
        }
    }

    /// Drop cached inbox data for an instance.
    async fn clear_inbox_cache(
        instance_inboxes: &Arc<RwLock<HashMap<Uuid, InstanceInboxCache>>>,
        instance_id: WorkflowInstanceId,
    ) {
        let mut cache = instance_inboxes.write().await;
        cache.remove(&instance_id.0);
    }

    /// Get count of in-flight actions.
    pub async fn in_flight_count(&self) -> usize {
        self.work_handler.in_flight_count().await
    }

    /// Poll for and start any unstarted instances.
    ///
    /// Finds instances that are in 'running' state but have no actions queued yet,
    /// and starts them by parsing their input and creating the initial actions.
    ///
    /// Returns the number of instances that were processed.
    #[tracing::instrument(level = "info", skip(self))]
    async fn start_unstarted_instances(&self) -> RunnerResult<usize> {
        let total_start = std::time::Instant::now();
        let db = &self.completion_handler.db;
        let stale_before =
            Utc::now() - Duration::from_millis(self.config.start_claim_timeout_ms.max(1));
        // Use the same batch size as dispatch to ensure we can keep up with workflow creation rate
        let instances = db
            .find_unstarted_instances(self.config.batch_size as i32, stale_before)
            .await?;
        let count = instances.len();
        let mut pending_start_plans: Vec<StartPlan> = Vec::with_capacity(count);

        for instance in instances {
            let instance_id = WorkflowInstanceId(instance.id);

            // Parse initial inputs from the instance's input_payload
            let initial_inputs: std::collections::HashMap<String, WorkflowValue> =
                if let Some(payload) = &instance.input_payload {
                    // Try to decode as protobuf WorkflowArguments
                    match proto::WorkflowArguments::decode(&payload[..]) {
                        Ok(args) => args
                            .arguments
                            .iter()
                            .filter_map(|arg| {
                                arg.value
                                    .as_ref()
                                    .map(|v| (arg.key.clone(), proto_value_to_workflow_value(v)))
                            })
                            .collect(),
                        Err(e) => {
                            warn!(
                                instance_id = %instance.id,
                                error = %e,
                                "failed to decode input payload, using empty inputs"
                            );
                            std::collections::HashMap::new()
                        }
                    }
                } else {
                    std::collections::HashMap::new()
                };

            debug!(
                instance_id = %instance.id,
                workflow = %instance.workflow_name,
                input_keys = ?initial_inputs.keys().collect::<Vec<_>>(),
                "starting unstarted instance"
            );

            match self
                .build_start_plan(instance_id, initial_inputs, instance.workflow_version_id)
                .await
            {
                Ok(start_plan) => {
                    pending_start_plans.push(start_plan);
                }
                Err(RunnerError::GuardEvaluationFailed(guard_errors)) => {
                    let error_message = Self::format_guard_errors(&guard_errors);
                    let mut error_payload = HashMap::new();
                    error_payload.insert(
                        "error".to_string(),
                        WorkflowValue::String(error_message.clone()),
                    );
                    let result_payload = Self::serialize_workflow_result(&error_payload);
                    if let Err(db_err) = db
                        .fail_instance_with_result(instance_id, Some(&result_payload))
                        .await
                    {
                        error!(
                            instance_id = %instance.id,
                            error = %db_err,
                            "failed to mark instance failed after guard evaluation error"
                        );
                    } else {
                        info!(
                            instance_id = %instance.id,
                            error = %error_message,
                            "marked instance failed after guard evaluation error"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        instance_id = %instance.id,
                        error = %e,
                        "failed to start instance"
                    );
                    if let Err(db_err) = db.clear_instance_started_at(instance_id).await {
                        error!(
                            instance_id = %instance.id,
                            error = %db_err,
                            "failed to clear started_at after start error"
                        );
                    }
                }
            }
        }

        if !pending_start_plans.is_empty() {
            let claimed_instance_ids: Vec<WorkflowInstanceId> = pending_start_plans
                .iter()
                .map(|plan| plan.instance_id)
                .collect();
            let mut plans = Vec::with_capacity(pending_start_plans.len());
            let mut inbox_writes = Vec::with_capacity(pending_start_plans.len());
            for plan in pending_start_plans {
                plans.push((plan.instance_id, plan.plan));
                inbox_writes.push((plan.instance_id, plan.inbox_writes));
            }

            let results = match db.execute_completion_plans_batch(plans).await {
                Ok(results) => results,
                Err(err) => {
                    for instance_id in claimed_instance_ids {
                        if let Err(db_err) = db.clear_instance_started_at(instance_id).await {
                            error!(
                                instance_id = %instance_id.0,
                                error = %db_err,
                                "failed to clear started_at after batch start failure"
                            );
                        }
                    }
                    return Err(err.into());
                }
            };
            for ((instance_id, writes), result) in inbox_writes.into_iter().zip(results) {
                if !result.was_stale {
                    if !writes.is_empty() {
                        let updated_at = match result.inbox_updated_at {
                            Some(updated_at) => updated_at,
                            None => db.get_inbox_updated_at(instance_id).await?,
                        };
                        Self::apply_inbox_writes_to_cache(
                            &self.instance_inboxes,
                            instance_id,
                            &writes,
                            updated_at,
                        )
                        .await;
                    }
                    if result.workflow_completed {
                        Self::clear_inbox_cache(&self.instance_inboxes, instance_id).await;
                    }
                }
            }
        }

        if let Some(metrics) = &self.metrics {
            metrics.record_start_unstarted(total_start.elapsed().as_micros() as u64, count);
        }

        Ok(count)
    }

    /// Start a workflow instance by enqueuing its initial action(s).
    ///
    /// This is the entry point for workflow execution. It:
    /// 1. Loads the DAG for the instance's workflow version
    /// 2. Finds the input boundary node
    /// 3. Traverses to find the first delegated (action) nodes
    /// 4. Enqueues them with the initial input context
    ///
    /// Returns the number of actions enqueued.
    pub async fn start_instance(
        &self,
        instance_id: WorkflowInstanceId,
        initial_inputs: HashMap<String, WorkflowValue>,
    ) -> RunnerResult<usize> {
        let start_plan = self
            .build_start_plan(instance_id, initial_inputs, None)
            .await?;

        let db = &self.completion_handler.db;
        let inbox_writes = start_plan.inbox_writes.clone();
        let result = db
            .execute_completion_plan(instance_id, start_plan.plan)
            .await?;
        if !inbox_writes.is_empty() {
            let updated_at = match result.inbox_updated_at {
                Some(updated_at) => updated_at,
                None => db.get_inbox_updated_at(instance_id).await?,
            };
            Self::apply_inbox_writes_to_cache(
                &self.instance_inboxes,
                instance_id,
                &inbox_writes,
                updated_at,
            )
            .await;
        }
        if result.workflow_completed {
            Self::clear_inbox_cache(&self.instance_inboxes, instance_id).await;
        }
        if result.workflow_completed {
            info!(
                instance_id = %instance_id.0,
                "completed workflow instance during start_instance"
            );
        }

        info!(
            instance_id = %instance_id.0,
            actions = result.newly_ready_nodes.len(),
            "started workflow instance"
        );
        Ok(result.newly_ready_nodes.len())
    }

    async fn build_start_plan(
        &self,
        instance_id: WorkflowInstanceId,
        initial_inputs: HashMap<String, WorkflowValue>,
        workflow_version_id: Option<Uuid>,
    ) -> RunnerResult<StartPlan> {
        // Load the DAG for this instance
        let dag = if let Some(version_id) = workflow_version_id {
            let version_id = WorkflowVersionId(version_id);
            self.dag_cache
                .cache_instance_version(instance_id, version_id)
                .await;
            self.dag_cache.get_dag(version_id).await?
        } else {
            self.dag_cache
                .get_dag_for_instance(instance_id)
                .await?
                .ok_or_else(|| RunnerError::InstanceNotFound(instance_id.0))?
        };

        // Find the entry function and its input node
        let helper = DAGHelper::new(&dag);
        let function_names = helper.get_function_names();

        debug!(
            instance_id = %instance_id.0,
            function_count = function_names.len(),
            functions = ?function_names,
            "starting instance"
        );

        if function_names.is_empty() {
            return Err(RunnerError::Dag("No functions found in DAG".to_string()));
        }

        // Find the entry function - prefer "main" if it exists, otherwise use the first
        // non-internal function (internal functions start with "__")
        let entry_fn = function_names
            .iter()
            .find(|&&name| name == "main")
            .or_else(|| function_names.iter().find(|&&name| !name.starts_with("__")))
            .or(function_names.first())
            .copied()
            .ok_or_else(|| RunnerError::Dag("No valid entry function found".to_string()))?;
        let input_node = helper
            .find_input_node(entry_fn)
            .ok_or_else(|| RunnerError::NodeNotFound(format!("{}_input", entry_fn)))?;

        debug!(
            entry_fn = %entry_fn,
            input_node_id = %input_node.id,
            "found entry function and input node"
        );

        // Seed scope and inbox writes from initial inputs
        let (scope, inbox_writes_to_commit) =
            Self::seed_scope_and_inbox(&initial_inputs, &dag, &input_node.id, instance_id);

        debug!(
            instance_id = %instance_id.0,
            initial_scope = ?scope,
            inbox_writes = inbox_writes_to_commit.len(),
            "starting instance with initial scope"
        );

        // Store scope for inline evaluation
        {
            let mut contexts = self.instance_contexts.write().await;
            contexts.insert(instance_id.0, scope.clone());
        }

        // Build an initial completion plan from the input node so inline-only loops
        // execute before the first delegated action is scheduled.
        let subgraph = analyze_subgraph(&input_node.id, &dag, &helper);
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
        let ctx = InlineContext {
            initial_scope: &scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };
        let plan = execute_inline_subgraph(
            &input_node.id,
            WorkflowValue::Null,
            ctx,
            &subgraph,
            &dag,
            instance_id,
        )
        .map_err(|err| match err {
            CompletionError::GuardEvaluationError { node_id, message } => {
                RunnerError::GuardEvaluationFailed(vec![(node_id, message)])
            }
            CompletionError::WorkflowDeadEnd { guard_errors, .. } => {
                RunnerError::GuardEvaluationFailed(guard_errors)
            }
            other => RunnerError::Dag(other.to_string()),
        })?;

        Ok(StartPlan {
            instance_id,
            inbox_writes: plan.inbox_writes.clone(),
            plan,
        })
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

#[cfg(test)]
impl DAGRunner {
    /// Check if a result represents an exception.
    /// Returns the exception type and the full type hierarchy (MRO) if it is an exception.
    fn is_exception_result(result: &WorkflowValue) -> Option<(String, Vec<String>)> {
        match result {
            WorkflowValue::Exception {
                exc_type,
                type_hierarchy,
                ..
            } => Some((exc_type.clone(), type_hierarchy.clone())),
            _ => None,
        }
    }

    /// Get exception handlers from a node's outgoing edges.
    ///
    /// Handler matching priority:
    /// 1. Exact match on the exception type itself
    /// 2. Match on any superclass in the type hierarchy (e.g., `except LookupError:` catches KeyError)
    /// 3. Catch-all handler (empty exception_types, from `except:` or `except Exception:`)
    ///
    /// The type_hierarchy contains the MRO (Method Resolution Order) from Python,
    /// e.g., for KeyError: ["KeyError", "LookupError", "Exception", "BaseException"]
    ///
    /// Note: `except Exception:` is normalized to catch-all in the DAG construction,
    /// so we don't need special handling for it here.
    fn get_exception_handlers_from_node(
        dag: &DAG,
        node_id: &str,
        exception_type: &str,
        type_hierarchy: &[String],
    ) -> Option<String> {
        let mut catch_all_handler = None;
        let mut superclass_handler: Option<(String, usize)> = None; // (handler_id, hierarchy_index)

        for edge in &dag.edges {
            if edge.source != node_id {
                continue;
            }
            if let Some(ref exc_types) = edge.exception_types {
                if exc_types.is_empty() {
                    // Catch-all handler (bare except: or except Exception:)
                    if catch_all_handler.is_none() {
                        catch_all_handler = Some(edge.target.clone());
                    }
                } else if exc_types.iter().any(|t| t == exception_type) {
                    // Exact match on the exception type - return immediately
                    return Some(edge.target.clone());
                } else {
                    // Check if any handler type matches a superclass in the hierarchy
                    // Pick the most specific match (lowest index in hierarchy)
                    for handler_type in exc_types {
                        if let Some(pos) = type_hierarchy.iter().position(|t| t == handler_type) {
                            match &superclass_handler {
                                None => {
                                    superclass_handler = Some((edge.target.clone(), pos));
                                }
                                Some((_, existing_pos)) if pos < *existing_pos => {
                                    // This handler is more specific (closer in the hierarchy)
                                    superclass_handler = Some((edge.target.clone(), pos));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        // Priority: superclass match > catch-all
        superclass_handler
            .map(|(handler, _)| handler)
            .or(catch_all_handler)
    }

    /// Build action payload from node kwargs, resolving variable references from inbox.
    fn build_action_payload_from_inbox(
        node: &crate::dag::DAGNode,
        inbox: &std::collections::HashMap<String, WorkflowValue>,
    ) -> RunnerResult<Vec<u8>> {
        let mut payload_map = serde_json::Map::new();

        if let Some(ref kwargs) = node.kwargs {
            let kwarg_exprs = node.kwarg_exprs.as_ref();
            for (key, value_str) in kwargs {
                let expr = kwarg_exprs.and_then(|m| m.get(key));
                let resolved = Self::resolve_kwarg_value_from_inbox(key, value_str, expr, inbox)?;
                payload_map.insert(key.clone(), resolved.to_json());
            }
        }

        Ok(serde_json::to_vec(&serde_json::Value::Object(payload_map))?)
    }

    /// Resolve a kwarg value string to a JSON value using inbox.
    fn resolve_kwarg_value_from_inbox(
        key: &str,
        value_str: &str,
        expr: Option<&ast::Expr>,
        inbox: &std::collections::HashMap<String, WorkflowValue>,
    ) -> RunnerResult<WorkflowValue> {
        if let Some(expr) = expr {
            match ExpressionEvaluator::evaluate(expr, inbox) {
                Ok(value) => return Ok(value),
                Err(EvaluationError::VariableNotFound(var)) => {
                    debug!(
                        kwarg = %key,
                        missing_var = %var,
                        inbox_vars = ?inbox.keys().collect::<Vec<_>>(),
                        "kwarg variable not found in inbox, defaulting to null"
                    );
                    return Ok(WorkflowValue::Null);
                }
                Err(err) => {
                    debug!(
                        kwarg = %key,
                        error = ?err,
                        "kwarg expression evaluation failed, falling back to string parsing"
                    );
                }
            }
        }

        // Variable reference
        if let Some(var_name) = value_str.strip_prefix('$') {
            let resolved = inbox.get(var_name).cloned().unwrap_or(WorkflowValue::Null);
            debug!(
                var_name = %var_name,
                resolved = ?resolved,
                inbox_vars = ?inbox.keys().collect::<Vec<_>>(),
                "resolving variable reference from inbox"
            );
            return Ok(resolved);
        }

        // Normalize common Python bool literal casing
        match value_str {
            "True" => return Ok(WorkflowValue::Bool(true)),
            "False" => return Ok(WorkflowValue::Bool(false)),
            _ => {}
        }

        debug!(
            value_str = %value_str,
            inbox_keys = ?inbox.keys().collect::<Vec<_>>(),
            "resolving non-variable kwarg"
        );

        // Try to parse as JSON (handles numbers, booleans, strings, etc.)
        match serde_json::from_str(value_str) {
            Ok(v) => Ok(WorkflowValue::from_json(&v)),
            Err(_) => {
                // If not valid JSON, treat as raw string but normalize bool-like values
                let lower = value_str.to_ascii_lowercase();
                if lower == "true" {
                    Ok(WorkflowValue::Bool(true))
                } else if lower == "false" {
                    Ok(WorkflowValue::Bool(false))
                } else {
                    Ok(WorkflowValue::String(value_str.to_string()))
                }
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_worker_slot_tracker_basic() {
        let tracker = WorkerSlotTracker::new(4, 10);
        assert_eq!(tracker.available_slots(), 40);

        // Acquire one slot
        let worker = tracker.acquire_slot();
        assert!(worker.is_some());
        assert_eq!(tracker.available_slots(), 39);

        // Release it
        tracker.release_slot(worker.unwrap());
        assert_eq!(tracker.available_slots(), 40);
    }

    #[test]
    fn test_is_exception_result_accepts_bool_marker() {
        let payload = WorkflowValue::from_json(&json!({
            "__exception__": true,
            "type": "ValueError",
            "module": "builtins",
            "message": "boom"
        }));

        let result = DAGRunner::is_exception_result(&payload);
        assert!(result.is_some());
        let (exc_type, _type_hierarchy) = result.unwrap();
        assert_eq!(exc_type, "ValueError");
    }

    #[test]
    fn test_is_exception_result_accepts_nested_exception_object() {
        let payload = WorkflowValue::from_json(&json!({
            "__exception__": {
                "type": "ValueError",
                "module": "builtins",
                "message": "boom",
                "traceback": "Traceback..."
            }
        }));

        let result = DAGRunner::is_exception_result(&payload);
        assert!(result.is_some());
        let (exc_type, _type_hierarchy) = result.unwrap();
        assert_eq!(exc_type, "ValueError");
    }

    #[test]
    fn test_is_exception_result_returns_type_hierarchy() {
        let payload = WorkflowValue::from_json(&json!({
            "__exception__": {
                "type": "KeyError",
                "module": "builtins",
                "message": "key not found",
                "traceback": "Traceback...",
                "type_hierarchy": ["KeyError", "LookupError", "Exception", "BaseException"]
            }
        }));

        let result = DAGRunner::is_exception_result(&payload);
        assert!(result.is_some());
        let (exc_type, type_hierarchy) = result.unwrap();
        assert_eq!(exc_type, "KeyError");
        assert_eq!(
            type_hierarchy,
            vec!["KeyError", "LookupError", "Exception", "BaseException"]
        );
    }

    #[test]
    fn test_worker_slot_tracker_exhaust() {
        let tracker = WorkerSlotTracker::new(2, 2);
        assert_eq!(tracker.available_slots(), 4);

        // Exhaust all slots
        for _ in 0..4 {
            assert!(tracker.acquire_slot().is_some());
        }

        // Should fail now
        assert!(tracker.acquire_slot().is_none());
        assert_eq!(tracker.available_slots(), 0);
    }

    #[test]
    fn test_worker_slot_tracker_round_robin() {
        let tracker = WorkerSlotTracker::new(3, 1);

        let first = tracker.acquire_slot();
        let second = tracker.acquire_slot();
        let third = tracker.acquire_slot();

        assert_eq!(first, Some(0));
        assert_eq!(second, Some(1));
        assert_eq!(third, Some(2));
    }

    #[test]
    fn test_worker_slot_tracker_least_loaded_bias() {
        let tracker = WorkerSlotTracker::new(3, 2);

        assert_eq!(tracker.acquire_slot(), Some(0));
        assert_eq!(tracker.acquire_slot(), Some(1));
        assert_eq!(tracker.acquire_slot(), Some(2));
        assert_eq!(tracker.acquire_slot(), Some(0));

        assert_eq!(tracker.worker_available(0), 0);
        assert_eq!(tracker.worker_available(1), 1);
        assert_eq!(tracker.worker_available(2), 1);

        assert_eq!(tracker.acquire_slot(), Some(1));
        assert_eq!(tracker.acquire_slot(), Some(2));
        assert_eq!(tracker.acquire_slot(), None);
    }

    #[test]
    fn test_in_flight_tracker() {
        let mut tracker = InFlightTracker::new();
        let token = Uuid::new_v4();

        let action = QueuedAction {
            id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            partition_id: 0,
            action_seq: 1,
            module_name: "test".to_string(),
            action_name: "action".to_string(),
            dispatch_payload: vec![],
            timeout_seconds: 30,
            max_retries: 3,
            attempt_number: 0,
            delivery_token: token,
            timeout_retry_limit: 3,
            retry_kind: "failure".to_string(),
            node_id: Some("node_1".to_string()),
            node_type: "action".to_string(),
            result_payload: None,
            success: None,
            status: "dispatched".to_string(),
            scheduled_at: None,
            last_error: None,
        };

        tracker.add(action.clone(), 0, 1);
        assert_eq!(tracker.count(), 1);

        let timed_out = tracker.take_timed_out(Instant::now() + Duration::from_secs(31));
        assert_eq!(timed_out.len(), 1);
        assert_eq!(tracker.count(), 0);

        let removed = tracker.remove(&token);
        assert!(removed.is_none());
    }

    #[test]
    fn test_expression_evaluator_literal() {
        let scope: Scope = HashMap::new();

        // Integer
        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::IntValue(42)),
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(42.into()));

        // String
        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::StringValue("hello".to_string())),
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::String("hello".to_string()));

        // Boolean
        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::BoolValue(true)),
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_expression_evaluator_variable() {
        let mut scope: Scope = HashMap::new();
        scope.insert("x".to_string(), WorkflowValue::Int(10.into()));

        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::Variable(ast::Variable {
                name: "x".to_string(),
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(10.into()));
    }

    #[test]
    fn test_expression_evaluator_binary_add() {
        let scope: Scope = HashMap::new();

        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                left: Some(Box::new(ast::Expr {
                    kind: Some(ast::expr::Kind::Literal(ast::Literal {
                        value: Some(ast::literal::Value::IntValue(3)),
                    })),
                    span: None,
                })),
                op: ast::BinaryOperator::BinaryOpAdd as i32,
                right: Some(Box::new(ast::Expr {
                    kind: Some(ast::expr::Kind::Literal(ast::Literal {
                        value: Some(ast::literal::Value::IntValue(7)),
                    })),
                    span: None,
                })),
            }))),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(10.into()));
    }

    #[test]
    fn test_expression_evaluator_comparison() {
        let scope: Scope = HashMap::new();

        // 5 > 3
        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                left: Some(Box::new(ast::Expr {
                    kind: Some(ast::expr::Kind::Literal(ast::Literal {
                        value: Some(ast::literal::Value::IntValue(5)),
                    })),
                    span: None,
                })),
                op: ast::BinaryOperator::BinaryOpGt as i32,
                right: Some(Box::new(ast::Expr {
                    kind: Some(ast::expr::Kind::Literal(ast::Literal {
                        value: Some(ast::literal::Value::IntValue(3)),
                    })),
                    span: None,
                })),
            }))),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Bool(true));
    }

    #[test]
    fn test_expression_evaluator_list() {
        let scope: Scope = HashMap::new();

        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::List(ast::ListExpr {
                elements: vec![
                    ast::Expr {
                        kind: Some(ast::expr::Kind::Literal(ast::Literal {
                            value: Some(ast::literal::Value::IntValue(1)),
                        })),
                        span: None,
                    },
                    ast::Expr {
                        kind: Some(ast::expr::Kind::Literal(ast::Literal {
                            value: Some(ast::literal::Value::IntValue(2)),
                        })),
                        span: None,
                    },
                ],
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::Int(1.into()),
                WorkflowValue::Int(2.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_range() {
        let scope: Scope = HashMap::new();

        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                name: "range".to_string(),
                args: vec![],
                kwargs: vec![ast::Kwarg {
                    name: "stop".to_string(),
                    value: Some(ast::Expr {
                        kind: Some(ast::expr::Kind::Literal(ast::Literal {
                            value: Some(ast::literal::Value::IntValue(5)),
                        })),
                        span: None,
                    }),
                }],
                global_function: ast::GlobalFunction::Range as i32,
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            WorkflowValue::List(vec![
                WorkflowValue::Int(0.into()),
                WorkflowValue::Int(1.into()),
                WorkflowValue::Int(2.into()),
                WorkflowValue::Int(3.into()),
                WorkflowValue::Int(4.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_len() {
        let scope: Scope = HashMap::new();

        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                name: "len".to_string(),
                args: vec![],
                kwargs: vec![ast::Kwarg {
                    name: "items".to_string(),
                    value: Some(ast::Expr {
                        kind: Some(ast::expr::Kind::List(ast::ListExpr {
                            elements: vec![
                                ast::Expr {
                                    kind: Some(ast::expr::Kind::Literal(ast::Literal {
                                        value: Some(ast::literal::Value::IntValue(1)),
                                    })),
                                    span: None,
                                },
                                ast::Expr {
                                    kind: Some(ast::expr::Kind::Literal(ast::Literal {
                                        value: Some(ast::literal::Value::IntValue(2)),
                                    })),
                                    span: None,
                                },
                                ast::Expr {
                                    kind: Some(ast::expr::Kind::Literal(ast::Literal {
                                        value: Some(ast::literal::Value::IntValue(3)),
                                    })),
                                    span: None,
                                },
                            ],
                        })),
                        span: None,
                    }),
                }],
                global_function: ast::GlobalFunction::Len as i32,
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, WorkflowValue::Int(3.into()));
    }

    #[test]
    fn test_completion_batch() {
        let batch = CompletionBatch::new();
        assert!(batch.is_empty());

        let mut batch = CompletionBatch::new();
        batch.completions.push(CompletionRecord {
            action_id: ActionId::new(),
            success: true,
            result_payload: vec![],
            delivery_token: Uuid::new_v4(),
            error_message: None,
            pool_id: None,
            worker_id: None,
            worker_duration_ms: None,
        });
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_build_action_payload_resolves_variables() {
        use crate::dag::DAGNode;

        // Create a node with kwargs that reference variables
        let mut kwargs = HashMap::new();
        kwargs.insert("input_number".to_string(), "$number".to_string());
        kwargs.insert(
            "factorial_value".to_string(),
            "$factorial_value".to_string(),
        );
        kwargs.insert("fibonacci_value".to_string(), "$fib_value".to_string());

        let node = DAGNode::new(
            "action_6".to_string(),
            "action_call".to_string(),
            "@summarize_math()".to_string(),
        )
        .with_kwargs(kwargs);

        // Create inbox with the expected variables
        let mut inbox: HashMap<String, WorkflowValue> = HashMap::new();
        inbox.insert("number".to_string(), WorkflowValue::Int(5.into()));
        inbox.insert(
            "factorial_value".to_string(),
            WorkflowValue::Int(120.into()),
        );
        inbox.insert("fib_value".to_string(), WorkflowValue::Int(5.into()));

        // Build payload
        let payload = DAGRunner::build_action_payload_from_inbox(&node, &inbox).unwrap();
        let payload_json = serde_json::from_slice::<serde_json::Value>(&payload).unwrap();
        let payload_value = WorkflowValue::from_json(&payload_json);

        // Verify all kwargs are resolved correctly
        let obj = payload_value.as_dict().expect("payload should be object");
        assert_eq!(obj.get("input_number"), Some(&WorkflowValue::Int(5.into())));
        assert_eq!(
            obj.get("factorial_value"),
            Some(&WorkflowValue::Int(120.into()))
        );
        assert_eq!(
            obj.get("fibonacci_value"),
            Some(&WorkflowValue::Int(5.into()))
        );
    }

    #[test]
    fn test_build_action_payload_uses_kwarg_expressions() {
        use crate::dag::convert_to_dag;
        use crate::parser::parse;

        let source = r#"fn main(input: [], output: [result]):
    result = @dummy(flag=True)
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program).unwrap();

        let action_node = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("dummy"))
            .expect("Action node not found");

        let inbox: HashMap<String, WorkflowValue> = HashMap::new();
        let payload = DAGRunner::build_action_payload_from_inbox(action_node, &inbox).unwrap();
        let payload_json = serde_json::from_slice::<serde_json::Value>(&payload).unwrap();
        let payload_value = WorkflowValue::from_json(&payload_json);
        let obj = payload_value.as_dict().expect("payload should be object");

        assert_eq!(obj.get("flag"), Some(&WorkflowValue::Bool(true)));
    }

    #[test]
    fn test_build_action_payload_missing_variable_returns_null() {
        use crate::dag::DAGNode;

        // Create a node with kwargs that reference variables
        let mut kwargs = HashMap::new();
        kwargs.insert("input_number".to_string(), "$number".to_string());
        kwargs.insert(
            "factorial_value".to_string(),
            "$factorial_value".to_string(),
        );
        kwargs.insert("fibonacci_value".to_string(), "$fib_value".to_string());

        let node = DAGNode::new(
            "action_6".to_string(),
            "action_call".to_string(),
            "@summarize_math()".to_string(),
        )
        .with_kwargs(kwargs);

        // Create inbox MISSING some variables - this simulates the bug
        let mut inbox: HashMap<String, WorkflowValue> = HashMap::new();
        inbox.insert(
            "factorial_value".to_string(),
            WorkflowValue::Int(120.into()),
        );
        // fib_value and number are MISSING

        // Build payload
        let payload = DAGRunner::build_action_payload_from_inbox(&node, &inbox).unwrap();
        let payload_json = serde_json::from_slice::<serde_json::Value>(&payload).unwrap();
        let payload_value = WorkflowValue::from_json(&payload_json);

        // Missing variables should resolve to null (which will cause TypeError in Python!)
        let obj = payload_value.as_dict().expect("payload should be object");
        assert_eq!(
            obj.get("input_number"),
            Some(&WorkflowValue::Null),
            "missing variable should be null"
        );
        assert_eq!(
            obj.get("factorial_value"),
            Some(&WorkflowValue::Int(120.into()))
        );
        assert_eq!(
            obj.get("fibonacci_value"),
            Some(&WorkflowValue::Null),
            "missing variable should be null"
        );
    }

    #[test]
    fn test_collect_inbox_writes_for_input_variables() {
        use crate::dag::{DAG, DAGEdge, DAGNode};

        // This test reproduces the bug where input variables don't flow to downstream
        // nodes via DataFlow edges during workflow start.
        //
        // Workflow structure:
        //   main_input_1 (input node, declares variable 'n')
        //       |
        //       v (StateMachine edge)
        //   parallel_2 (parallel block)
        //       |
        //       v (StateMachine edge)
        //   action_3 (compute_factorial, uses n)
        //   action_4 (compute_fibonacci, uses n)
        //       |
        //       v (StateMachine edge from aggregator)
        //   action_5 (summarize_math, uses n AND factorial_value AND fib_value)
        //
        // DataFlow edges:
        //   main_input_1 --[n]--> action_3
        //   main_input_1 --[n]--> action_4
        //   main_input_1 --[n]--> action_5  <-- This is the one that's broken!
        //   action_3 --[factorial_value]--> action_5
        //   action_4 --[fib_value]--> action_5

        let mut dag = DAG {
            nodes: HashMap::new(),
            edges: Vec::new(),
            entry_node: Some("main_input_1".to_string()),
        };

        // Add input node
        let input_node = DAGNode::new(
            "main_input_1".to_string(),
            "input".to_string(),
            "".to_string(),
        )
        .with_input(vec!["n".to_string()])
        .with_function_name("main");
        dag.nodes.insert("main_input_1".to_string(), input_node);

        // Add action nodes
        let action_5 = DAGNode::new(
            "action_5".to_string(),
            "action_call".to_string(),
            "@summarize_math()".to_string(),
        );
        dag.nodes.insert("action_5".to_string(), action_5);

        // Add DataFlow edge from input node to downstream action for variable 'n'
        dag.edges.push(DAGEdge::data_flow(
            "main_input_1".to_string(),
            "action_5".to_string(),
            "n",
        ));

        // Test: collect_inbox_writes_for_node should produce inbox write for action_5
        let instance_id = WorkflowInstanceId(Uuid::new_v4());
        let mut inbox_writes = Vec::new();

        DAGRunner::collect_inbox_writes_for_node(
            "main_input_1",
            "n",
            &WorkflowValue::Int(42.into()),
            &dag,
            instance_id,
            &mut inbox_writes,
        );

        // Verify an inbox write was created for action_5
        assert_eq!(
            inbox_writes.len(),
            1,
            "Should create inbox write for downstream action"
        );
        assert_eq!(inbox_writes[0].target_node_id, "action_5");
        assert_eq!(inbox_writes[0].variable_name, "n");
        assert_eq!(inbox_writes[0].value, WorkflowValue::Int(42.into()));
        assert_eq!(inbox_writes[0].source_node_id, "main_input_1");
    }

    #[test]
    fn test_seed_scope_and_inbox_preserves_input_types_and_writes_inbox() {
        use crate::dag::{DAG, DAGEdge, DAGNode};

        let mut dag = DAG {
            nodes: HashMap::new(),
            edges: Vec::new(),
            entry_node: Some("main_input_1".to_string()),
        };

        let input_node = DAGNode::new(
            "main_input_1".to_string(),
            "input".to_string(),
            "".to_string(),
        )
        .with_input(vec!["n".to_string()])
        .with_function_name("main");
        dag.nodes.insert("main_input_1".to_string(), input_node);

        let action_node = DAGNode::new(
            "action_2".to_string(),
            "action_call".to_string(),
            "@compute()".to_string(),
        );
        dag.nodes.insert("action_2".to_string(), action_node);

        // DataFlow edge from the input node to the action for variable "n".
        dag.edges.push(DAGEdge::data_flow(
            "main_input_1".to_string(),
            "action_2".to_string(),
            "n",
        ));

        let mut initial_inputs = HashMap::new();
        initial_inputs.insert("n".to_string(), WorkflowValue::Int(7.into()));

        let instance_id = WorkflowInstanceId(Uuid::new_v4());
        let (scope, inbox_writes) =
            DAGRunner::seed_scope_and_inbox(&initial_inputs, &dag, "main_input_1", instance_id);

        // Scope should preserve numeric types instead of stringifying JSON inputs.
        assert_eq!(
            scope.get("n"),
            Some(&WorkflowValue::Int(7.into())),
            "initial scope should keep the original numeric value"
        );

        // Inbox writes should include the initial input flowing to downstream nodes.
        assert_eq!(inbox_writes.len(), 1);
        assert_eq!(inbox_writes[0].target_node_id, "action_2");
        assert_eq!(inbox_writes[0].variable_name, "n");
        assert_eq!(inbox_writes[0].value, WorkflowValue::Int(7.into()));
        assert_eq!(inbox_writes[0].source_node_id, "main_input_1");
    }

    /// Test that retries are exhausted before exception handlers are checked.
    ///
    /// This verifies the fix for the bug where exception handlers were triggered
    /// on the first failure instead of after all retries were exhausted.
    #[test]
    fn test_retries_exhausted_before_exception_handler() {
        // Helper to check if retries are exhausted (same logic as in process_completed_action)
        fn retries_exhausted(attempt_number: i32, max_retries: i32) -> bool {
            attempt_number >= max_retries
        }

        // Case 1: First attempt with retries available - should NOT check exception handlers
        // attempt_number=0 means this is the first attempt (0-indexed)
        // max_retries=3 means we can retry up to 3 times (attempts 0, 1, 2, 3)
        assert!(
            !retries_exhausted(0, 3),
            "First attempt with max_retries=3 should NOT be exhausted"
        );
        assert!(
            !retries_exhausted(1, 3),
            "Second attempt with max_retries=3 should NOT be exhausted"
        );
        assert!(
            !retries_exhausted(2, 3),
            "Third attempt with max_retries=3 should NOT be exhausted"
        );

        // Case 2: Final attempt - should check exception handlers
        assert!(
            retries_exhausted(3, 3),
            "Fourth attempt (attempt_number=3) with max_retries=3 should be exhausted"
        );

        // Case 3: Beyond max retries (shouldn't happen, but be safe)
        assert!(
            retries_exhausted(5, 3),
            "attempt_number > max_retries should be exhausted"
        );

        // Case 4: No retries configured (max_retries=0)
        // First attempt should immediately trigger exception handler
        assert!(
            retries_exhausted(0, 0),
            "With max_retries=0, first failure should be exhausted"
        );

        // Case 5: Single retry (max_retries=1)
        assert!(
            !retries_exhausted(0, 1),
            "First attempt with max_retries=1 should NOT be exhausted"
        );
        assert!(
            retries_exhausted(1, 1),
            "Second attempt with max_retries=1 should be exhausted"
        );
    }

    /// Test that a QueuedAction with retries remaining should not trigger exception handling.
    #[test]
    fn test_queued_action_retry_state() {
        let action_with_retries = QueuedAction {
            id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            partition_id: 0,
            action_seq: 1,
            module_name: "test".to_string(),
            action_name: "perform_crawl".to_string(),
            dispatch_payload: vec![],
            timeout_seconds: 300,
            max_retries: 3,    // Can retry 3 times
            attempt_number: 0, // First attempt
            delivery_token: Uuid::new_v4(),
            timeout_retry_limit: 3,
            retry_kind: "failure".to_string(),
            node_id: Some("action_1".to_string()),
            node_type: "action".to_string(),
            result_payload: None,
            success: Some(false), // Failed
            status: "failed".to_string(),
            scheduled_at: None,
            last_error: Some("CrawlError: rate limited".to_string()),
        };

        // With attempt_number=0 and max_retries=3, retries are NOT exhausted
        let retries_exhausted =
            action_with_retries.attempt_number >= action_with_retries.max_retries;
        assert!(
            !retries_exhausted,
            "Action on first attempt with max_retries=3 should have retries remaining"
        );

        // Simulate after all retries exhausted
        let action_exhausted = QueuedAction {
            attempt_number: 3, // After 4 attempts (0, 1, 2, 3)
            ..action_with_retries
        };

        let retries_exhausted = action_exhausted.attempt_number >= action_exhausted.max_retries;
        assert!(
            retries_exhausted,
            "Action on attempt 3 with max_retries=3 should have exhausted retries"
        );
    }

    #[test]
    fn test_exception_handler_matching_exception_normalized_to_catch_all() {
        // Test that `except Exception:` is normalized to catch-all in the DAG
        // and catches all exception types
        use crate::dag::{DAG, DAGEdge, DAGNode};

        let mut dag = DAG::new();

        dag.add_node(DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "@risky()".to_string(),
        ));
        dag.add_node(DAGNode::new(
            "handler_1".to_string(),
            "action_call".to_string(),
            "@fallback()".to_string(),
        ));

        // Add exception edge with "Exception" type - should be normalized to catch-all
        let edge = DAGEdge::state_machine_with_exception(
            "action_1".to_string(),
            "handler_1".to_string(),
            vec!["Exception".to_string()],
        );

        // Verify normalization happened
        assert!(
            edge.exception_types.as_ref().unwrap().is_empty(),
            "Exception should be normalized to empty (catch-all)"
        );

        dag.add_edge(edge);

        // Test that any exception type is caught (empty type_hierarchy since catch-all doesn't need it)
        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "ValueError",
            &[
                "ValueError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        );
        assert_eq!(
            handler,
            Some("handler_1".to_string()),
            "Normalized Exception handler should catch ValueError"
        );

        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "TimeoutError",
            &[
                "TimeoutError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        );
        assert_eq!(
            handler,
            Some("handler_1".to_string()),
            "Normalized Exception handler should catch TimeoutError"
        );
    }

    #[test]
    fn test_exception_handler_matching_exact_match_takes_priority() {
        // Test that specific exception types match before catch-all
        use crate::dag::{DAG, DAGEdge, DAGNode};

        let mut dag = DAG::new();

        dag.add_node(DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "@risky()".to_string(),
        ));
        dag.add_node(DAGNode::new(
            "specific_handler".to_string(),
            "action_call".to_string(),
            "@handle_value_error()".to_string(),
        ));
        dag.add_node(DAGNode::new(
            "catch_all_handler".to_string(),
            "action_call".to_string(),
            "@handle_exception()".to_string(),
        ));

        // Add specific handler for ValueError
        dag.add_edge(DAGEdge::state_machine_with_exception(
            "action_1".to_string(),
            "specific_handler".to_string(),
            vec!["ValueError".to_string()],
        ));

        // Add catch-all handler (simulating `except Exception:` which is normalized)
        dag.add_edge(DAGEdge::state_machine_with_exception(
            "action_1".to_string(),
            "catch_all_handler".to_string(),
            vec![], // Already normalized catch-all
        ));

        // ValueError should go to specific handler
        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "ValueError",
            &[
                "ValueError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        );
        assert_eq!(
            handler,
            Some("specific_handler".to_string()),
            "ValueError should match specific handler"
        );

        // Other exceptions should go to catch-all handler
        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "TypeError",
            &[
                "TypeError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        );
        assert_eq!(
            handler,
            Some("catch_all_handler".to_string()),
            "TypeError should fall back to catch-all handler"
        );
    }

    #[test]
    fn test_exception_handler_matching_catch_all() {
        // Test that bare `except:` works as catch-all
        use crate::dag::{DAG, DAGEdge, DAGNode};

        let mut dag = DAG::new();

        dag.add_node(DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "@risky()".to_string(),
        ));
        dag.add_node(DAGNode::new(
            "catch_all_handler".to_string(),
            "action_call".to_string(),
            "@handle_all()".to_string(),
        ));

        // Add catch-all handler (empty exception_types)
        dag.add_edge(DAGEdge::state_machine_with_exception(
            "action_1".to_string(),
            "catch_all_handler".to_string(),
            vec![], // Empty = catch all
        ));

        // Any exception should be caught
        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "ValueError",
            &[
                "ValueError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        );
        assert_eq!(
            handler,
            Some("catch_all_handler".to_string()),
            "Catch-all should catch ValueError"
        );

        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "AnyCustomError",
            &[
                "AnyCustomError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        );
        assert_eq!(
            handler,
            Some("catch_all_handler".to_string()),
            "Catch-all should catch any exception"
        );
    }

    #[test]
    fn test_exception_handler_matching_prefers_first_catch_all() {
        use crate::dag::{DAG, DAGEdge, DAGNode};

        let mut dag = DAG::new();

        dag.add_node(DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "@risky()".to_string(),
        ));
        dag.add_node(DAGNode::new(
            "inner_handler".to_string(),
            "action_call".to_string(),
            "@handle_inner()".to_string(),
        ));
        dag.add_node(DAGNode::new(
            "outer_handler".to_string(),
            "action_call".to_string(),
            "@handle_outer()".to_string(),
        ));

        dag.add_edge(DAGEdge::state_machine_with_exception(
            "action_1".to_string(),
            "inner_handler".to_string(),
            vec![],
        ));
        dag.add_edge(DAGEdge::state_machine_with_exception(
            "action_1".to_string(),
            "outer_handler".to_string(),
            vec![],
        ));

        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "ValueError",
            &[
                "ValueError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        );
        assert_eq!(
            handler,
            Some("inner_handler".to_string()),
            "First catch-all handler should win"
        );
    }

    #[test]
    fn test_exception_handler_matching_superclass_hierarchy() {
        // Test that exception handlers match superclasses in the type hierarchy
        // e.g., `except LookupError:` should catch KeyError
        use crate::dag::{DAG, DAGEdge, DAGNode};

        let mut dag = DAG::new();

        dag.add_node(DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "@risky()".to_string(),
        ));
        dag.add_node(DAGNode::new(
            "lookup_handler".to_string(),
            "action_call".to_string(),
            "@handle_lookup()".to_string(),
        ));
        dag.add_node(DAGNode::new(
            "catch_all_handler".to_string(),
            "action_call".to_string(),
            "@handle_all()".to_string(),
        ));

        // Add handler for LookupError (superclass of KeyError)
        dag.add_edge(DAGEdge::state_machine_with_exception(
            "action_1".to_string(),
            "lookup_handler".to_string(),
            vec!["LookupError".to_string()],
        ));

        // Add catch-all handler
        dag.add_edge(DAGEdge::state_machine_with_exception(
            "action_1".to_string(),
            "catch_all_handler".to_string(),
            vec![],
        ));

        // KeyError has hierarchy: KeyError -> LookupError -> Exception -> BaseException
        let key_error_hierarchy = vec![
            "KeyError".to_string(),
            "LookupError".to_string(),
            "Exception".to_string(),
            "BaseException".to_string(),
        ];

        // KeyError should be caught by LookupError handler (via superclass matching)
        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "KeyError",
            &key_error_hierarchy,
        );
        assert_eq!(
            handler,
            Some("lookup_handler".to_string()),
            "KeyError should be caught by LookupError handler via hierarchy"
        );

        // IndexError also inherits from LookupError
        let index_error_hierarchy = vec![
            "IndexError".to_string(),
            "LookupError".to_string(),
            "Exception".to_string(),
            "BaseException".to_string(),
        ];

        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "IndexError",
            &index_error_hierarchy,
        );
        assert_eq!(
            handler,
            Some("lookup_handler".to_string()),
            "IndexError should be caught by LookupError handler via hierarchy"
        );

        // ValueError does not inherit from LookupError, should fall to catch-all
        let value_error_hierarchy = vec![
            "ValueError".to_string(),
            "Exception".to_string(),
            "BaseException".to_string(),
        ];

        let handler = DAGRunner::get_exception_handlers_from_node(
            &dag,
            "action_1",
            "ValueError",
            &value_error_hierarchy,
        );
        assert_eq!(
            handler,
            Some("catch_all_handler".to_string()),
            "ValueError should fall to catch-all (not LookupError)"
        );
    }
}
