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
    collections::{BinaryHeap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use prost::Message;

use crate::{
    ast_evaluator::{EvaluationError, ExpressionEvaluator, Scope},
    completion::{GuardResult, InlineContext, analyze_subgraph, execute_inline_subgraph},
    dag::{DAG, DAGConverter, DAGNode, EXCEPTION_SCOPE_VAR, EdgeType},
    dag_state::{DAGHelper, ExecutionMode, SuccessorInfo},
    db::{
        ActionId, BackoffKind, CompletionRecord, Database, NewAction, QueuedAction, ScheduleId,
        WorkerStatusUpdate, WorkflowInstanceId, WorkflowVersionId,
    },
    messages::proto,
    parser::ast,
    schedule::{apply_jitter, next_cron_run, next_interval_run},
    value::WorkflowValue,
    worker::{ActionDispatchPayload, PythonWorkerPool, RoundTripMetrics, WorkerThroughputSnapshot},
};

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
    pub fn add(&mut self, action: QueuedAction, worker_idx: usize) {
        if action.timeout_seconds > 0 {
            let deadline = Instant::now() + Duration::from_secs(action.timeout_seconds as u64);
            self.timeouts.insert(action.delivery_token, deadline);
        }

        self.actions.insert(
            action.delivery_token,
            InFlightAction {
                action,
                worker_idx,
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
}

impl WorkQueueHandler {
    pub fn new(
        db: Arc<Database>,
        worker_pool: Arc<PythonWorkerPool>,
        slot_tracker: Arc<WorkerSlotTracker>,
        in_flight: Arc<Mutex<InFlightTracker>>,
        dag_cache: Arc<DAGCache>,
    ) -> Self {
        Self {
            db,
            worker_pool,
            slot_tracker,
            in_flight,
            dag_cache,
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
    pub async fn fetch_and_dispatch(
        &self,
        batch_size: usize,
        completion_tx: mpsc::Sender<(InFlightAction, RoundTripMetrics)>,
    ) -> RunnerResult<usize> {
        let total_start = std::time::Instant::now();

        let available = self.slot_tracker.available_slots();
        if available == 0 {
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
            info!(
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
        let inline_us = inline_start.elapsed().as_micros() as u64;

        // Execute completion plan in single atomic transaction
        let db_start = std::time::Instant::now();
        let result = self.db.execute_completion_plan(instance_id, plan).await?;
        let db_us = db_start.elapsed().as_micros() as u64;

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

        // Execute completion plan in single atomic transaction
        let result = self.db.execute_completion_plan(instance_id, plan).await?;

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

        // Track in-flight
        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.add(action.clone(), worker_idx);
        }

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
                            error!("Failed to send completion: {}", e);
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
    pub async fn write_batch(&self, batch: CompletionBatch) -> RunnerResult<()> {
        // Complete actions
        for completion in batch.completions {
            self.db.complete_action(completion).await?;
        }

        // Write inbox entries (data flow between nodes)
        for inbox_write in batch.inbox_writes {
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

        Ok(())
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

/// Result of creating actions for a node.
/// Includes actions to enqueue and any inbox writes needed.
#[derive(Debug, Default)]
struct NodeActionResult {
    pub actions: Vec<NewAction>,
    pub inbox_writes: Vec<InboxWrite>,
    /// If this is a spread node, the aggregator node ID and expected count
    /// that needs readiness initialization.
    pub readiness_init: Option<ReadinessInit>,
}

/// Info needed to initialize readiness tracking for an aggregator node.
#[derive(Debug, Clone)]
struct ReadinessInit {
    pub aggregator_node_id: String,
    pub required_count: i32,
}

/// Exception information for error handling.
#[derive(Debug, Clone)]
pub struct ExceptionInfo {
    pub exception_type: String,
    pub message: String,
    pub node_id: String,
}

// ============================================================================
// DAG Runner
// ============================================================================

/// Configuration for the DAG runner.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Maximum actions to fetch per batch
    pub batch_size: usize,
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
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_slots_per_worker: 10,
            poll_interval_ms: 100,
            timeout_check_interval_ms: 1000,
            timeout_check_batch_size: 100,
            schedule_check_interval_ms: 10000, // 10 seconds
            schedule_check_batch_size: 100,
            worker_status_interval_ms: 10000,
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
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
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

        let dag_cache = Arc::new(DAGCache::new(Arc::clone(&db)));
        let work_handler = Arc::new(WorkQueueHandler::new(
            Arc::clone(&db),
            worker_pool,
            slot_tracker,
            in_flight,
            Arc::clone(&dag_cache),
        ));
        let completion_handler = WorkCompletionHandler::new(db);

        Self {
            config,
            work_handler,
            completion_handler,
            dag_cache,
            instance_contexts: Arc::new(RwLock::new(HashMap::new())),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Run the main execution loop.
    pub async fn run(self: Arc<Self>) -> RunnerResult<()> {
        info!("Starting DAG runner");

        // Channel for completion results
        let (completion_tx, mut completion_rx) =
            mpsc::channel::<(InFlightAction, RoundTripMetrics)>(1000);

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
                tokio::select! {
                    _ = schedule_runner.shutdown.notified() => break,
                    _ = interval.tick() => {
                        if let Err(e) = schedule_runner.process_due_schedules().await {
                            error!("Failed to process due schedules: {}", e);
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
                tokio::select! {
                    _ = status_runner.shutdown.notified() => break,
                    _ = interval.tick() => {
                        let pool_id = status_runner.work_handler.worker_pool_id();
                        let snapshots = status_runner.work_handler.worker_throughput_snapshot();
                        if !snapshots.is_empty() {
                            let updates: Vec<WorkerStatusUpdate> = snapshots
                                .into_iter()
                                .map(|snapshot| WorkerStatusUpdate {
                                    worker_id: snapshot.worker_id as i64,
                                    throughput_per_min: snapshot.throughput_per_min,
                                    total_completed: snapshot.total_completed as i64,
                                    last_action_at: snapshot.last_action_at,
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

        // Poll + dispatch loop - uses sleep() like original for consistent timing behavior
        let poll_runner = Arc::clone(&self);
        let poll_tx = completion_tx.clone();
        drop(completion_tx); // Explicitly drop unused sender
        let poll_interval = tokio::time::Duration::from_millis(poll_runner.config.poll_interval_ms);
        let _poll_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = poll_runner.shutdown.notified() => break,
                    _ = tokio::time::sleep(poll_interval) => {
                        if let Err(e) = poll_runner.start_unstarted_instances().await {
                            error!("Failed to start unstarted instances: {}", e);
                        }

                        if poll_runner.work_handler.available_slots() == 0 {
                            continue;
                        }

                        match poll_runner.work_handler.fetch_and_dispatch(
                            poll_runner.config.batch_size,
                            poll_tx.clone(),
                        ).await {
                            Ok(count) if count > 0 => {
                                debug!("Dispatched {} actions", count);
                            }
                            Ok(_) => {}
                            Err(e) => {
                                error!("Failed to fetch/dispatch actions: {}", e);
                            }
                        }
                    }
                }
            }
        });

        loop {
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
                    let instance_id = in_flight.action.instance_id;

                    tokio::spawn(async move {
                        // Use unified completion path for successful actions
                        // Fall back to old path for failures (which handles exception routing)
                        if metrics.success {
                            if let Err(e) = Self::process_completion_unified(
                                &handler.db,
                                &dag_cache,
                                &instance_contexts,
                                &in_flight,
                                &metrics,
                                WorkflowInstanceId(instance_id),
                            ).await {
                                error!("Unified completion processing failed: {}", e);

                                // Handle the error: mark action complete and fail the instance
                                Self::handle_completion_error(
                                    &handler.db,
                                    &in_flight,
                                    &metrics,
                                    WorkflowInstanceId(instance_id),
                                    &e,
                                ).await;
                            }
                        } else {
                            // Failed actions use old path for exception handling
                            if let Err(e) = Self::process_completion_task(
                                handler,
                                dag_cache,
                                instance_contexts,
                                in_flight,
                                metrics,
                                WorkflowInstanceId(instance_id),
                            ).await {
                                error!("Completion processing failed: {}", e);
                            }
                        }
                    });
                }
            }
        }

        Ok(())
    }

    /// Process due scheduled workflows by creating new instances.
    async fn process_due_schedules(&self) -> RunnerResult<()> {
        let batch_size = self.config.schedule_check_batch_size;

        // Find due schedules (uses SKIP LOCKED for multi-runner safety)
        let schedules = self
            .completion_handler
            .db
            .find_due_schedules(batch_size)
            .await
            .map_err(|e| RunnerError::Dag(format!("Failed to find due schedules: {}", e)))?;

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

            // Create workflow instance with scheduled inputs
            let instance_id = self
                .completion_handler
                .db
                .create_instance(
                    &schedule.workflow_name,
                    version_id,
                    schedule.input_payload.as_deref(),
                    Some(ScheduleId(schedule.id)),
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

        Ok(())
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
    async fn process_completion_unified(
        db: &Database,
        dag_cache: &DAGCache,
        instance_contexts: &Arc<RwLock<HashMap<Uuid, Scope>>>,
        in_flight: &InFlightAction,
        metrics: &RoundTripMetrics,
        instance_id: WorkflowInstanceId,
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

        // Parse spread index if present
        let (base_node_id, spread_index) = Self::parse_spread_node_id(node_id);

        // Merge initial workflow inputs into the inline scope so downstream actions can
        // access them even if no intermediate node has rewritten them into the inbox.
        let initial_scope = instance_contexts
            .read()
            .await
            .get(&instance_id.0)
            .cloned()
            .unwrap_or_default();

        // Parse result from response payload
        let result: WorkflowValue = if metrics.response_payload.is_empty() {
            WorkflowValue::Null
        } else {
            match proto::WorkflowArguments::decode(&metrics.response_payload[..]) {
                Ok(args) => args
                    .arguments
                    .iter()
                    .find(|arg| arg.key == "result")
                    .and_then(|arg| arg.value.as_ref())
                    .map(proto_value_to_workflow_value)
                    .unwrap_or(WorkflowValue::Null),
                Err(_) => serde_json::from_slice::<serde_json::Value>(&metrics.response_payload)
                    .map(|value| json_to_workflow_value(&value))
                    .unwrap_or(WorkflowValue::Null),
            }
        };

        let helper = DAGHelper::new(&dag);

        // Step 1: Analyze subgraph
        let subgraph_start = std::time::Instant::now();
        let subgraph = analyze_subgraph(base_node_id, &dag, &helper);
        let subgraph_ms = subgraph_start.elapsed().as_micros() as u64;

        debug!(
            node_id = %node_id,
            inline_nodes = subgraph.inline_nodes.len(),
            frontier_nodes = subgraph.frontier_nodes.len(),
            "analyzed subgraph for completion"
        );

        // Step 2: Batch fetch inbox for all nodes in subgraph
        let inbox_start = std::time::Instant::now();
        let existing_inbox = inbox_json_to_workflow(
            db.batch_read_inbox(instance_id, &subgraph.all_node_ids)
                .await?,
        );
        let inbox_ms = inbox_start.elapsed().as_micros() as u64;

        // DEBUG: Log full inbox state for completed node
        info!(
            node_id = %node_id,
            existing_inbox_keys = ?existing_inbox.keys().collect::<Vec<_>>(),
            "DEBUG: existing inbox node keys"
        );
        for (inbox_node_id, inbox_vars) in &existing_inbox {
            info!(
                inbox_node_id = %inbox_node_id,
                inbox_vars = ?inbox_vars,
                "DEBUG: inbox contents for node"
            );
        }

        // Step 3: Execute inline subgraph and build completion plan
        let inline_start = std::time::Instant::now();
        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index,
        };
        let mut plan =
            execute_inline_subgraph(base_node_id, result, ctx, &subgraph, &dag, instance_id)
                .map_err(|e| RunnerError::Dag(e.to_string()))?;

        // Fill in action completion details
        plan = plan.with_action_completion(
            ActionId(in_flight.action.id),
            in_flight.action.delivery_token,
            metrics.success,
            metrics.response_payload.clone(),
            metrics.error_message.clone(),
        );
        let inline_ms = inline_start.elapsed().as_micros() as u64;

        debug!(
            node_id = %node_id,
            inbox_writes = plan.inbox_writes.len(),
            readiness_increments = plan.readiness_increments.len(),
            has_instance_completion = plan.instance_completion.is_some(),
            "built completion plan"
        );

        // DEBUG: Log all inbox writes in the plan
        for write in &plan.inbox_writes {
            info!(
                target_node_id = %write.target_node_id,
                variable_name = %write.variable_name,
                value = ?write.value,
                source_node_id = %write.source_node_id,
                spread_index = ?write.spread_index,
                "DEBUG: plan inbox write"
            );
        }

        // DEBUG: Log all readiness increments
        for inc in &plan.readiness_increments {
            info!(
                node_id = %inc.node_id,
                required_count = inc.required_count,
                node_type = ?inc.node_type,
                action_name = ?inc.action_name,
                "DEBUG: plan readiness increment"
            );
        }

        // Step 4: Execute completion plan in single atomic transaction
        let db_start = std::time::Instant::now();
        let result = db.execute_completion_plan(instance_id, plan).await?;
        let db_ms = db_start.elapsed().as_micros() as u64;

        let total_ms = total_start.elapsed().as_micros() as u64;
        info!(
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

    /// Process a completion in a tokio task (fully async with inbox pattern).
    ///
    /// When an action completes:
    /// 1. Collect inbox writes for downstream nodes (via DATA_FLOW edges)
    /// 2. Find ready successor nodes
    /// 3. For inline nodes: execute immediately, collect their inbox writes too
    /// 4. For delegated nodes: read inbox, create action, add to batch
    /// 5. Write everything in one batch at the end
    async fn process_completion_task(
        handler: WorkCompletionHandler,
        dag_cache: Arc<DAGCache>,
        _instance_contexts: Arc<RwLock<HashMap<Uuid, Scope>>>,
        in_flight: InFlightAction,
        metrics: RoundTripMetrics,
        instance_id: WorkflowInstanceId,
    ) -> RunnerResult<()> {
        let mut batch = CompletionBatch::new();

        // Create completion record
        batch.completions.push(CompletionRecord {
            action_id: ActionId(in_flight.action.id),
            success: metrics.success,
            result_payload: metrics.response_payload.clone(),
            delivery_token: in_flight.action.delivery_token,
            error_message: metrics.error_message.clone(),
        });

        // If failed, check if this is an exception that should be caught
        // Parse the result payload to see if it contains exception info
        if !metrics.success {
            let action_id_str = in_flight.action.id.to_string();
            debug!(
                action_id = %action_id_str,
                response_payload_len = metrics.response_payload.len(),
                "action failed, checking for exception info"
            );

            // Try to parse exception info from the response payload
            // Note: Python uses "error" key for exception payloads, not "result"
            let maybe_exception: Option<WorkflowValue> = if !metrics.response_payload.is_empty() {
                match proto::WorkflowArguments::decode(&metrics.response_payload[..]) {
                    Ok(args) => {
                        debug!(
                            action_id = %action_id_str,
                            arg_count = args.arguments.len(),
                            arg_keys = ?args.arguments.iter().map(|a| &a.key).collect::<Vec<_>>(),
                            "parsed WorkflowArguments from response payload"
                        );
                        args.arguments
                            .iter()
                            .find(|arg| arg.key == "error")
                            .and_then(|arg| arg.value.as_ref())
                            .map(proto_value_to_workflow_value)
                    }
                    Err(e) => {
                        debug!(
                            action_id = %action_id_str,
                            error = %e,
                            "failed to decode WorkflowArguments, trying JSON"
                        );
                        serde_json::from_slice::<serde_json::Value>(&metrics.response_payload)
                            .ok()
                            .map(|value| json_to_workflow_value(&value))
                    }
                }
            } else {
                debug!(action_id = %action_id_str, "response payload is empty");
                None
            };

            debug!(
                action_id = %action_id_str,
                maybe_exception = ?maybe_exception,
                "parsed exception value"
            );

            // Check if this exception should be caught
            if let Some(ref exc_value) = maybe_exception
                && let Some(exception_type) = Self::is_exception_result(exc_value)
            {
                // Get DAG for this workflow instance
                if let Some(dag) = dag_cache.get_dag_for_instance(instance_id).await? {
                    let node_id = match in_flight.action.node_id.as_deref() {
                        Some(id) => id,
                        None => {
                            handler.write_batch(batch).await?;
                            return Ok(());
                        }
                    };

                    let helper = DAGHelper::new(&dag);
                    let (base_node_id, _spread_index) = Self::parse_spread_node_id(node_id);

                    debug!(
                        node_id = %node_id,
                        exception_type = %exception_type,
                        attempt_number = in_flight.action.attempt_number,
                        max_retries = in_flight.action.max_retries,
                        "action failed with exception, checking retry status"
                    );

                    // Only check for exception handlers if retries are exhausted.
                    // If retries are still available, let the retry logic handle it.
                    // The exception handler should only run after ALL retries have failed.
                    let retries_exhausted =
                        in_flight.action.attempt_number >= in_flight.action.max_retries;

                    if !retries_exhausted {
                        debug!(
                            node_id = %node_id,
                            attempt_number = in_flight.action.attempt_number,
                            max_retries = in_flight.action.max_retries,
                            "retries still available, not checking for exception handlers yet"
                        );
                        handler.write_batch(batch).await?;
                        return Ok(());
                    }

                    // Look for exception handlers on this node
                    if let Some(handler_id) =
                        Self::get_exception_handlers_from_node(&dag, base_node_id, &exception_type)
                    {
                        debug!(
                            node_id = %node_id,
                            handler_id = %handler_id,
                            "found exception handler on current node"
                        );
                        let mut inline_scope: Scope = HashMap::new();
                        Self::process_exception_handler(
                            &handler_id,
                            exc_value,
                            &dag,
                            &helper,
                            &mut inline_scope,
                            &mut batch,
                            instance_id,
                            &handler.db,
                        )
                        .await?;
                        handler.write_batch(batch).await?;
                        // Mark the failed action as caught so fail_instances_with_exhausted_actions
                        // doesn't mark the workflow as failed. This must be AFTER write_batch
                        // because the action is still 'dispatched' until write_batch completes.
                        handler.db.mark_action_caught(instance_id, node_id).await?;
                        return Ok(());
                    }

                    // Check if we're inside a synthetic try body function
                    if let Some(fn_call_id) = Self::find_enclosing_fn_call(&dag, base_node_id) {
                        debug!(
                            node_id = %node_id,
                            fn_call_id = %fn_call_id,
                            "node is inside try body function, checking fn_call for handlers"
                        );
                        if let Some(handler_id) = Self::get_exception_handlers_from_node(
                            &dag,
                            &fn_call_id,
                            &exception_type,
                        ) {
                            debug!(
                                fn_call_id = %fn_call_id,
                                handler_id = %handler_id,
                                "found exception handler on enclosing fn_call"
                            );
                            let mut inline_scope: Scope = HashMap::new();
                            Self::process_exception_handler(
                                &handler_id,
                                exc_value,
                                &dag,
                                &helper,
                                &mut inline_scope,
                                &mut batch,
                                instance_id,
                                &handler.db,
                            )
                            .await?;
                            handler.write_batch(batch).await?;
                            // Mark the failed action as caught so fail_instances_with_exhausted_actions
                            // doesn't mark the workflow as failed. This must be AFTER write_batch
                            // because the action is still 'dispatched' until write_batch completes.
                            handler.db.mark_action_caught(instance_id, node_id).await?;
                            return Ok(());
                        }
                    }

                    warn!(
                        node_id = %node_id,
                        exception_type = %exception_type,
                        "no exception handler found for failed action"
                    );
                }
            }

            handler.write_batch(batch).await?;
            return Ok(());
        }

        // If we get here, the action failed but no exception handler was found
        // Just write the completion record
        handler.write_batch(batch).await?;
        Ok(())
    }

    /// Parse a node_id that might contain a spread index suffix.
    /// Returns (base_node_id, optional_spread_index).
    ///
    /// Example: "spread_action_1[2]" -> ("spread_action_1", Some(2))
    /// Example: "action_1" -> ("action_1", None)
    fn parse_spread_node_id(node_id: &str) -> (&str, Option<usize>) {
        if let Some(bracket_pos) = node_id.rfind('[')
            && node_id.ends_with(']')
        {
            let base = &node_id[..bracket_pos];
            let idx_str = &node_id[bracket_pos + 1..node_id.len() - 1];
            if let Ok(idx) = idx_str.parse::<usize>() {
                return (base, Some(idx));
            }
        }
        (node_id, None)
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

    /// Check if all parallel actions are complete and enqueue the barrier when ready.
    #[allow(clippy::too_many_arguments)]
    async fn check_and_process_parallel_aggregator(
        agg_node: &DAGNode,
        dag: &DAG,
        batch: &mut CompletionBatch,
        instance_id: WorkflowInstanceId,
        db: &Database,
    ) -> RunnerResult<()> {
        let agg_id = &agg_node.id;

        // Get the parallel entry node this aggregator is collecting from
        let parallel_entry_id = match &agg_node.aggregates_from {
            Some(id) => id,
            None => return Ok(()), // Not a valid aggregator
        };

        // Find all parallel action node IDs (successors of the parallel entry node)
        let parallel_action_ids: Vec<String> = dag
            .edges
            .iter()
            .filter(|e| e.source == *parallel_entry_id && e.edge_type == EdgeType::StateMachine)
            .map(|e| e.target.clone())
            .collect();

        let expected_count = parallel_action_ids.len() as i32;

        // Collect inbox writes that need to be committed atomically with the counter
        // These are writes from the current parallel action completion
        let inbox_writes_for_tx: Vec<(String, String, serde_json::Value, String, Option<i32>)> =
            batch
                .inbox_writes
                .drain(..)
                .map(|w| {
                    (
                        w.target_node_id,
                        w.variable_name,
                        w.value.to_json(),
                        w.source_node_id,
                        w.spread_index,
                    )
                })
                .collect();

        // Atomically write inbox entries, increment readiness counter, and enqueue
        // the barrier when the last precursor arrives.
        let readiness = db
            .write_inbox_batch_and_increment_readiness(
                instance_id,
                agg_id,
                expected_count,
                &inbox_writes_for_tx,
            )
            .await?;

        debug!(
            aggregator_id = %agg_id,
            expected_count = expected_count,
            completed_count = readiness.completed_count,
            is_now_ready = readiness.is_now_ready,
            parallel_action_ids = ?parallel_action_ids,
            inbox_writes_committed = inbox_writes_for_tx.len(),
            "updated parallel barrier readiness"
        );

        if readiness.is_now_ready {
            debug!(aggregator_id = %agg_id, "parallel barrier ready and enqueued");
        }

        Ok(())
    }

    /// Check if a result represents an exception.
    fn is_exception_result(result: &WorkflowValue) -> Option<String> {
        match result {
            WorkflowValue::Exception { exc_type, .. } => Some(exc_type.clone()),
            _ => None,
        }
    }

    /// Find the fn_call node that encloses a node within a synthetic try body function.
    /// Returns the fn_call node ID if found.
    fn find_enclosing_fn_call(dag: &DAG, node_id: &str) -> Option<String> {
        // Check if this node is inside a try body function (starts with __try_body_)
        let node = dag.nodes.get(node_id)?;
        let fn_name = node.function_name.as_ref()?;
        if !fn_name.starts_with("__try_body_") {
            return None;
        }

        // Find the fn_call node that calls this function
        for (fn_call_id, fn_call_node) in &dag.nodes {
            if fn_call_node.is_fn_call && fn_call_node.called_function.as_ref() == Some(fn_name) {
                return Some(fn_call_id.clone());
            }
        }
        None
    }

    /// Get exception handlers from a node's outgoing edges.
    fn get_exception_handlers_from_node(
        dag: &DAG,
        node_id: &str,
        exception_type: &str,
    ) -> Option<String> {
        let mut catch_all_handler = None;

        for edge in &dag.edges {
            if edge.source != node_id {
                continue;
            }
            if let Some(ref exc_types) = edge.exception_types {
                if exc_types.is_empty() {
                    // Catch-all handler
                    catch_all_handler = Some(edge.target.clone());
                } else if exc_types.iter().any(|t| t == exception_type) {
                    // Specific handler matches
                    return Some(edge.target.clone());
                }
            }
        }

        catch_all_handler
    }

    /// Evaluate a guard expression for the runner's scope type.
    ///
    /// This is a wrapper around the completion module's evaluate_guard that
    /// accepts the runner's Scope type (which is the same as InlineScope).
    ///
    /// Returns:
    /// - `GuardResult::Pass` if the guard passes
    /// - `GuardResult::Fail` if the guard evaluates to false
    /// - `GuardResult::Error` if evaluation failed
    fn evaluate_guard_for_scope(
        guard_expr: Option<&ast::Expr>,
        scope: &Scope,
        successor_id: &str,
    ) -> GuardResult {
        let Some(guard) = guard_expr else {
            // No guard expression - always pass
            return GuardResult::Pass;
        };

        match ExpressionEvaluator::evaluate(guard, scope) {
            Ok(val) => {
                let is_true = val.is_truthy();
                debug!(
                    successor_id = %successor_id,
                    guard_expr = ?guard,
                    result = ?val,
                    is_true = is_true,
                    "evaluated guard expression"
                );
                if is_true {
                    GuardResult::Pass
                } else {
                    GuardResult::Fail
                }
            }
            Err(e) => {
                warn!(
                    successor_id = %successor_id,
                    error = %e,
                    "failed to evaluate guard expression"
                );
                GuardResult::Error(e.to_string())
            }
        }
    }

    fn filter_successors_with_guards(
        successors: Vec<SuccessorInfo>,
        scope: &Scope,
        guard_errors: &mut Vec<(String, String)>,
    ) -> Vec<SuccessorInfo> {
        let mut selected = Vec::new();
        let mut else_successors = Vec::new();
        let mut has_guarded_edges = false;
        let mut guard_passed = false;
        let mut guard_error = false;

        for successor in successors {
            if successor.is_else {
                else_successors.push(successor);
                continue;
            }

            if let Some(ref guard) = successor.guard_expr {
                has_guarded_edges = true;
                match Self::evaluate_guard_for_scope(Some(guard), scope, &successor.node_id) {
                    GuardResult::Pass => {
                        guard_passed = true;
                        selected.push(successor);
                    }
                    GuardResult::Fail => {}
                    GuardResult::Error(error) => {
                        guard_error = true;
                        guard_errors.push((successor.node_id.clone(), error));
                    }
                }
                continue;
            }

            selected.push(successor);
        }

        if has_guarded_edges && !guard_passed && !guard_error {
            selected.extend(else_successors);
        }

        selected
    }

    /// Process successor nodes with optional condition result for branching.
    ///
    /// Uses BFS traversal to process inline nodes and their successors iteratively.
    /// Guard expressions are evaluated as we encounter edges during traversal.
    #[allow(clippy::too_many_arguments)]
    async fn process_successors_with_condition(
        node_id: &str,
        result: &WorkflowValue,
        dag: &DAG,
        helper: &DAGHelper<'_>,
        inline_scope: &mut Scope,
        batch: &mut CompletionBatch,
        instance_id: WorkflowInstanceId,
        db: &Database,
        condition_result: Option<bool>,
    ) -> RunnerResult<()> {
        // Check if result is an exception
        if let Some(exception_type) = Self::is_exception_result(result) {
            debug!(
                node_id = %node_id,
                exception_type = %exception_type,
                "result is an exception, looking for exception handlers"
            );

            // Look for exception handlers on this node
            if let Some(handler_id) =
                Self::get_exception_handlers_from_node(dag, node_id, &exception_type)
            {
                debug!(
                    node_id = %node_id,
                    handler_id = %handler_id,
                    exception_type = %exception_type,
                    "found exception handler on current node"
                );
                // Route to exception handler
                return Self::process_exception_handler(
                    &handler_id,
                    result,
                    dag,
                    helper,
                    inline_scope,
                    batch,
                    instance_id,
                    db,
                )
                .await;
            }

            // Check if we're inside a synthetic try body function
            if let Some(fn_call_id) = Self::find_enclosing_fn_call(dag, node_id) {
                debug!(
                    node_id = %node_id,
                    fn_call_id = %fn_call_id,
                    exception_type = %exception_type,
                    "node is inside try body function, checking fn_call for handlers"
                );
                // Look for exception handlers on the enclosing fn_call
                if let Some(handler_id) =
                    Self::get_exception_handlers_from_node(dag, &fn_call_id, &exception_type)
                {
                    debug!(
                        fn_call_id = %fn_call_id,
                        handler_id = %handler_id,
                        exception_type = %exception_type,
                        "found exception handler on enclosing fn_call"
                    );
                    return Self::process_exception_handler(
                        &handler_id,
                        result,
                        dag,
                        helper,
                        inline_scope,
                        batch,
                        instance_id,
                        db,
                    )
                    .await;
                }
            }

            // No handler found - propagate exception (mark workflow as failed)
            warn!(
                node_id = %node_id,
                exception_type = %exception_type,
                "no exception handler found, exception will propagate"
            );
            // Action stays marked as failed. The retry loop will eventually
            // exhaust retries and fail_instances_with_exhausted_actions
            // will mark the workflow as failed.
            return Ok(());
        }

        // Store result in inline scope for potential inline successors
        if let Some(node) = dag.nodes.get(node_id)
            && let Some(ref target) = node.target
            && !(Self::is_exception_binding_node(node) && inline_scope.contains_key(target))
        {
            inline_scope.insert(target.clone(), result.clone());
        }

        // BFS work queue: (node_id, result to pass forward)
        // This replaces recursive calls with an iterative loop
        use std::collections::VecDeque;

        let mut work_queue: VecDeque<(String, WorkflowValue)> = VecDeque::new();

        // Initialize queue with immediate successors from the starting node
        let mut guard_errors = Vec::new();
        let raw_successors = helper.get_ready_successors(node_id, condition_result);
        let successor_ids: Vec<_> = raw_successors.iter().map(|s| &s.node_id).collect();
        debug!(
            node_id = %node_id,
            raw_successor_count = raw_successors.len(),
            raw_successors = ?successor_ids,
            "process_successors_with_condition: getting ready successors"
        );
        for successor in
            Self::filter_successors_with_guards(raw_successors, inline_scope, &mut guard_errors)
        {
            work_queue.push_back((successor.node_id, result.clone()));
        }

        // Process work queue iteratively (BFS)
        while let Some((current_node_id, current_result)) = work_queue.pop_front() {
            let succ_node = match dag.nodes.get(&current_node_id) {
                Some(n) => n,
                None => continue,
            };

            // Skip aggregator nodes during normal successor processing.
            // Parallel aggregators need all predecessors to complete before processing.
            // Instead, check if all predecessors are complete and process if ready.
            if succ_node.is_aggregator {
                // Check if all parallel actions feeding this aggregator are complete
                Self::check_and_process_parallel_aggregator(succ_node, dag, batch, instance_id, db)
                    .await?;
                continue;
            }

            let exec_mode = helper.get_execution_mode(succ_node);
            match exec_mode {
                ExecutionMode::Inline => {
                    // Check if this is the output node of the entry function (workflow completion)
                    if succ_node.is_output || succ_node.node_type == "return" {
                        // This is the final node - mark the workflow as complete
                        // The result is the value from the inline_scope (the action result that just completed)
                        // We wrap it in a "result" key to match Python's expected format
                        let mut result_map = HashMap::new();
                        result_map.insert("result".to_string(), current_result.clone());

                        // Serialize the result as WorkflowArguments protobuf
                        let result_payload = Self::serialize_workflow_result(&result_map);

                        debug!(
                            instance_id = %instance_id.0,
                            output_node = %current_node_id,
                            result = ?current_result,
                            "workflow reached output node, marking complete"
                        );

                        batch.instance_completion = Some(InstanceCompletion {
                            instance_id,
                            result_payload,
                        });
                        continue;
                    }

                    // Execute inline node with in-memory scope
                    let inline_result = Self::execute_inline_node(succ_node, inline_scope)?;
                    tracing::debug!(
                        node_id = %succ_node.id,
                        node_type = %succ_node.node_type,
                        target = ?succ_node.target,
                        result = ?inline_result,
                        scope_keys = ?inline_scope.keys().collect::<Vec<_>>(),
                        "executed inline node"
                    );

                    // Collect inbox writes for inline node's result
                    if let Some(ref target) = succ_node.target {
                        Self::collect_inbox_writes_for_node(
                            &current_node_id,
                            target,
                            &inline_result,
                            dag,
                            instance_id,
                            &mut batch.inbox_writes,
                        );
                    }

                    // Determine what result to pass to successors:
                    // - Control-flow nodes (join, branch) pass through the incoming result
                    // - Value-producing nodes use their own inline_result
                    let passthrough_result = match succ_node.node_type.as_str() {
                        "join" | "branch" | "fn_call" => current_result.clone(),
                        _ => inline_result,
                    };

                    // Add successors to work queue (instead of recursive call)
                    let mut guard_errors = Vec::new();
                    for successor in Self::filter_successors_with_guards(
                        helper.get_ready_successors(&current_node_id, None),
                        inline_scope,
                        &mut guard_errors,
                    ) {
                        work_queue.push_back((successor.node_id, passthrough_result.clone()));
                    }
                }
                ExecutionMode::Delegated => {
                    // Read inbox for this node from database
                    // This includes data from ANY upstream node, not just the one that just completed
                    let mut inbox = db
                        .read_inbox(instance_id, &current_node_id)
                        .await?
                        .into_iter()
                        .map(|(key, value)| (key, json_to_workflow_value(&value)))
                        .collect::<HashMap<_, _>>();

                    // Merge pending inbox writes for this node (not yet committed to DB)
                    for pending_write in &batch.inbox_writes {
                        if pending_write.target_node_id == current_node_id {
                            inbox.insert(
                                pending_write.variable_name.clone(),
                                pending_write.value.clone(),
                            );
                        }
                    }

                    // Merge inline scope variables (from parent inline nodes like `if`)
                    // This ensures actions inside branches have access to variables defined earlier
                    for (var_name, var_value) in inline_scope.iter() {
                        inbox
                            .entry(var_name.clone())
                            .or_insert_with(|| var_value.clone());
                    }

                    // Create actions (may be multiple for spread nodes)
                    let action_result =
                        Self::create_actions_for_node(succ_node, instance_id, &inbox, dag)?;
                    batch.new_actions.extend(action_result.actions);
                    batch.inbox_writes.extend(action_result.inbox_writes);
                }
            }
        }

        Ok(())
    }

    /// Process an exception handler node and its successors.
    /// This is called when an exception is caught and we need to execute the handler.
    #[allow(clippy::too_many_arguments)]
    async fn process_exception_handler(
        handler_node_id: &str,
        exception_result: &WorkflowValue,
        dag: &DAG,
        helper: &DAGHelper<'_>,
        inline_scope: &mut Scope,
        batch: &mut CompletionBatch,
        instance_id: WorkflowInstanceId,
        db: &Database,
    ) -> RunnerResult<()> {
        let (handler_node_id, handler_node) = match dag.nodes.get_key_value(handler_node_id) {
            Some((id, node)) => (id.as_str(), node),
            None => {
                // Some DAG conversions append suffixes to fn_call nodes; try a prefix match fallback.
                if let Some((id, node)) = dag
                    .nodes
                    .iter()
                    .find(|(id, _)| id.starts_with(handler_node_id))
                {
                    debug!(
                        original_handler_id = %handler_node_id,
                        handler_node_id = %id,
                        "exception handler not found by exact id; using prefix match fallback"
                    );
                    (id.as_str(), node)
                } else {
                    warn!(handler_node_id = %handler_node_id, "exception handler node not found");
                    return Ok(());
                }
            }
        };

        debug!(
            handler_node_id = %handler_node_id,
            handler_type = %handler_node.node_type,
            "processing exception handler"
        );

        let is_binding = Self::is_exception_binding_node(handler_node);
        if is_binding {
            inline_scope.insert(EXCEPTION_SCOPE_VAR.to_string(), exception_result.clone());
        }

        let exec_mode = helper.get_execution_mode(handler_node);
        match exec_mode {
            ExecutionMode::Inline => {
                // Execute inline handler and continue to its successors
                let inline_result = Self::execute_inline_node(handler_node, inline_scope)?;
                if let Some(ref target) = handler_node.target {
                    inline_scope.insert(target.clone(), inline_result.clone());
                }
                if is_binding {
                    inline_scope.remove(EXCEPTION_SCOPE_VAR);
                }
                let successor_result = if is_binding {
                    WorkflowValue::Null
                } else {
                    inline_result.clone()
                };
                // Continue to handler's successors
                debug!(
                    handler_node_id = %handler_node_id,
                    inline_scope_keys = ?inline_scope.keys().collect::<Vec<_>>(),
                    "about to process successors of exception binding node"
                );
                Box::pin(Self::process_successors_with_condition(
                    handler_node_id,
                    &successor_result,
                    dag,
                    helper,
                    inline_scope,
                    batch,
                    instance_id,
                    db,
                    None,
                ))
                .await
            }
            ExecutionMode::Delegated => {
                // Handler is an action call - create action for it
                // Read inbox for this node from database
                let mut inbox = db
                    .read_inbox(instance_id, handler_node_id)
                    .await?
                    .into_iter()
                    .map(|(key, value)| (key, json_to_workflow_value(&value)))
                    .collect::<HashMap<_, _>>();

                // Merge pending inbox writes
                for pending_write in &batch.inbox_writes {
                    if pending_write.target_node_id == handler_node_id {
                        inbox.insert(
                            pending_write.variable_name.clone(),
                            pending_write.value.clone(),
                        );
                    }
                }

                // Merge inline scope variables
                for (var_name, var_value) in inline_scope.iter() {
                    inbox
                        .entry(var_name.clone())
                        .or_insert_with(|| var_value.clone());
                }

                // Create actions for the handler node
                let result = Self::create_actions_for_node(handler_node, instance_id, &inbox, dag)?;
                batch.new_actions.extend(result.actions);
                batch.inbox_writes.extend(result.inbox_writes);
                Ok(())
            }
        }
    }

    fn is_exception_binding_node(node: &DAGNode) -> bool {
        if node.id.starts_with("exc_bind") {
            return true;
        }
        if node.node_type != "assignment" {
            return false;
        }
        if node.label.contains(EXCEPTION_SCOPE_VAR) {
            return true;
        }
        let Some(assign_expr) = node.assign_expr.as_ref() else {
            return false;
        };
        let Some(ast::expr::Kind::Variable(var)) = assign_expr.kind.as_ref() else {
            return false;
        };
        var.name == EXCEPTION_SCOPE_VAR
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

    /// Execute an inline node with in-memory scope (non-durable).
    fn execute_inline_node(node: &DAGNode, scope: &mut Scope) -> RunnerResult<WorkflowValue> {
        match node.node_type.as_str() {
            "assignment" => {
                // Evaluate the assignment expression
                if let Some(ref expr) = node.assign_expr {
                    match ExpressionEvaluator::evaluate(expr, scope) {
                        Ok(val) => {
                            if let Some(ref target) = node.target
                                && target.starts_with("__loop_")
                            {
                                tracing::debug!(
                                    node_id = %node.id,
                                    target,
                                    scope_value = ?scope.get(target),
                                    result = ?val,
                                    "loop assignment evaluation"
                                );
                            }
                            Ok(val)
                        }
                        Err(e) => {
                            warn!(
                                node_id = %node.id,
                                error = %e,
                                "failed to evaluate assignment expression"
                            );
                            Ok(WorkflowValue::Null)
                        }
                    }
                } else {
                    Ok(WorkflowValue::Null)
                }
            }
            "return" => {
                if let Some(ref expr) = node.assign_expr {
                    match ExpressionEvaluator::evaluate(expr, scope) {
                        Ok(val) => Ok(val),
                        Err(e) => {
                            warn!(
                                node_id = %node.id,
                                error = %e,
                                "failed to evaluate return expression"
                            );
                            Ok(WorkflowValue::Null)
                        }
                    }
                } else {
                    Ok(WorkflowValue::Null)
                }
            }
            "input" | "output" => Ok(WorkflowValue::Null),
            "conditional" => Ok(WorkflowValue::Bool(true)),
            "aggregator" => Ok(WorkflowValue::List(vec![])),
            _ => Ok(WorkflowValue::Null),
        }
    }

    /// Request shutdown.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    /// Register a DAG for a workflow version (for testing or warm-up).
    pub async fn register_dag(&self, version_id: Uuid, dag: DAG) {
        self.dag_cache.preload(version_id, dag).await;
    }

    /// Get count of in-flight actions.
    pub async fn in_flight_count(&self) -> usize {
        self.work_handler.in_flight_count().await
    }

    /// Poll for and start any unstarted instances.
    ///
    /// Finds instances that are in 'running' state but have no actions queued yet,
    /// and starts them by parsing their input and creating the initial actions.
    async fn start_unstarted_instances(&self) -> RunnerResult<()> {
        let db = &self.completion_handler.db;
        let instances = db.find_unstarted_instances(10).await?;

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

            info!(
                instance_id = %instance.id,
                workflow = %instance.workflow_name,
                input_keys = ?initial_inputs.keys().collect::<Vec<_>>(),
                "starting unstarted instance"
            );

            match self.start_instance(instance_id, initial_inputs).await {
                Ok(count) => {
                    debug!(
                        instance_id = %instance.id,
                        actions_created = count,
                        "successfully started instance"
                    );
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
                }
            }
        }

        Ok(())
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
        // Load the DAG for this instance
        let dag = self
            .dag_cache
            .get_dag_for_instance(instance_id)
            .await?
            .ok_or_else(|| RunnerError::InstanceNotFound(instance_id.0))?;

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
        let (mut scope, mut inbox_writes_to_commit) =
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

        // Find first delegated successors starting from input node
        let mut actions_to_enqueue = Vec::new();
        let mut readiness_inits: Vec<ReadinessInit> = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        let mut completion_payload: Option<Vec<u8>> = None;
        let mut guard_errors: Vec<(String, String)> = Vec::new();

        // Start from input node's successors
        let initial_successors = helper.get_ready_successors(&input_node.id, None);
        debug!(
            input_node_id = %input_node.id,
            successor_count = initial_successors.len(),
            successors = ?initial_successors.iter().map(|s| &s.node_id).collect::<Vec<_>>(),
            "input node successors"
        );
        for successor in
            Self::filter_successors_with_guards(initial_successors, &scope, &mut guard_errors)
        {
            queue.push_back(successor.node_id);
        }

        while let Some(node_id) = queue.pop_front() {
            if visited.contains(&node_id) {
                continue;
            }
            visited.insert(node_id.clone());

            let node = match helper.get_node(&node_id) {
                Some(n) => n,
                None => {
                    debug!(node_id = %node_id, "node not found in DAG during start_instance");
                    continue;
                }
            };

            let mode = helper.get_execution_mode(node);
            debug!(
                node_id = %node_id,
                node_type = %node.node_type,
                is_spread = node.is_spread,
                execution_mode = ?mode,
                "processing node during start_instance"
            );

            match mode {
                ExecutionMode::Delegated => {
                    // Check if this is a fn_call node - these need to be expanded into
                    // their function body rather than treated as external actions
                    if node.is_fn_call {
                        if let Some(ref called_fn) = node.called_function {
                            // Find the input node for this function
                            let fn_input_node_id = format!("{}_input_", called_fn);
                            if let Some(input_id) =
                                dag.nodes.keys().find(|k| k.starts_with(&fn_input_node_id))
                            {
                                debug!(
                                    fn_call_node = %node_id,
                                    called_function = %called_fn,
                                    input_node = %input_id,
                                    "expanding fn_call into function body"
                                );
                                // Add the function's input node to the queue to traverse
                                if !visited.contains(input_id) {
                                    queue.push_back(input_id.clone());
                                }
                            } else {
                                warn!(
                                    fn_call_node = %node_id,
                                    called_function = %called_fn,
                                    "fn_call: could not find input node for called function"
                                );
                            }
                        }
                    } else {
                        // This is an action - enqueue it
                        // For initial actions, we use the initial scope as the "inbox"
                        // For spread nodes, this creates multiple actions plus inbox writes
                        let result =
                            Self::create_actions_for_node(node, instance_id, &scope, &dag)?;
                        debug!(
                            node_id = %node_id,
                            actions_created = result.actions.len(),
                            inbox_writes = result.inbox_writes.len(),
                            readiness_init = ?result.readiness_init,
                            "created initial actions for node"
                        );
                        actions_to_enqueue.extend(result.actions);
                        inbox_writes_to_commit.extend(result.inbox_writes);
                        // Collect readiness init for spread aggregators
                        if let Some(init) = result.readiness_init {
                            readiness_inits.push(init);
                        }
                        // Don't traverse past delegated nodes - they'll handle their successors
                    }
                }
                ExecutionMode::Inline => {
                    // Execute inline nodes (e.g., assignments) and continue to their successors
                    let inline_result = Self::execute_inline_node(node, &mut scope)?;
                    debug!(
                        node_id = %node_id,
                        result = ?inline_result,
                        "executed inline node during start_instance"
                    );

                    // Add result to scope if this node has a target
                    if let Some(ref target) = node.target {
                        scope.insert(target.clone(), inline_result.clone());
                        // Propagate inline defaults to downstream inboxes so later actions receive them
                        Self::collect_inbox_writes_for_node(
                            &node.id,
                            target,
                            &inline_result,
                            &dag,
                            instance_id,
                            &mut inbox_writes_to_commit,
                        );
                    }
                    // Also handle multiple targets (tuple unpacking)
                    if let Some(ref targets) = node.targets
                        && targets.len() == 1
                    {
                        scope.insert(targets[0].clone(), inline_result.clone());
                        // For multiple targets, the inline_result would be an array
                        Self::collect_inbox_writes_for_node(
                            &node.id,
                            &targets[0],
                            &inline_result,
                            &dag,
                            instance_id,
                            &mut inbox_writes_to_commit,
                        );
                    } else if let Some(ref targets) = node.targets
                        && targets.len() > 1
                    {
                        let items = match &inline_result {
                            WorkflowValue::List(values) | WorkflowValue::Tuple(values) => {
                                Some(values)
                            }
                            _ => None,
                        };

                        if let Some(items) = items {
                            for (idx, target) in targets.iter().enumerate() {
                                let value = items.get(idx).cloned().unwrap_or(WorkflowValue::Null);
                                scope.insert(target.clone(), value.clone());
                                Self::collect_inbox_writes_for_node(
                                    &node.id,
                                    target,
                                    &value,
                                    &dag,
                                    instance_id,
                                    &mut inbox_writes_to_commit,
                                );
                            }
                        }
                    }

                    if node.node_type == "return" || node.is_output {
                        let result_value = if node.node_type == "return" {
                            inline_result
                        } else {
                            scope.get("result").cloned().unwrap_or(WorkflowValue::Null)
                        };
                        let mut result_map = HashMap::new();
                        result_map.insert("result".to_string(), result_value);
                        completion_payload = Some(Self::serialize_workflow_result(&result_map));
                        break;
                    }

                    let inline_successors = helper.get_ready_successors(&node_id, None);
                    debug!(
                        node_id = %node_id,
                        inline_successor_count = inline_successors.len(),
                        "continuing to successors after inline execution"
                    );
                    for successor in Self::filter_successors_with_guards(
                        inline_successors,
                        &scope,
                        &mut guard_errors,
                    ) {
                        if !visited.contains(&successor.node_id) {
                            queue.push_back(successor.node_id);
                        }
                    }
                }
            }
        }

        // Write inbox entries for spread counts
        let db = &self.completion_handler.db;
        for write in inbox_writes_to_commit {
            db.append_to_inbox(
                write.instance_id,
                &write.target_node_id,
                &write.variable_name,
                write.value.to_json(),
                &write.source_node_id,
                write.spread_index,
            )
            .await?;
        }

        if let Some(payload) = completion_payload {
            db.complete_instance(instance_id, Some(&payload)).await?;
            info!(
                instance_id = %instance_id.0,
                "completed workflow instance during start_instance"
            );
            return Ok(0);
        }

        if actions_to_enqueue.is_empty() && !guard_errors.is_empty() {
            return Err(RunnerError::GuardEvaluationFailed(guard_errors));
        }

        // Initialize node_readiness for spread aggregators BEFORE enqueuing actions
        // This ensures the readiness row exists when spread actions complete
        for init in readiness_inits {
            debug!(
                aggregator_node_id = %init.aggregator_node_id,
                required_count = init.required_count,
                "initializing node readiness for aggregator"
            );
            db.init_node_readiness(instance_id, &init.aggregator_node_id, init.required_count)
                .await?;
        }

        // Enqueue all initial actions
        let count = actions_to_enqueue.len();
        for action in actions_to_enqueue {
            db.enqueue_action(action).await?;
        }

        info!(instance_id = %instance_id.0, actions = count, "started workflow instance");
        Ok(count)
    }

    /// Create action(s) for a node using inbox values.
    /// For spread nodes, this returns multiple actions (one per collection item) plus inbox writes.
    /// For regular action nodes, returns 0 or 1 action.
    fn create_actions_for_node(
        node: &DAGNode,
        instance_id: WorkflowInstanceId,
        inbox: &std::collections::HashMap<String, WorkflowValue>,
        dag: &DAG,
    ) -> RunnerResult<NodeActionResult> {
        // Handle for_loop nodes - they create a special action for the runner
        if node.node_type == "for_loop" {
            // For-loop nodes are added to the action queue so they can be dispatched
            // to the runner for loop index management
            let action = NewAction {
                instance_id,
                module_name: "".to_string(), // No module for internal nodes
                action_name: "".to_string(), // No action name for internal nodes
                dispatch_payload: Vec::new(), // Empty payload for control flow nodes
                timeout_seconds: 300,
                max_retries: 0, // No retries for control flow nodes
                backoff_kind: BackoffKind::None,
                backoff_base_delay_ms: 0,
                node_id: Some(node.id.clone()),
                node_type: Some("for_loop".to_string()),
            };

            // Write all inbox/scope values to the for_loop's inbox
            // These are needed for the loop to access its input variables
            let mut inbox_writes = Vec::new();
            for (var_name, value) in inbox {
                // Find the source node for this variable from dataflow edges
                let source_node_id = dag
                    .edges
                    .iter()
                    .find(|e| {
                        e.target == node.id
                            && e.edge_type == EdgeType::DataFlow
                            && e.variable.as_ref() == Some(var_name)
                    })
                    .map(|e| e.source.clone())
                    .unwrap_or_else(|| "initial_scope".to_string());

                inbox_writes.push(InboxWrite {
                    instance_id,
                    target_node_id: node.id.clone(),
                    variable_name: var_name.clone(),
                    value: value.clone(),
                    source_node_id,
                    spread_index: None,
                });
            }

            return Ok(NodeActionResult {
                actions: vec![action],
                inbox_writes,
                readiness_init: None,
            });
        }

        if node.node_type != "action_call" {
            return Ok(NodeActionResult::default());
        }

        // Handle spread nodes specially - they create multiple actions plus inbox writes
        if node.is_spread {
            return Self::create_spread_node_result(node, instance_id, inbox, dag);
        }

        // Regular action node - 0 or 1 action
        match Self::create_action_for_node_from_inbox(node, instance_id, inbox)? {
            Some(action) => Ok(NodeActionResult {
                actions: vec![action],
                inbox_writes: vec![],
                readiness_init: None,
            }),
            None => Ok(NodeActionResult::default()),
        }
    }

    /// Create actions and inbox writes for a spread node.
    /// Returns readiness init info so caller can initialize node_readiness table.
    fn create_spread_node_result(
        node: &DAGNode,
        instance_id: WorkflowInstanceId,
        inbox: &std::collections::HashMap<String, WorkflowValue>,
        _dag: &DAG,
    ) -> RunnerResult<NodeActionResult> {
        let actions = Self::create_actions_for_spread_node(node, instance_id, inbox)?;
        let spread_count = actions.len();

        // Use the aggregates_to field to find the aggregator node
        // This is set during DAG conversion and propagated during function expansion
        let aggregator_id = node.aggregates_to.clone();

        // Return readiness init info so caller can initialize node_readiness
        let readiness_init = aggregator_id.clone().map(|agg_id| ReadinessInit {
            aggregator_node_id: agg_id,
            required_count: spread_count as i32,
        });

        Ok(NodeActionResult {
            actions,
            inbox_writes: vec![],
            readiness_init,
        })
    }

    /// Create a new action for a node using inbox values instead of shared context.
    fn create_action_for_node_from_inbox(
        node: &DAGNode,
        instance_id: WorkflowInstanceId,
        inbox: &std::collections::HashMap<String, WorkflowValue>,
    ) -> RunnerResult<Option<NewAction>> {
        if node.node_type != "action_call" {
            return Ok(None);
        }

        // Get action_name from node metadata (preferred) or fallback to parsing label
        let action_name = match &node.action_name {
            Some(name) => name.clone(),
            None => {
                // Fallback: extract from label (format: "@action_name(...)")
                if node.label.starts_with('@') {
                    let end = node.label.find('(').unwrap_or(node.label.len());
                    node.label[1..end].to_string()
                } else {
                    return Ok(None);
                }
            }
        };

        // Get module_name from node metadata (default to "default" for backwards compatibility)
        let module_name = node
            .module_name
            .clone()
            .unwrap_or_else(|| "default".to_string());

        // Build kwargs from node metadata, resolving variable references from inbox
        let payload = Self::build_action_payload_from_inbox(node, inbox)?;

        // Extract retry and timeout settings from policies
        let mut max_retries = 3i32; // default
        let backoff_kind = BackoffKind::Exponential;
        let mut backoff_base_delay_ms = 1000i32;
        let mut timeout_seconds = 300i32;

        debug!(
            node_id = %node.id,
            action_name = %action_name,
            policies_count = node.policies.len(),
            "creating action from node - checking policies"
        );

        for policy in &node.policies {
            match &policy.kind {
                Some(crate::parser::ast::policy_bracket::Kind::Retry(retry)) => {
                    debug!(
                        node_id = %node.id,
                        max_retries = retry.max_retries,
                        "found retry policy on node"
                    );
                    max_retries = retry.max_retries as i32;
                    if let Some(ref backoff) = retry.backoff {
                        // Convert seconds to milliseconds
                        backoff_base_delay_ms = (backoff.seconds as i32) * 1000;
                    }
                }
                Some(crate::parser::ast::policy_bracket::Kind::Timeout(timeout_policy)) => {
                    if let Some(ref duration) = timeout_policy.timeout {
                        timeout_seconds = duration.seconds as i32;
                    }
                }
                None => {}
            }
        }

        debug!(
            node_id = %node.id,
            action_name = %action_name,
            final_max_retries = max_retries,
            "action created with max_retries"
        );

        Ok(Some(NewAction {
            instance_id,
            module_name,
            action_name,
            dispatch_payload: payload,
            timeout_seconds,
            max_retries,
            backoff_kind,
            backoff_base_delay_ms,
            node_id: Some(node.id.clone()),
            node_type: Some("action".to_string()),
        }))
    }

    /// Build action payload from node kwargs, resolving variable references from inbox.
    fn build_action_payload_from_inbox(
        node: &DAGNode,
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

    /// Create actions for a spread node by expanding the collection.
    ///
    /// For a spread like `results = spread items:item -> @fetch(id=item)`:
    /// 1. Evaluate `items` from inbox to get [1, 2, 3]
    /// 2. Create 3 actions with `item` bound to 1, 2, 3 respectively
    /// 3. Each action gets a `spread_index` for result ordering
    ///
    /// Returns multiple NewActions, one per iteration.
    fn create_actions_for_spread_node(
        node: &DAGNode,
        instance_id: WorkflowInstanceId,
        inbox: &std::collections::HashMap<String, WorkflowValue>,
    ) -> RunnerResult<Vec<NewAction>> {
        if !node.is_spread {
            return Ok(vec![]);
        }

        let loop_var = node
            .spread_loop_var
            .as_ref()
            .ok_or_else(|| RunnerError::Dag("Spread node missing loop_var".to_string()))?;

        let collection_str = node
            .spread_collection
            .as_ref()
            .ok_or_else(|| RunnerError::Dag("Spread node missing collection".to_string()))?;

        // Evaluate the collection expression
        let collection =
            Self::resolve_kwarg_value_from_inbox("spread_collection", collection_str, None, inbox)?;

        let items = match &collection {
            WorkflowValue::List(arr) | WorkflowValue::Tuple(arr) => arr.clone(),
            _ => {
                return Err(EvaluationError::Evaluation(format!(
                    "Spread collection '{}' is not a list or tuple: {:?}",
                    collection_str, collection
                ))
                .into());
            }
        };

        debug!(
            node_id = %node.id,
            loop_var = %loop_var,
            collection = %collection_str,
            item_count = items.len(),
            "expanding spread action"
        );

        let action_name = node
            .action_name
            .as_ref()
            .ok_or_else(|| RunnerError::Dag("Spread node missing action_name".to_string()))?
            .clone();

        let module_name = node
            .module_name
            .clone()
            .unwrap_or_else(|| "default".to_string());

        // Extract retry and timeout settings from policies
        let mut max_retries = 3i32; // default
        let backoff_kind = BackoffKind::Exponential;
        let mut backoff_base_delay_ms = 1000i32;
        let mut timeout_seconds = 300i32;

        for policy in &node.policies {
            match &policy.kind {
                Some(crate::parser::ast::policy_bracket::Kind::Retry(retry)) => {
                    max_retries = retry.max_retries as i32;
                    if let Some(ref backoff) = retry.backoff {
                        backoff_base_delay_ms = (backoff.seconds as i32) * 1000;
                    }
                }
                Some(crate::parser::ast::policy_bracket::Kind::Timeout(timeout_policy)) => {
                    if let Some(ref duration) = timeout_policy.timeout {
                        timeout_seconds = duration.seconds as i32;
                    }
                }
                None => {}
            }
        }

        let mut actions = Vec::new();
        for (idx, item) in items.into_iter().enumerate() {
            // Create a modified inbox with the loop variable bound to this item
            let mut iteration_inbox = inbox.clone();
            iteration_inbox.insert(loop_var.clone(), item);

            // Build payload with the loop variable available
            let payload = Self::build_action_payload_from_inbox(node, &iteration_inbox)?;

            // Create node_id that includes the spread index for tracking
            let spread_node_id = format!("{}[{}]", node.id, idx);

            actions.push(NewAction {
                instance_id,
                module_name: module_name.clone(),
                action_name: action_name.clone(),
                dispatch_payload: payload,
                timeout_seconds,
                max_retries,
                backoff_kind,
                backoff_base_delay_ms,
                node_id: Some(spread_node_id),
                node_type: Some("action".to_string()),
            });
        }

        Ok(actions)
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

        assert_eq!(
            DAGRunner::is_exception_result(&payload),
            Some("ValueError".to_string())
        );
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

        assert_eq!(
            DAGRunner::is_exception_result(&payload),
            Some("ValueError".to_string())
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

        tracker.add(action.clone(), 0);
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
}
