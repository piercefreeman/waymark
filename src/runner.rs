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
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use prost::Message;

use crate::{
    dag::{DAG, DAGConverter, DAGNode},
    dag_state::{DAGHelper, ExecutionMode},
    db::{
        ActionId, BackoffKind, CompletionRecord, Database, NewAction, QueuedAction,
        WorkflowInstanceId, WorkflowVersionId,
    },
    messages::proto,
    parser::ast,
    worker::{ActionDispatchPayload, PythonWorkerPool, RoundTripMetrics},
};

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
    Evaluation(String),

    #[error("DAG error: {0}")]
    Dag(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Instance not found: {0}")]
    InstanceNotFound(Uuid),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    #[error("Action handler not found: {0}")]
    HandlerNotFound(String),

    #[error("Channel closed")]
    ChannelClosed,
}

pub type RunnerResult<T> = Result<T, RunnerError>;

// ============================================================================
// Runtime Value
// ============================================================================

/// Runtime value for expression evaluation.
/// Uses serde_json::Value for JSON-compatible types.
pub type Value = JsonValue;

// ============================================================================
// JSON to WorkflowArguments Conversion
// ============================================================================

/// Convert a JSON value to WorkflowArgumentValue.
fn json_to_workflow_value(value: &JsonValue) -> proto::WorkflowArgumentValue {
    let kind = match value {
        JsonValue::Null => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::NullValue(0)),
            })
        }
        JsonValue::Bool(b) => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::BoolValue(*b)),
            })
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::IntValue(i)),
                })
            } else if let Some(f) = n.as_f64() {
                proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::DoubleValue(f)),
                })
            } else {
                proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::DoubleValue(0.0)),
                })
            }
        }
        JsonValue::String(s) => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                    s.clone(),
                )),
            })
        }
        JsonValue::Array(arr) => {
            let items: Vec<proto::WorkflowArgumentValue> =
                arr.iter().map(json_to_workflow_value).collect();
            proto::workflow_argument_value::Kind::ListValue(proto::WorkflowListArgument { items })
        }
        JsonValue::Object(obj) => {
            let entries: Vec<proto::WorkflowArgument> = obj
                .iter()
                .map(|(k, v)| proto::WorkflowArgument {
                    key: k.clone(),
                    value: Some(json_to_workflow_value(v)),
                })
                .collect();
            proto::workflow_argument_value::Kind::DictValue(proto::WorkflowDictArgument { entries })
        }
    };

    proto::WorkflowArgumentValue { kind: Some(kind) }
}

/// Convert JSON bytes (expected to be an object) to WorkflowArguments.
///
/// The dispatch_payload is stored as JSON bytes representing the kwargs
/// for the action. This function parses and converts them to the proto format.
fn json_bytes_to_workflow_args(payload: &[u8]) -> proto::WorkflowArguments {
    if payload.is_empty() {
        return proto::WorkflowArguments { arguments: vec![] };
    }

    let json: JsonValue = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to parse dispatch payload as JSON: {}", e);
            return proto::WorkflowArguments { arguments: vec![] };
        }
    };

    match json {
        JsonValue::Object(obj) => {
            let arguments: Vec<proto::WorkflowArgument> = obj
                .iter()
                .map(|(k, v)| proto::WorkflowArgument {
                    key: k.clone(),
                    value: Some(json_to_workflow_value(v)),
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
        let dag = converter.convert(&program);

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
        }
    }

    /// Get total available slots across all workers.
    pub fn available_slots(&self) -> usize {
        self.total_available.load(Ordering::SeqCst)
    }

    /// Try to acquire a slot from any worker. Returns worker index if successful.
    pub fn acquire_slot(&self) -> Option<usize> {
        for (idx, slots) in self.worker_slots.iter().enumerate() {
            let current = slots.load(Ordering::SeqCst);
            if current > 0 {
                // Try to decrement
                if slots
                    .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    self.total_available.fetch_sub(1, Ordering::SeqCst);
                    return Some(idx);
                }
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
    pub dispatched_at: std::time::Instant,
}

/// Manages in-flight actions for correlation on completion.
#[derive(Debug, Default)]
pub struct InFlightTracker {
    /// Actions keyed by delivery_token
    actions: HashMap<Uuid, InFlightAction>,
}

impl InFlightTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an action to tracking.
    pub fn add(&mut self, action: QueuedAction, worker_idx: usize) {
        self.actions.insert(
            action.delivery_token,
            InFlightAction {
                action,
                worker_idx,
                dispatched_at: std::time::Instant::now(),
            },
        );
    }

    /// Remove and return an action by delivery token.
    pub fn remove(&mut self, delivery_token: &Uuid) -> Option<InFlightAction> {
        self.actions.remove(delivery_token)
    }

    /// Get current count of in-flight actions.
    pub fn count(&self) -> usize {
        self.actions.len()
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
    /// Context updates (instance_id -> variables)
    pub context_updates: HashMap<Uuid, JsonValue>,
}

impl CompletionBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.completions.is_empty()
            && self.new_actions.is_empty()
            && self.context_updates.is_empty()
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
}

impl WorkQueueHandler {
    pub fn new(
        db: Arc<Database>,
        worker_pool: Arc<PythonWorkerPool>,
        slot_tracker: Arc<WorkerSlotTracker>,
        in_flight: Arc<Mutex<InFlightTracker>>,
    ) -> Self {
        Self {
            db,
            worker_pool,
            slot_tracker,
            in_flight,
        }
    }

    /// Get the number of available worker slots.
    pub fn available_slots(&self) -> usize {
        self.slot_tracker.available_slots()
    }

    /// Fetch and dispatch a batch of actions to workers.
    ///
    /// Returns immediately after dispatching. Completions are sent to the provided channel.
    pub async fn fetch_and_dispatch(
        &self,
        batch_size: usize,
        completion_tx: mpsc::Sender<(InFlightAction, RoundTripMetrics)>,
    ) -> RunnerResult<usize> {
        let available = self.slot_tracker.available_slots();
        if available == 0 {
            return Ok(0);
        }

        let limit = available.min(batch_size);
        let actions = self.db.dispatch_actions(limit as i32).await?;
        let dispatched = actions.len();

        for action in actions {
            self.dispatch_action(action, completion_tx.clone()).await?;
        }

        Ok(dispatched)
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
        let worker = Arc::clone(&self.worker_pool.workers()[worker_idx]);
        let delivery_token = action.delivery_token;
        let in_flight_tracker = Arc::clone(&self.in_flight);
        let slot_tracker = Arc::clone(&self.slot_tracker);

        tokio::spawn(async move {
            match worker.send_action(dispatch).await {
                Ok(metrics) => {
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
    db: Arc<Database>,
}

impl WorkCompletionHandler {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Process a completed action and determine next steps.
    ///
    /// This is designed to be called from a tokio task for parallelization.
    pub async fn process_completion(
        &self,
        in_flight: InFlightAction,
        metrics: RoundTripMetrics,
        dag: &DAG,
        instance_context: &mut InstanceContext,
    ) -> RunnerResult<CompletionBatch> {
        let mut batch = CompletionBatch::new();

        // Create completion record
        let completion = CompletionRecord {
            action_id: ActionId(in_flight.action.id),
            success: metrics.success,
            result_payload: metrics.response_payload.clone(),
            delivery_token: in_flight.action.delivery_token,
            error_message: metrics.error_message.clone(),
        };
        batch.completions.push(completion);

        if !metrics.success {
            // Handle failure - may need to route to exception handler
            // For now, just record the failure
            return Ok(batch);
        }

        // Parse result payload
        let result: JsonValue = if metrics.response_payload.is_empty() {
            JsonValue::Null
        } else {
            serde_json::from_slice(&metrics.response_payload)?
        };

        // Find the node that was executed
        let node_id = in_flight.action.node_id.as_deref();
        if node_id.is_none() {
            // No node tracking - just complete
            return Ok(batch);
        }
        let node_id = node_id.unwrap();

        // Get DAG helper for traversal
        let helper = DAGHelper::new(dag);

        // Store result in context
        if let Some(node) = dag.nodes.get(node_id) {
            // Determine target variable from node type
            if let Some(target_var) = self.get_target_variable(node) {
                instance_context
                    .variables
                    .insert(target_var, result.clone());
            }
        }

        // Find data flow targets and push values
        let targets = helper.get_data_flow_targets(node_id);
        for target in targets {
            if let Some(var) = &target.variable {
                instance_context
                    .variables
                    .insert(var.clone(), result.clone());
            }
        }

        // Get successors and determine what can be inlined vs queued
        let successors = helper.get_ready_successors(node_id, None);

        for successor in successors {
            let succ_node = match dag.nodes.get(&successor.node_id) {
                Some(n) => n,
                None => continue,
            };

            let exec_mode = helper.get_execution_mode(succ_node);
            match exec_mode {
                ExecutionMode::Inline => {
                    // Execute inline node immediately
                    let inline_result = self.execute_inline_node(succ_node, instance_context)?;

                    // Recursively process inline successors
                    // Note: In production, we'd want to limit recursion depth
                    let inline_batch = self.process_inline_completion(
                        &successor.node_id,
                        inline_result,
                        dag,
                        instance_context,
                    )?;

                    // Merge batches
                    batch.new_actions.extend(inline_batch.new_actions);
                }
                ExecutionMode::Delegated => {
                    // Create new action for delegated execution
                    if let Some(new_action) = self.create_action_for_node(
                        succ_node,
                        WorkflowInstanceId(in_flight.action.instance_id),
                        instance_context,
                    )? {
                        batch.new_actions.push(new_action);
                    }
                }
            }
        }

        Ok(batch)
    }

    /// Process completion of an inline node (recursive helper).
    fn process_inline_completion(
        &self,
        node_id: &str,
        result: JsonValue,
        dag: &DAG,
        instance_context: &mut InstanceContext,
    ) -> RunnerResult<CompletionBatch> {
        let mut batch = CompletionBatch::new();
        let helper = DAGHelper::new(dag);

        // Store result
        if let Some(node) = dag.nodes.get(node_id)
            && let Some(target_var) = self.get_target_variable(node)
        {
            instance_context
                .variables
                .insert(target_var, result.clone());
        }

        // Get successors
        let successors = helper.get_ready_successors(node_id, None);

        for successor in successors {
            let succ_node = match dag.nodes.get(&successor.node_id) {
                Some(n) => n,
                None => continue,
            };

            let exec_mode = helper.get_execution_mode(succ_node);
            match exec_mode {
                ExecutionMode::Inline => {
                    let inline_result = self.execute_inline_node(succ_node, instance_context)?;
                    let inner_batch = self.process_inline_completion(
                        &successor.node_id,
                        inline_result,
                        dag,
                        instance_context,
                    )?;
                    batch.new_actions.extend(inner_batch.new_actions);
                }
                ExecutionMode::Delegated => {
                    if let Some(new_action) = self.create_action_for_node(
                        succ_node,
                        instance_context.instance_id,
                        instance_context,
                    )? {
                        batch.new_actions.push(new_action);
                    }
                }
            }
        }

        Ok(batch)
    }

    /// Execute an inline node (assignment, expression, etc.).
    fn execute_inline_node(
        &self,
        node: &DAGNode,
        _context: &mut InstanceContext,
    ) -> RunnerResult<JsonValue> {
        // Dispatch based on node type
        match node.node_type.as_str() {
            "assignment" => {
                // We need the AST to evaluate - for now return null
                // In full implementation, we'd store AST reference in node
                Ok(JsonValue::Null)
            }
            "input" | "output" => {
                // Boundary nodes just pass through
                Ok(JsonValue::Null)
            }
            "return" => {
                // Return passes through current scope
                Ok(JsonValue::Null)
            }
            "conditional" => {
                // Conditional - need to evaluate condition
                // Return condition result for branching
                Ok(JsonValue::Bool(true))
            }
            "aggregator" => {
                // Collect spread/parallel results
                Ok(JsonValue::Array(vec![]))
            }
            _ => Ok(JsonValue::Null),
        }
    }

    /// Get the target variable name for a node (if it assigns to one).
    fn get_target_variable(&self, node: &DAGNode) -> Option<String> {
        // Extract from node label or type
        // Format is typically "target = ..." for assignments
        if node.node_type == "assignment" {
            // Parse from label
            if let Some(eq_pos) = node.label.find('=') {
                let target = node.label[..eq_pos].trim();
                return Some(target.to_string());
            }
        }
        None
    }

    /// Create a NewAction for a delegated node.
    fn create_action_for_node(
        &self,
        node: &DAGNode,
        instance_id: WorkflowInstanceId,
        context: &InstanceContext,
    ) -> RunnerResult<Option<NewAction>> {
        // Only create actions for action_call nodes
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

        // Build dispatch payload from context
        let payload = serde_json::to_vec(&context.variables)?;

        Ok(Some(NewAction {
            instance_id,
            module_name,
            action_name,
            dispatch_payload: payload,
            timeout_seconds: 300, // Default 5 minutes
            max_retries: 3,
            backoff_kind: BackoffKind::Exponential,
            backoff_base_delay_ms: 1000,
            node_id: Some(node.id.clone()),
        }))
    }

    /// Write a completion batch to the database in a single transaction.
    pub async fn write_batch(&self, batch: CompletionBatch) -> RunnerResult<()> {
        // Complete actions
        for completion in batch.completions {
            self.db.complete_action(completion).await?;
        }

        // Enqueue new actions
        for new_action in batch.new_actions {
            self.db.enqueue_action(new_action).await?;
        }

        // TODO: Update contexts in DB

        Ok(())
    }
}

// ============================================================================
// Instance Context
// ============================================================================

/// Execution context for a workflow instance.
///
/// Tracks variables and state during execution.
#[derive(Debug, Clone)]
pub struct InstanceContext {
    /// Instance ID
    pub instance_id: WorkflowInstanceId,
    /// Current variable values
    pub variables: HashMap<String, JsonValue>,
    /// Exception info if in error state
    pub exception: Option<ExceptionInfo>,
}

impl InstanceContext {
    pub fn new(instance_id: WorkflowInstanceId) -> Self {
        Self {
            instance_id,
            variables: HashMap::new(),
            exception: None,
        }
    }

    pub fn with_inputs(mut self, inputs: HashMap<String, JsonValue>) -> Self {
        self.variables = inputs;
        self
    }
}

/// Exception information for error handling.
#[derive(Debug, Clone)]
pub struct ExceptionInfo {
    pub exception_type: String,
    pub message: String,
    pub node_id: String,
}

// ============================================================================
// Expression Evaluator
// ============================================================================

/// Evaluates AST expressions in a given context.
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Evaluate an expression to a runtime value.
    pub fn evaluate(expr: &ast::Expr, context: &InstanceContext) -> RunnerResult<JsonValue> {
        let kind = expr
            .kind
            .as_ref()
            .ok_or_else(|| RunnerError::Evaluation("Empty expression".to_string()))?;

        match kind {
            ast::expr::Kind::Literal(lit) => Self::eval_literal(lit),
            ast::expr::Kind::Variable(var) => Self::eval_variable(var, context),
            ast::expr::Kind::BinaryOp(op) => Self::eval_binary_op(op, context),
            ast::expr::Kind::UnaryOp(op) => Self::eval_unary_op(op, context),
            ast::expr::Kind::List(list) => Self::eval_list(list, context),
            ast::expr::Kind::Dict(dict) => Self::eval_dict(dict, context),
            ast::expr::Kind::Index(idx) => Self::eval_index(idx, context),
            ast::expr::Kind::Dot(dot) => Self::eval_dot(dot, context),
            ast::expr::Kind::FunctionCall(call) => Self::eval_function_call(call, context),
            ast::expr::Kind::ActionCall(_) => Err(RunnerError::Evaluation(
                "Action calls cannot be evaluated inline".to_string(),
            )),
        }
    }

    fn eval_literal(lit: &ast::Literal) -> RunnerResult<JsonValue> {
        let value = lit
            .value
            .as_ref()
            .ok_or_else(|| RunnerError::Evaluation("Empty literal".to_string()))?;

        Ok(match value {
            ast::literal::Value::IntValue(i) => JsonValue::Number((*i).into()),
            ast::literal::Value::FloatValue(f) => JsonValue::Number(
                serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0)),
            ),
            ast::literal::Value::StringValue(s) => JsonValue::String(s.clone()),
            ast::literal::Value::BoolValue(b) => JsonValue::Bool(*b),
            ast::literal::Value::IsNone(true) => JsonValue::Null,
            ast::literal::Value::IsNone(false) => JsonValue::Null,
        })
    }

    fn eval_variable(var: &ast::Variable, context: &InstanceContext) -> RunnerResult<JsonValue> {
        context
            .variables
            .get(&var.name)
            .cloned()
            .ok_or_else(|| RunnerError::VariableNotFound(var.name.clone()))
    }

    fn eval_binary_op(op: &ast::BinaryOp, context: &InstanceContext) -> RunnerResult<JsonValue> {
        let left_expr = op
            .left
            .as_ref()
            .ok_or_else(|| RunnerError::Evaluation("Missing left operand".to_string()))?;
        let right_expr = op
            .right
            .as_ref()
            .ok_or_else(|| RunnerError::Evaluation("Missing right operand".to_string()))?;

        let left = Self::evaluate(left_expr, context)?;
        let right = Self::evaluate(right_expr, context)?;

        match ast::BinaryOperator::try_from(op.op)
            .unwrap_or(ast::BinaryOperator::BinaryOpUnspecified)
        {
            ast::BinaryOperator::BinaryOpAdd => Self::apply_add(&left, &right),
            ast::BinaryOperator::BinaryOpSub => Self::apply_sub(&left, &right),
            ast::BinaryOperator::BinaryOpMul => Self::apply_mul(&left, &right),
            ast::BinaryOperator::BinaryOpDiv => Self::apply_div(&left, &right),
            ast::BinaryOperator::BinaryOpEq => Ok(JsonValue::Bool(left == right)),
            ast::BinaryOperator::BinaryOpNe => Ok(JsonValue::Bool(left != right)),
            ast::BinaryOperator::BinaryOpLt => Self::apply_lt(&left, &right),
            ast::BinaryOperator::BinaryOpLe => Self::apply_le(&left, &right),
            ast::BinaryOperator::BinaryOpGt => Self::apply_gt(&left, &right),
            ast::BinaryOperator::BinaryOpGe => Self::apply_ge(&left, &right),
            ast::BinaryOperator::BinaryOpAnd => Self::apply_and(&left, &right),
            ast::BinaryOperator::BinaryOpOr => Self::apply_or(&left, &right),
            ast::BinaryOperator::BinaryOpIn => Self::apply_in(&left, &right),
            ast::BinaryOperator::BinaryOpNotIn => {
                let result = Self::apply_in(&left, &right)?;
                Ok(JsonValue::Bool(!result.as_bool().unwrap_or(false)))
            }
            _ => Err(RunnerError::Evaluation(
                "Unknown binary operator".to_string(),
            )),
        }
    }

    fn eval_unary_op(op: &ast::UnaryOp, context: &InstanceContext) -> RunnerResult<JsonValue> {
        let operand_expr = op
            .operand
            .as_ref()
            .ok_or_else(|| RunnerError::Evaluation("Missing operand".to_string()))?;

        let operand = Self::evaluate(operand_expr, context)?;

        match ast::UnaryOperator::try_from(op.op).unwrap_or(ast::UnaryOperator::UnaryOpUnspecified)
        {
            ast::UnaryOperator::UnaryOpNeg => match &operand {
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(JsonValue::Number((-i).into()))
                    } else if let Some(f) = n.as_f64() {
                        Ok(JsonValue::Number(
                            serde_json::Number::from_f64(-f)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ))
                    } else {
                        Err(RunnerError::Evaluation(
                            "Cannot negate non-numeric value".to_string(),
                        ))
                    }
                }
                _ => Err(RunnerError::Evaluation(
                    "Cannot negate non-numeric value".to_string(),
                )),
            },
            ast::UnaryOperator::UnaryOpNot => Ok(JsonValue::Bool(!Self::is_truthy(&operand))),
            _ => Err(RunnerError::Evaluation(
                "Unknown unary operator".to_string(),
            )),
        }
    }

    fn eval_list(list: &ast::ListExpr, context: &InstanceContext) -> RunnerResult<JsonValue> {
        let elements: Result<Vec<JsonValue>, _> = list
            .elements
            .iter()
            .map(|e| Self::evaluate(e, context))
            .collect();
        Ok(JsonValue::Array(elements?))
    }

    fn eval_dict(dict: &ast::DictExpr, context: &InstanceContext) -> RunnerResult<JsonValue> {
        let mut map = serde_json::Map::new();
        for entry in &dict.entries {
            let key_expr = entry
                .key
                .as_ref()
                .ok_or_else(|| RunnerError::Evaluation("Missing dict key".to_string()))?;
            let val_expr = entry
                .value
                .as_ref()
                .ok_or_else(|| RunnerError::Evaluation("Missing dict value".to_string()))?;

            let key = Self::evaluate(key_expr, context)?;
            let val = Self::evaluate(val_expr, context)?;

            let key_str = match key {
                JsonValue::String(s) => s,
                other => other.to_string(),
            };
            map.insert(key_str, val);
        }
        Ok(JsonValue::Object(map))
    }

    fn eval_index(idx: &ast::IndexAccess, context: &InstanceContext) -> RunnerResult<JsonValue> {
        let obj_expr = idx
            .object
            .as_ref()
            .ok_or_else(|| RunnerError::Evaluation("Missing index object".to_string()))?;
        let idx_expr = idx
            .index
            .as_ref()
            .ok_or_else(|| RunnerError::Evaluation("Missing index".to_string()))?;

        let obj = Self::evaluate(obj_expr, context)?;
        let index = Self::evaluate(idx_expr, context)?;

        match (&obj, &index) {
            (JsonValue::Array(arr), JsonValue::Number(n)) => {
                let i = n.as_i64().unwrap_or(0) as usize;
                arr.get(i)
                    .cloned()
                    .ok_or_else(|| RunnerError::Evaluation(format!("Index {} out of bounds", i)))
            }
            (JsonValue::Object(map), JsonValue::String(key)) => map
                .get(key)
                .cloned()
                .ok_or_else(|| RunnerError::Evaluation(format!("Key '{}' not found", key))),
            (JsonValue::String(s), JsonValue::Number(n)) => {
                let i = n.as_i64().unwrap_or(0) as usize;
                s.chars()
                    .nth(i)
                    .map(|c| JsonValue::String(c.to_string()))
                    .ok_or_else(|| RunnerError::Evaluation(format!("Index {} out of bounds", i)))
            }
            _ => Err(RunnerError::Evaluation(
                "Invalid index operation".to_string(),
            )),
        }
    }

    fn eval_dot(dot: &ast::DotAccess, context: &InstanceContext) -> RunnerResult<JsonValue> {
        let obj_expr = dot
            .object
            .as_ref()
            .ok_or_else(|| RunnerError::Evaluation("Missing dot object".to_string()))?;

        let obj = Self::evaluate(obj_expr, context)?;

        match &obj {
            JsonValue::Object(map) => map.get(&dot.attribute).cloned().ok_or_else(|| {
                RunnerError::Evaluation(format!("Attribute '{}' not found", dot.attribute))
            }),
            _ => Err(RunnerError::Evaluation(
                "Dot access on non-object".to_string(),
            )),
        }
    }

    fn eval_function_call(
        call: &ast::FunctionCall,
        context: &InstanceContext,
    ) -> RunnerResult<JsonValue> {
        // Evaluate kwargs
        let mut kwargs = HashMap::new();
        for kwarg in &call.kwargs {
            let val_expr = kwarg
                .value
                .as_ref()
                .ok_or_else(|| RunnerError::Evaluation("Missing kwarg value".to_string()))?;
            let val = Self::evaluate(val_expr, context)?;
            kwargs.insert(kwarg.name.clone(), val);
        }

        // Built-in functions
        match call.name.as_str() {
            "range" => Self::builtin_range(&kwargs),
            "len" => Self::builtin_len(&kwargs),
            "enumerate" => Self::builtin_enumerate(&kwargs),
            _ => Err(RunnerError::HandlerNotFound(call.name.clone())),
        }
    }

    // Helper methods for operators
    fn apply_add(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                    Ok(JsonValue::Number((ai + bi).into()))
                } else if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(af + bf)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    Err(RunnerError::Evaluation(
                        "Cannot add incompatible numbers".to_string(),
                    ))
                }
            }
            (JsonValue::String(a), JsonValue::String(b)) => {
                Ok(JsonValue::String(format!("{}{}", a, b)))
            }
            (JsonValue::Array(a), JsonValue::Array(b)) => {
                let mut result = a.clone();
                result.extend(b.clone());
                Ok(JsonValue::Array(result))
            }
            _ => Err(RunnerError::Evaluation(
                "Cannot add incompatible types".to_string(),
            )),
        }
    }

    fn apply_sub(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                    Ok(JsonValue::Number((ai - bi).into()))
                } else if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(af - bf)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    Err(RunnerError::Evaluation(
                        "Cannot subtract incompatible numbers".to_string(),
                    ))
                }
            }
            _ => Err(RunnerError::Evaluation(
                "Cannot subtract non-numbers".to_string(),
            )),
        }
    }

    fn apply_mul(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                    Ok(JsonValue::Number((ai * bi).into()))
                } else if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(af * bf)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    Err(RunnerError::Evaluation(
                        "Cannot multiply incompatible numbers".to_string(),
                    ))
                }
            }
            _ => Err(RunnerError::Evaluation(
                "Cannot multiply non-numbers".to_string(),
            )),
        }
    }

    fn apply_div(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(1.0);
                if bf == 0.0 {
                    Err(RunnerError::Evaluation("Division by zero".to_string()))
                } else {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(af / bf)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                }
            }
            _ => Err(RunnerError::Evaluation(
                "Cannot divide non-numbers".to_string(),
            )),
        }
    }

    fn apply_lt(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(0.0);
                Ok(JsonValue::Bool(af < bf))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::Bool(a < b)),
            _ => Err(RunnerError::Evaluation(
                "Cannot compare incompatible types".to_string(),
            )),
        }
    }

    fn apply_le(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(0.0);
                Ok(JsonValue::Bool(af <= bf))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::Bool(a <= b)),
            _ => Err(RunnerError::Evaluation(
                "Cannot compare incompatible types".to_string(),
            )),
        }
    }

    fn apply_gt(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(0.0);
                Ok(JsonValue::Bool(af > bf))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::Bool(a > b)),
            _ => Err(RunnerError::Evaluation(
                "Cannot compare incompatible types".to_string(),
            )),
        }
    }

    fn apply_ge(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(0.0);
                Ok(JsonValue::Bool(af >= bf))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::Bool(a >= b)),
            _ => Err(RunnerError::Evaluation(
                "Cannot compare incompatible types".to_string(),
            )),
        }
    }

    fn apply_and(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        Ok(JsonValue::Bool(
            Self::is_truthy(left) && Self::is_truthy(right),
        ))
    }

    fn apply_or(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        Ok(JsonValue::Bool(
            Self::is_truthy(left) || Self::is_truthy(right),
        ))
    }

    fn apply_in(left: &JsonValue, right: &JsonValue) -> RunnerResult<JsonValue> {
        match right {
            JsonValue::Array(arr) => Ok(JsonValue::Bool(arr.contains(left))),
            JsonValue::Object(map) => {
                let key = match left {
                    JsonValue::String(s) => s.clone(),
                    other => other.to_string(),
                };
                Ok(JsonValue::Bool(map.contains_key(&key)))
            }
            JsonValue::String(s) => {
                let needle = match left {
                    JsonValue::String(n) => n.clone(),
                    other => other.to_string(),
                };
                Ok(JsonValue::Bool(s.contains(&needle)))
            }
            _ => Err(RunnerError::Evaluation(
                "'in' requires array, object, or string".to_string(),
            )),
        }
    }

    fn is_truthy(value: &JsonValue) -> bool {
        match value {
            JsonValue::Null => false,
            JsonValue::Bool(b) => *b,
            JsonValue::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
            JsonValue::String(s) => !s.is_empty(),
            JsonValue::Array(a) => !a.is_empty(),
            JsonValue::Object(o) => !o.is_empty(),
        }
    }

    // Built-in functions
    fn builtin_range(kwargs: &HashMap<String, JsonValue>) -> RunnerResult<JsonValue> {
        let start = kwargs.get("start").and_then(|v| v.as_i64()).unwrap_or(0);
        let stop = kwargs.get("stop").and_then(|v| v.as_i64()).ok_or_else(|| {
            RunnerError::Evaluation("range() requires 'stop' argument".to_string())
        })?;
        let step = kwargs.get("step").and_then(|v| v.as_i64()).unwrap_or(1);

        if step == 0 {
            return Err(RunnerError::Evaluation(
                "range() step cannot be zero".to_string(),
            ));
        }

        let mut result = Vec::new();
        let mut i = start;
        while (step > 0 && i < stop) || (step < 0 && i > stop) {
            result.push(JsonValue::Number(i.into()));
            i += step;
        }

        Ok(JsonValue::Array(result))
    }

    fn builtin_len(kwargs: &HashMap<String, JsonValue>) -> RunnerResult<JsonValue> {
        let items = kwargs.get("items").ok_or_else(|| {
            RunnerError::Evaluation("len() requires 'items' argument".to_string())
        })?;

        let len = match items {
            JsonValue::Array(a) => a.len(),
            JsonValue::Object(o) => o.len(),
            JsonValue::String(s) => s.len(),
            _ => {
                return Err(RunnerError::Evaluation(
                    "len() requires array, object, or string".to_string(),
                ));
            }
        };

        Ok(JsonValue::Number((len as i64).into()))
    }

    fn builtin_enumerate(kwargs: &HashMap<String, JsonValue>) -> RunnerResult<JsonValue> {
        let items = kwargs.get("items").ok_or_else(|| {
            RunnerError::Evaluation("enumerate() requires 'items' argument".to_string())
        })?;

        let arr = match items {
            JsonValue::Array(a) => a,
            _ => {
                return Err(RunnerError::Evaluation(
                    "enumerate() requires array".to_string(),
                ));
            }
        };

        let result: Vec<JsonValue> = arr
            .iter()
            .enumerate()
            .map(|(i, v)| JsonValue::Array(vec![JsonValue::Number((i as i64).into()), v.clone()]))
            .collect();

        Ok(JsonValue::Array(result))
    }
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
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_slots_per_worker: 10,
            poll_interval_ms: 100,
            timeout_check_interval_ms: 1000,
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
    /// Instance contexts by instance ID
    instance_contexts: Arc<RwLock<HashMap<Uuid, InstanceContext>>>,
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

        let work_handler = Arc::new(WorkQueueHandler::new(
            Arc::clone(&db),
            worker_pool,
            slot_tracker,
            in_flight,
        ));
        let completion_handler = WorkCompletionHandler::new(Arc::clone(&db));
        let dag_cache = Arc::new(DAGCache::new(db));

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
    pub async fn run(&self) -> RunnerResult<()> {
        info!("Starting DAG runner");

        // Channel for completion results
        let (completion_tx, mut completion_rx) =
            mpsc::channel::<(InFlightAction, RoundTripMetrics)>(1000);

        loop {
            tokio::select! {
                // Check for shutdown
                _ = self.shutdown.notified() => {
                    info!("Runner shutdown requested");
                    break;
                }

                // Process completions
                Some((in_flight, metrics)) = completion_rx.recv() => {
                    // Spawn tokio task for parallel processing
                    let handler = self.completion_handler.clone();
                    let dag_cache = Arc::clone(&self.dag_cache);
                    let instance_contexts = Arc::clone(&self.instance_contexts);
                    let instance_id = in_flight.action.instance_id;

                    tokio::spawn(async move {
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
                    });
                }

                // Fetch and dispatch work (delegated to WorkQueueHandler)
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(self.config.poll_interval_ms)) => {
                    if self.work_handler.available_slots() == 0 {
                        continue;
                    }

                    match self.work_handler.fetch_and_dispatch(
                        self.config.batch_size,
                        completion_tx.clone(),
                    ).await {
                        Ok(count) if count > 0 => {
                            debug!("Dispatched {} actions", count);
                        }
                        Ok(_) => {
                            // No work available
                        }
                        Err(e) => {
                            error!("Failed to fetch/dispatch actions: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a completion in a tokio task.
    ///
    /// The CPU-intensive DAG traversal and inline execution is offloaded to
    /// `spawn_blocking` for true CPU parallelism across threads.
    async fn process_completion_task(
        handler: WorkCompletionHandler,
        dag_cache: Arc<DAGCache>,
        instance_contexts: Arc<RwLock<HashMap<Uuid, InstanceContext>>>,
        in_flight: InFlightAction,
        metrics: RoundTripMetrics,
        instance_id: WorkflowInstanceId,
    ) -> RunnerResult<()> {
        // Step 1: Async I/O - Get or create instance context
        let context = {
            let contexts = instance_contexts.read().await;
            contexts.get(&instance_id.0).cloned()
        }
        .unwrap_or_else(|| InstanceContext::new(instance_id));

        // Step 2: Async I/O - Get DAG for this workflow instance (loads from DB if not cached)
        let dag = dag_cache.get_dag_for_instance(instance_id).await?;

        // Step 3: CPU-bound work - DAG traversal and inline execution
        // Offload to blocking thread pool for true CPU parallelism
        let (batch, updated_context) = if let Some(dag) = dag {
            let dag_clone = dag;
            let in_flight_clone = in_flight.clone();
            let metrics_clone = metrics.clone();

            tokio::task::spawn_blocking(move || {
                // All CPU-intensive work happens here on a blocking thread
                Self::process_completion_blocking(
                    in_flight_clone,
                    metrics_clone,
                    &dag_clone,
                    context,
                )
            })
            .await
            .map_err(|e| RunnerError::Worker(format!("Blocking task failed: {}", e)))??
        } else {
            // No DAG - just create completion record
            let mut batch = CompletionBatch::new();
            batch.completions.push(CompletionRecord {
                action_id: ActionId(in_flight.action.id),
                success: metrics.success,
                result_payload: metrics.response_payload,
                delivery_token: in_flight.action.delivery_token,
                error_message: metrics.error_message,
            });
            (batch, context)
        };

        // Step 4: Async I/O - Write batch to database
        handler.write_batch(batch).await?;

        // Step 5: Async I/O - Update context cache
        {
            let mut contexts = instance_contexts.write().await;
            contexts.insert(instance_id.0, updated_context);
        }

        Ok(())
    }

    /// Synchronous, CPU-bound completion processing.
    ///
    /// This runs on a blocking thread and performs:
    /// - DAG traversal to find successors
    /// - Expression evaluation for inline nodes
    /// - Recursive inline execution
    fn process_completion_blocking(
        in_flight: InFlightAction,
        metrics: RoundTripMetrics,
        dag: &DAG,
        mut context: InstanceContext,
    ) -> RunnerResult<(CompletionBatch, InstanceContext)> {
        let mut batch = CompletionBatch::new();

        // Create completion record
        batch.completions.push(CompletionRecord {
            action_id: ActionId(in_flight.action.id),
            success: metrics.success,
            result_payload: metrics.response_payload.clone(),
            delivery_token: in_flight.action.delivery_token,
            error_message: metrics.error_message.clone(),
        });

        if !metrics.success {
            return Ok((batch, context));
        }

        // Parse result payload
        let result: JsonValue = if metrics.response_payload.is_empty() {
            JsonValue::Null
        } else {
            serde_json::from_slice(&metrics.response_payload)?
        };

        // Find the node that was executed
        let node_id = match in_flight.action.node_id.as_deref() {
            Some(id) => id,
            None => return Ok((batch, context)),
        };

        let helper = DAGHelper::new(dag);

        // Store result in context
        if let Some(node) = dag.nodes.get(node_id)
            && let Some(target_var) = Self::get_target_variable_static(node)
        {
            context.variables.insert(target_var, result.clone());
        }

        // Find data flow targets and push values
        let targets = helper.get_data_flow_targets(node_id);
        for target in targets {
            if let Some(var) = &target.variable {
                context.variables.insert(var.clone(), result.clone());
            }
        }

        // Get successors and process inline nodes, queue delegated nodes
        let successors = helper.get_ready_successors(node_id, None);

        for successor in successors {
            let succ_node = match dag.nodes.get(&successor.node_id) {
                Some(n) => n,
                None => continue,
            };

            let exec_mode = helper.get_execution_mode(succ_node);
            match exec_mode {
                ExecutionMode::Inline => {
                    // Execute inline node and recursively process successors
                    let inline_result = Self::execute_inline_node_static(succ_node, &mut context)?;
                    Self::process_inline_successors(
                        &successor.node_id,
                        inline_result,
                        dag,
                        &mut context,
                        &mut batch,
                        in_flight.action.instance_id,
                    )?;
                }
                ExecutionMode::Delegated => {
                    if let Some(new_action) = Self::create_action_for_node_static(
                        succ_node,
                        WorkflowInstanceId(in_flight.action.instance_id),
                        &context,
                    )? {
                        batch.new_actions.push(new_action);
                    }
                }
            }
        }

        Ok((batch, context))
    }

    /// Recursively process inline successors (CPU-bound helper).
    fn process_inline_successors(
        node_id: &str,
        result: JsonValue,
        dag: &DAG,
        context: &mut InstanceContext,
        batch: &mut CompletionBatch,
        instance_id: Uuid,
    ) -> RunnerResult<()> {
        let helper = DAGHelper::new(dag);

        // Store result
        if let Some(node) = dag.nodes.get(node_id)
            && let Some(target_var) = Self::get_target_variable_static(node)
        {
            context.variables.insert(target_var, result.clone());
        }

        // Get successors
        let successors = helper.get_ready_successors(node_id, None);

        for successor in successors {
            let succ_node = match dag.nodes.get(&successor.node_id) {
                Some(n) => n,
                None => continue,
            };

            let exec_mode = helper.get_execution_mode(succ_node);
            match exec_mode {
                ExecutionMode::Inline => {
                    let inline_result = Self::execute_inline_node_static(succ_node, context)?;
                    Self::process_inline_successors(
                        &successor.node_id,
                        inline_result,
                        dag,
                        context,
                        batch,
                        instance_id,
                    )?;
                }
                ExecutionMode::Delegated => {
                    if let Some(new_action) = Self::create_action_for_node_static(
                        succ_node,
                        WorkflowInstanceId(instance_id),
                        context,
                    )? {
                        batch.new_actions.push(new_action);
                    }
                }
            }
        }

        Ok(())
    }

    /// Static helper to get target variable (for blocking context).
    fn get_target_variable_static(node: &DAGNode) -> Option<String> {
        if node.node_type == "assignment"
            && let Some(eq_pos) = node.label.find('=')
        {
            let target = node.label[..eq_pos].trim();
            return Some(target.to_string());
        }
        None
    }

    /// Static helper to execute inline node (for blocking context).
    fn execute_inline_node_static(
        node: &DAGNode,
        _context: &mut InstanceContext,
    ) -> RunnerResult<JsonValue> {
        match node.node_type.as_str() {
            "assignment" => Ok(JsonValue::Null),
            "input" | "output" => Ok(JsonValue::Null),
            "return" => Ok(JsonValue::Null),
            "conditional" => Ok(JsonValue::Bool(true)),
            "aggregator" => Ok(JsonValue::Array(vec![])),
            _ => Ok(JsonValue::Null),
        }
    }

    /// Static helper to create action for node (for blocking context).
    fn create_action_for_node_static(
        node: &DAGNode,
        instance_id: WorkflowInstanceId,
        context: &InstanceContext,
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

        let payload = serde_json::to_vec(&context.variables)?;

        Ok(Some(NewAction {
            instance_id,
            module_name,
            action_name,
            dispatch_payload: payload,
            timeout_seconds: 300,
            max_retries: 3,
            backoff_kind: BackoffKind::Exponential,
            backoff_base_delay_ms: 1000,
            node_id: Some(node.id.clone()),
        }))
    }

    /// Request shutdown.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Register a DAG for a workflow version (for testing or warm-up).
    pub async fn register_dag(&self, version_id: Uuid, dag: DAG) {
        self.dag_cache.preload(version_id, dag).await;
    }

    /// Get count of in-flight actions.
    pub async fn in_flight_count(&self) -> usize {
        self.work_handler.in_flight_count().await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

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
        };

        tracker.add(action.clone(), 0);
        assert_eq!(tracker.count(), 1);

        let removed = tracker.remove(&token);
        assert!(removed.is_some());
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn test_expression_evaluator_literal() {
        let context = InstanceContext::new(WorkflowInstanceId::new());

        // Integer
        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::IntValue(42)),
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(result, JsonValue::Number(42.into()));

        // String
        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::StringValue("hello".to_string())),
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(result, JsonValue::String("hello".to_string()));

        // Boolean
        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::BoolValue(true)),
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_expression_evaluator_variable() {
        let mut context = InstanceContext::new(WorkflowInstanceId::new());
        context
            .variables
            .insert("x".to_string(), JsonValue::Number(10.into()));

        let expr = ast::Expr {
            kind: Some(ast::expr::Kind::Variable(ast::Variable {
                name: "x".to_string(),
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(result, JsonValue::Number(10.into()));
    }

    #[test]
    fn test_expression_evaluator_binary_add() {
        let context = InstanceContext::new(WorkflowInstanceId::new());

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
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(result, JsonValue::Number(10.into()));
    }

    #[test]
    fn test_expression_evaluator_comparison() {
        let context = InstanceContext::new(WorkflowInstanceId::new());

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
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_expression_evaluator_list() {
        let context = InstanceContext::new(WorkflowInstanceId::new());

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
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_range() {
        let context = InstanceContext::new(WorkflowInstanceId::new());

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
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Number(0.into()),
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into()),
                JsonValue::Number(4.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_len() {
        let context = InstanceContext::new(WorkflowInstanceId::new());

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
            })),
            span: None,
        };
        let result = ExpressionEvaluator::evaluate(&expr, &context).unwrap();
        assert_eq!(result, JsonValue::Number(3.into()));
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
}
