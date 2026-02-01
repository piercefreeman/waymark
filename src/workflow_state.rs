//! Workflow state management for instance-local execution.
//!
//! The workflow state is the in-memory source of truth for an instance. It tracks
//! runtime executions (by execution_id) and the metadata needed to queue work.
//! Heavy inputs/results are stored separately in node_payloads.
//!
//! Key concepts:
//! - **Reference DAG**: Immutable workflow definition (from workflow_versions)
//! - **Workflow State**: Mutable execution metadata (this module)
//! - **Runtime Executions**: Expanded nodes per loop/spread iteration

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use chrono::Utc;
use prost::Message;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::ast_evaluator::{ExpressionEvaluator, Scope};
use crate::dag::{DAG, DAGEdge, DAGNode, EXCEPTION_SCOPE_VAR, EdgeType};
use crate::db::WorkflowInstanceId;

/// Maximum number of loop iterations before terminating execution.
/// This prevents infinite loops (e.g., `while True:`) from running forever.
pub const MAX_LOOP_ITERATIONS: i32 = 50_000;
use crate::dag_runtime::DAGHelper;
use crate::messages::execution::{
    AttemptRecord, BackoffConfig, BackoffKind, ExceptionInfo, ExecutionGraph, ExecutionNode,
    NodeKind, NodeStatus,
};
use crate::messages::proto::WorkflowArguments;
use crate::messages::{MessageError, decode_message, encode_message};
use crate::parser::ast;
use crate::value::{WorkflowValue, workflow_value_from_proto_bytes, workflow_value_to_proto_bytes};

/// A tuple representing a node's payload data: (execution_id, inputs, result)
pub type PayloadTuple = (String, Option<Vec<u8>>, Option<Vec<u8>>);
type PayloadRef<'a> = (&'a Option<Vec<u8>>, &'a Option<Vec<u8>>);
type PayloadMap<'a> = HashMap<&'a str, PayloadRef<'a>>;

/// Result of applying a batch of completions
#[derive(Debug, Default)]
pub struct BatchCompletionResult {
    /// Nodes that became ready after applying completions
    pub newly_ready: Vec<String>,
    /// Whether the workflow completed (output node reached)
    pub workflow_completed: bool,
    /// Result payload if workflow completed
    pub result_payload: Option<Vec<u8>>,
    /// Whether the workflow failed
    pub workflow_failed: bool,
    /// Error message if workflow failed
    pub error_message: Option<String>,
}

/// A completion to apply to the execution graph
#[derive(Debug, Clone)]
pub struct Completion {
    pub node_id: String,
    pub success: bool,
    pub result: Option<Vec<u8>>,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub worker_id: String,
    /// Total round-trip time from dispatch to result (for metrics)
    pub duration_ms: i64,
    /// Actual time spent executing on the worker (for accurate started_at calculation)
    pub worker_duration_ms: Option<i64>,
}

/// Wrapper around ExecutionGraph with high-level operations
#[derive(Debug, Clone)]
pub struct ExecutionState {
    pub graph: ExecutionGraph,
}

/// Runtime workflow state (public minimal API).
#[derive(Debug, Clone)]
pub struct WorkflowState {
    instance_id: WorkflowInstanceId,
    dag: Arc<DAG>,
    inner: ExecutionState,
    updated_executions: HashMap<String, ActionExecutionUpdate>,
}

#[derive(Debug, Clone)]
pub struct InstanceRehydrate {
    pub instance_id: WorkflowInstanceId,
    pub workflow_name: String,
    pub input_payload: Option<Vec<u8>>,
    pub execution_graph: Option<Vec<u8>>,
    pub dag: Arc<DAG>,
}

#[derive(Debug, Clone)]
pub struct RehydrateResult {
    pub state: WorkflowState,
    pub update: WorkflowStateUpdate,
}

#[derive(Debug, Clone)]
pub struct ActionExecutionMetadata {
    pub node_id: String,
    pub action_id: String,
    pub execution_id: Option<String>,
    pub status: NodeStatus,
    pub attempt_number: i32,
    pub max_retries: i32,
    pub worker_id: Option<String>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub duration_ms: Option<i64>,
    pub parent_execution_id: Option<String>,
    pub spread_index: Option<i32>,
    pub loop_index: Option<i32>,
    pub waiting_for: Vec<String>,
    pub completed_count: i32,
    pub node_kind: NodeKind,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub timeout_seconds: i32,
    pub timeout_retry_limit: i32,
    pub backoff: BackoffConfig,
    pub sleep_wakeup_time_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ActionExecutionUpdate {
    pub instance_id: WorkflowInstanceId,
    pub node_id: String,
    pub action_id: String,
    pub execution_id: Option<String>,
    pub status: NodeStatus,
    pub attempt_number: i32,
    pub max_retries: i32,
    pub worker_id: Option<String>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub duration_ms: Option<i64>,
    pub parent_execution_id: Option<String>,
    pub spread_index: Option<i32>,
    pub loop_index: Option<i32>,
    pub waiting_for: Vec<String>,
    pub completed_count: i32,
    pub node_kind: NodeKind,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub timeout_seconds: i32,
    pub timeout_retry_limit: i32,
    pub backoff: BackoffConfig,
    pub sleep_wakeup_time_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ActionNodeArchive {
    pub node_id: String,
    pub action_id: String,
    pub execution_id: String,
    pub status: NodeStatus,
    pub attempt_number: i32,
    pub max_retries: i32,
    pub worker_id: Option<String>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub duration_ms: Option<i64>,
    pub parent_execution_id: Option<String>,
    pub spread_index: Option<i32>,
    pub loop_index: Option<i32>,
    pub waiting_for: Vec<String>,
    pub completed_count: i32,
    pub node_kind: NodeKind,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub timeout_seconds: i32,
    pub timeout_retry_limit: i32,
    pub backoff: BackoffConfig,
    pub inputs: Option<Vec<u8>>,
    pub result: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct ReadyAction {
    pub node_id: String,
    pub action_id: String,
    pub node_kind: NodeKind,
    pub module_name: Option<String>,
    pub action_name: Option<String>,
    pub inputs: Option<Vec<u8>>,
    pub timeout_seconds: u32,
    pub max_retries: u32,
    pub attempt_number: u32,
}

#[derive(Debug, Clone)]
pub struct ActionPayload {
    pub execution_id: String,
    pub action_id: String,
    pub inputs: Option<Vec<u8>>,
    pub result: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowStateUpdate {
    pub ready_actions: Vec<ReadyAction>,
    pub payloads: Vec<ActionPayload>,
    pub workflow_completed: bool,
    pub workflow_failed: bool,
    pub result_payload: Option<Vec<u8>>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkflowStateSnapshot {
    pub instance_id: WorkflowInstanceId,
    pub ready_queue: Vec<String>,
    pub next_wakeup_time: Option<i64>,
    pub executions: HashMap<String, ActionExecutionMetadata>,
    pub encoded_graph: Vec<u8>,
    pub workflow_completed: bool,
    pub workflow_failed: bool,
    pub result_payload: Option<Vec<u8>>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ActionUpdate {
    Started {
        node_id: String,
        worker_id: String,
        inputs: Option<Vec<u8>>,
    },
    Completed(Completion),
}

#[derive(Debug, thiserror::Error)]
pub enum WorkflowStateError {
    #[error("Serialization error: {0}")]
    Serialization(#[from] prost::DecodeError),
    #[error("Message error: {0}")]
    Message(#[from] MessageError),
}

/// Worker identifier used for durable sleep nodes.
pub const SLEEP_WORKER_ID: &str = "sleep";
const SCOPE_DELIM: &str = "::";
const EXECUTION_GRAPH_MAGIC: [u8; 4] = *b"RAPZ";
const EXECUTION_GRAPH_VERSION: u8 = 1;
const EXECUTION_GRAPH_CODEC_ZSTD: u8 = 1;
const EXECUTION_GRAPH_HEADER_LEN: usize = 4 + 1 + 1 + 8;
const EXECUTION_GRAPH_ZSTD_LEVEL: i32 = 3;

/// Determine the NodeKind for a DAG node based on its properties.
/// This is the source of truth for node type classification.
fn determine_node_kind(dag_node: &DAGNode) -> NodeKind {
    // Sleep actions are special - check first
    if dag_node.action_name.as_deref() == Some("sleep") {
        return NodeKind::Sleep;
    }
    // Aggregator nodes collect spread results
    if dag_node.is_aggregator {
        return NodeKind::Aggregator;
    }
    // Spread nodes are templates for parallel expansion
    if dag_node.is_spread {
        return NodeKind::Spread;
    }
    // Map node_type string to NodeKind enum
    match dag_node.node_type.as_str() {
        "action_call" => NodeKind::Action,
        "assignment" => NodeKind::Assignment,
        "branch" => NodeKind::Branch,
        "join" => NodeKind::Join,
        "fn_call" => NodeKind::FnCall,
        "return" => NodeKind::Return,
        "break" => NodeKind::Break,
        "for_loop" => NodeKind::ForLoop,
        "input" => NodeKind::Input,
        "output" => NodeKind::Output,
        _ => NodeKind::Unspecified,
    }
}

fn encode_execution_graph_bytes(raw: &[u8]) -> Vec<u8> {
    let compressed = match zstd::bulk::compress(raw, EXECUTION_GRAPH_ZSTD_LEVEL) {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!(
                ?err,
                "failed to compress execution graph; storing uncompressed"
            );
            return raw.to_vec();
        }
    };

    if compressed.len() + EXECUTION_GRAPH_HEADER_LEN >= raw.len() {
        return raw.to_vec();
    }

    let mut out = Vec::with_capacity(EXECUTION_GRAPH_HEADER_LEN + compressed.len());
    out.extend_from_slice(&EXECUTION_GRAPH_MAGIC);
    out.push(EXECUTION_GRAPH_VERSION);
    out.push(EXECUTION_GRAPH_CODEC_ZSTD);
    out.extend_from_slice(&(raw.len() as u64).to_le_bytes());
    out.extend_from_slice(&compressed);
    out
}

fn decode_execution_graph_bytes(bytes: &[u8]) -> Result<Vec<u8>, prost::DecodeError> {
    if bytes.len() < EXECUTION_GRAPH_HEADER_LEN || !bytes.starts_with(&EXECUTION_GRAPH_MAGIC) {
        return Ok(bytes.to_vec());
    }

    let version = bytes[EXECUTION_GRAPH_MAGIC.len()];
    let codec = bytes[EXECUTION_GRAPH_MAGIC.len() + 1];
    if version != EXECUTION_GRAPH_VERSION || codec != EXECUTION_GRAPH_CODEC_ZSTD {
        warn!(version, codec, "unknown execution graph compression header");
        return Err(prost::DecodeError::new(
            "unknown execution graph compression header",
        ));
    }

    let len_start = EXECUTION_GRAPH_MAGIC.len() + 2;
    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&bytes[len_start..len_start + 8]);
    let raw_len = u64::from_le_bytes(len_bytes);
    let raw_len = usize::try_from(raw_len)
        .map_err(|_| prost::DecodeError::new("execution graph length overflow"))?;
    let compressed = &bytes[EXECUTION_GRAPH_HEADER_LEN..];

    match zstd::bulk::decompress(compressed, raw_len) {
        Ok(decoded) => Ok(decoded),
        Err(err) => {
            warn!(?err, "failed to decompress execution graph");
            Err(prost::DecodeError::new(
                "failed to decompress execution graph",
            ))
        }
    }
}

impl ExecutionState {
    /// Create a new execution state from protobuf bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        let decoded = decode_execution_graph_bytes(bytes)?;
        let graph = ExecutionGraph::decode(&*decoded)?;
        Ok(Self { graph })
    }

    /// Serialize the execution state to protobuf bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let raw = self.graph.encode_to_vec();
        encode_execution_graph_bytes(&raw)
    }

    /// Serialize with payloads + variable data stripped (metadata only).
    ///
    /// WARNING: This strips inputs/results/variables/exceptions. Use only when
    /// payloads are persisted separately and variables will be rehydrated.
    pub fn to_bytes_fully_stripped(&self) -> Vec<u8> {
        let mut stripped = self.graph.clone();

        stripped.variables.clear();
        stripped.exceptions.clear();

        for node in stripped.nodes.values_mut() {
            node.inputs = None;
            node.result = None;
            node.loop_accumulators = None;

            for attempt in &mut node.attempts {
                attempt.result = None;
            }
        }

        let raw = stripped.encode_to_vec();
        encode_execution_graph_bytes(&raw)
    }

    /// Hydrate a stripped execution state with payloads loaded from storage.
    ///
    /// This is used during cold-start recovery to reconstitute the full
    /// in-memory state from a stripped graph + separately stored payloads.
    ///
    /// Each payload tuple is (execution_id, inputs, result).
    pub fn hydrate_payloads(&mut self, payloads: &[PayloadTuple]) {
        // Build a map from execution_id to payload for efficient lookup
        let payload_map: PayloadMap<'_> = payloads
            .iter()
            .map(|(exec_id, inputs, result)| (exec_id.as_str(), (inputs, result)))
            .collect();

        // Iterate through all nodes and match by execution_id
        for node in self.graph.nodes.values_mut() {
            if let Some(exec_id) = &node.execution_id
                && let Some((inputs, result)) = payload_map.get(exec_id.as_str())
            {
                if inputs.is_some() {
                    node.inputs = (*inputs).clone();
                }
                if result.is_some() {
                    node.result = (*result).clone();
                }
            }
        }
    }

    /// Rebuild variable bindings from completed node results + initial inputs.
    ///
    /// This is required when the persisted graph is metadata-only.
    pub fn rehydrate_variables(&mut self, dag: &DAG, inputs: &WorkflowArguments) {
        self.graph.variables.clear();

        for arg in &inputs.arguments {
            if let Some(value) = &arg.value {
                let encoded = encode_message(value);
                self.graph.variables.insert(arg.key.clone(), encoded);
            }
        }

        struct RehydrateEntry {
            node_id: String,
            template_id: String,
            targets: Vec<String>,
            result: Vec<u8>,
            completed_at_ms: i64,
        }

        let mut completed_nodes: Vec<RehydrateEntry> = self
            .graph
            .nodes
            .iter()
            .filter_map(|(node_id, node)| {
                let status = NodeStatus::try_from(node.status).unwrap_or(NodeStatus::Unspecified);
                if !matches!(status, NodeStatus::Completed | NodeStatus::Caught) {
                    return None;
                }
                let result = node.result.as_ref()?.clone();
                let mut targets = node.targets.clone();
                if targets.is_empty()
                    && let Some(dag_node) = dag.nodes.get(&node.template_id)
                {
                    if let Some(dag_targets) = dag_node.targets.clone() {
                        targets = dag_targets;
                    } else if let Some(target) = dag_node.target.clone() {
                        targets = vec![target];
                    }
                }
                if targets.is_empty() {
                    return None;
                }
                Some(RehydrateEntry {
                    node_id: node_id.clone(),
                    template_id: node.template_id.clone(),
                    targets,
                    result,
                    completed_at_ms: node.completed_at_ms.unwrap_or(0),
                })
            })
            .collect();

        completed_nodes.sort_by_key(|entry| entry.completed_at_ms);

        for entry in completed_nodes {
            let node_type = dag
                .nodes
                .get(&entry.template_id)
                .map(|n| n.node_type.clone())
                .unwrap_or_default();

            if entry.result.is_empty() {
                continue;
            }
            let Some(workflow_value) = crate::inline_executor::extract_result_value(&entry.result)
            else {
                warn!(
                    node_id = %entry.node_id,
                    "Failed to decode result value; skipping variable storage"
                );
                continue;
            };

            let targets = entry.targets.clone();
            if targets.len() > 1 {
                match &workflow_value {
                    WorkflowValue::Tuple(items) | WorkflowValue::List(items) => {
                        for (target, item) in targets.iter().zip(items.iter()) {
                            self.store_variable_for_node(&entry.node_id, &node_type, target, item);
                        }
                    }
                    _ => {
                        warn!(
                            node_id = %entry.node_id,
                            targets = ?targets,
                            "Value is not iterable for tuple unpacking"
                        );
                        for target in &targets {
                            self.store_variable_for_node(
                                &entry.node_id,
                                &node_type,
                                target,
                                &workflow_value,
                            );
                        }
                    }
                }
            } else {
                for target in &targets {
                    self.store_variable_for_node(
                        &entry.node_id,
                        &node_type,
                        target,
                        &workflow_value,
                    );
                }
            }
        }
    }

    /// Create an empty execution state
    pub fn new() -> Self {
        Self {
            graph: ExecutionGraph {
                nodes: HashMap::new(),
                variables: HashMap::new(),
                ready_queue: Vec::new(),
                exceptions: HashMap::new(),
                next_wakeup_time: None,
            },
        }
    }

    /// Initialize the execution graph from a DAG and initial inputs
    pub fn initialize_from_dag(&mut self, dag: &DAG, inputs: &WorkflowArguments) {
        let helper = DAGHelper::new(dag);

        // Store initial inputs as variables
        for arg in &inputs.arguments {
            if let Some(value) = &arg.value {
                let encoded = encode_message(value);
                self.graph.variables.insert(arg.key.clone(), encoded);
            }
        }

        // Create execution nodes for all DAG nodes
        for (node_id, node) in &dag.nodes {
            let (max_retries, timeout_seconds, backoff) = Self::extract_policies(&node.policies);
            let targets = if let Some(targets) = node.targets.clone() {
                targets
            } else if let Some(target) = node.target.clone() {
                vec![target]
            } else {
                Vec::new()
            };

            let exec_node = ExecutionNode {
                template_id: node_id.clone(),
                spread_index: None,
                status: NodeStatus::Blocked as i32,
                worker_id: None,
                started_at_ms: None,
                completed_at_ms: None,
                duration_ms: None,
                inputs: None,
                result: None,
                error: None,
                error_type: None,
                waiting_for: Vec::new(),
                completed_count: 0,
                attempt_number: 0,
                max_retries,
                attempts: Vec::new(),
                timeout_seconds,
                timeout_retry_limit: 3,
                backoff: Some(backoff),
                loop_index: None,
                loop_accumulators: None,
                targets,
                node_kind: determine_node_kind(node) as i32,
                parent_execution_id: None,
                iteration_index: None,
                execution_id: None,
            };

            self.graph.nodes.insert(node_id.clone(), exec_node);
        }

        // Find entry points (nodes with no predecessors) and mark them ready
        for (node_id, node) in &dag.nodes {
            let incoming_count = helper
                .get_incoming_edges(node_id)
                .iter()
                .filter(|edge| edge.edge_type == EdgeType::StateMachine)
                .count();
            if ((incoming_count == 0) || node.is_input)
                && let Some(exec_node) = self.graph.nodes.get_mut(node_id)
            {
                exec_node.status = NodeStatus::Pending as i32;
                self.graph.ready_queue.push(node_id.clone());
            }
        }
    }

    /// Extract retry/timeout/backoff from policy brackets
    fn extract_policies(policies: &[ast::PolicyBracket]) -> (i32, i32, BackoffConfig) {
        let mut max_retries = 3;
        let mut timeout_seconds = 300;
        let mut backoff = BackoffConfig {
            kind: BackoffKind::None as i32,
            base_delay_ms: 0,
            multiplier: 1.0,
        };

        for policy in policies {
            if let Some(kind) = &policy.kind {
                match kind {
                    ast::policy_bracket::Kind::Retry(retry) => {
                        max_retries = retry.max_retries as i32;
                        if let Some(backoff_duration) = &retry.backoff {
                            backoff = BackoffConfig {
                                kind: BackoffKind::Exponential as i32,
                                base_delay_ms: (backoff_duration.seconds * 1000) as i32,
                                multiplier: 2.0,
                            };
                        }
                    }
                    ast::policy_bracket::Kind::Timeout(timeout_policy) => {
                        if let Some(duration) = &timeout_policy.timeout {
                            timeout_seconds = duration.seconds as i32;
                        }
                    }
                }
            }
        }

        (max_retries, timeout_seconds, backoff)
    }

    /// Mark a node as running (dispatched to a worker).
    /// Returns the generated execution_id for this run.
    ///
    /// Note: `started_at_ms` is NOT set here - it will be set when the completion
    /// arrives, computed from `completed_at_ms - worker_duration_ms`. This ensures
    /// the recorded start time reflects when the worker actually started executing,
    /// not when we dispatched the action.
    pub fn mark_running(
        &mut self,
        node_id: &str,
        worker_id: &str,
        inputs: Option<Vec<u8>>,
    ) -> Option<String> {
        self.mark_running_inner(node_id, worker_id, inputs, true)
    }

    /// Mark a node as running without removing it from the ready queue.
    /// Returns the generated execution_id for this run.
    /// This allows callers to batch ready_queue removals for performance.
    pub fn mark_running_no_queue_removal(
        &mut self,
        node_id: &str,
        worker_id: &str,
        inputs: Option<Vec<u8>>,
    ) -> Option<String> {
        self.mark_running_inner(node_id, worker_id, inputs, false)
    }

    fn mark_running_inner(
        &mut self,
        node_id: &str,
        worker_id: &str,
        inputs: Option<Vec<u8>>,
        remove_ready: bool,
    ) -> Option<String> {
        let execution_id = if let Some(node) = self.graph.nodes.get_mut(node_id) {
            node.status = NodeStatus::Running as i32;
            node.worker_id = Some(worker_id.to_string());
            // started_at_ms is set on completion with accurate worker timing
            node.inputs = inputs;
            // Generate a unique execution_id for this specific run
            let exec_id = Uuid::new_v4().to_string();
            node.execution_id = Some(exec_id.clone());
            Some(exec_id)
        } else {
            None
        };

        if remove_ready {
            self.graph.ready_queue.retain(|id| id != node_id);
        }

        execution_id
    }

    /// Drain the ready queue, returning all node IDs that are ready to execute
    pub fn drain_ready_queue(&mut self) -> Vec<String> {
        std::mem::take(&mut self.graph.ready_queue)
    }

    /// Get a copy of the ready queue without draining it.
    /// Used when we need to iterate but only remove specific items.
    pub fn peek_ready_queue(&self) -> Vec<String> {
        self.graph.ready_queue.clone()
    }

    /// Remove a specific node from the ready queue.
    pub fn remove_from_ready_queue(&mut self, node_id: &str) {
        self.graph.ready_queue.retain(|id| id != node_id);
    }

    /// Remove a batch of nodes from the ready queue in one pass.
    pub fn remove_from_ready_queue_batch(&mut self, node_ids: &[String]) {
        if node_ids.is_empty() {
            return;
        }
        let remove_set: HashSet<&str> = node_ids.iter().map(String::as_str).collect();
        self.graph
            .ready_queue
            .retain(|id| !remove_set.contains(id.as_str()));
    }

    /// Check if a node exists and get its status
    pub fn get_node_status(&self, node_id: &str) -> Option<NodeStatus> {
        self.graph
            .nodes
            .get(node_id)
            .map(|n| NodeStatus::try_from(n.status).unwrap_or(NodeStatus::Unspecified))
    }

    /// Get the current attempt number for a node
    pub fn get_attempt_number(&self, node_id: &str) -> u32 {
        self.graph
            .nodes
            .get(node_id)
            .map(|n| n.attempt_number as u32)
            .unwrap_or(0)
    }

    /// Get the max retries for a node
    pub fn get_max_retries(&self, node_id: &str) -> u32 {
        self.graph
            .nodes
            .get(node_id)
            .map(|n| n.max_retries as u32)
            .unwrap_or(0)
    }

    /// Get the timeout in seconds for a node
    pub fn get_timeout_seconds(&self, node_id: &str) -> u32 {
        self.graph
            .nodes
            .get(node_id)
            .map(|n| n.timeout_seconds as u32)
            .unwrap_or(0)
    }

    /// Build a scope from the execution graph's variables for expression evaluation
    pub fn build_scope(&self) -> Scope {
        let mut scope = Scope::new();
        for (name, bytes) in &self.graph.variables {
            if name.contains(SCOPE_DELIM) {
                continue;
            }
            if let Some(value) = workflow_value_from_proto_bytes(bytes) {
                scope.insert(name.clone(), value);
            }
        }
        scope
    }

    pub fn build_scope_for_node(&self, node_id: &str) -> Scope {
        let mut scope = self.build_scope();

        let Some(prefix) = Self::scope_prefix(node_id) else {
            return scope;
        };

        if Self::is_bind_node(node_id) {
            if let Some(parent_prefix) = Self::parent_prefix(prefix) {
                self.merge_scoped_vars(&mut scope, parent_prefix);
            }
        } else {
            self.merge_scoped_vars(&mut scope, prefix);
        }

        scope
    }

    fn build_minimal_scope_for_node(
        &self,
        node_id: &str,
        exec_node: Option<&ExecutionNode>,
        loop_var: Option<&str>,
        required_vars: &HashSet<String>,
    ) -> Scope {
        let mut scope = Scope::new();
        if required_vars.is_empty() {
            return scope;
        }

        let mut prefix = Self::scope_prefix(node_id);
        if Self::is_bind_node(node_id) {
            prefix = prefix.and_then(Self::parent_prefix);
        }

        let template_id = exec_node
            .map(|node| node.template_id.as_str())
            .unwrap_or(node_id);
        let spread_index = exec_node.and_then(|node| node.spread_index);

        for var_name in required_vars {
            let mut value_bytes = None;

            // IMPORTANT: For spread actions, the loop variable must be looked up from the
            // spread-specific storage FIRST, before checking scoped variables. This handles
            // the case where the same variable name was used in an earlier for loop - we
            // don't want the stale value from that loop, we want the spread iteration value.
            if loop_var == Some(var_name.as_str())
                && let Some(index) = spread_index
            {
                let scoped_var = format!("{}[{}].{}", template_id, index, var_name);
                value_bytes = self.graph.variables.get(&scoped_var);
            }

            // Then try scoped variables (from function scope, loops, etc.)
            if value_bytes.is_none()
                && let Some(prefix) = prefix
            {
                let scoped_key = Self::scoped_var_key(prefix, var_name);
                value_bytes = self.graph.variables.get(&scoped_key);
            }

            // Finally try global variables
            if value_bytes.is_none() {
                value_bytes = self.graph.variables.get(var_name);
            }

            if let Some(bytes) = value_bytes
                && let Some(value) = workflow_value_from_proto_bytes(bytes)
            {
                scope.insert(var_name.clone(), value);
            }
        }

        scope
    }

    fn collect_expr_vars(expr: &ast::Expr, vars: &mut HashSet<String>) {
        use ast::expr;

        match &expr.kind {
            Some(expr::Kind::Variable(v)) => {
                vars.insert(v.name.clone());
            }
            Some(expr::Kind::BinaryOp(bin)) => {
                if let Some(left) = bin.left.as_ref() {
                    Self::collect_expr_vars(left, vars);
                }
                if let Some(right) = bin.right.as_ref() {
                    Self::collect_expr_vars(right, vars);
                }
            }
            Some(expr::Kind::UnaryOp(unary)) => {
                if let Some(op) = unary.operand.as_ref() {
                    Self::collect_expr_vars(op, vars);
                }
            }
            Some(expr::Kind::List(list)) => {
                for elem in &list.elements {
                    Self::collect_expr_vars(elem, vars);
                }
            }
            Some(expr::Kind::Dict(dict)) => {
                for entry in &dict.entries {
                    if let Some(key) = entry.key.as_ref() {
                        Self::collect_expr_vars(key, vars);
                    }
                    if let Some(value) = entry.value.as_ref() {
                        Self::collect_expr_vars(value, vars);
                    }
                }
            }
            Some(expr::Kind::Index(index)) => {
                if let Some(obj) = index.object.as_ref() {
                    Self::collect_expr_vars(obj, vars);
                }
                if let Some(idx) = index.index.as_ref() {
                    Self::collect_expr_vars(idx, vars);
                }
            }
            Some(expr::Kind::Dot(dot)) => {
                if let Some(obj) = dot.object.as_ref() {
                    Self::collect_expr_vars(obj, vars);
                }
            }
            Some(expr::Kind::FunctionCall(call)) => {
                for arg in &call.args {
                    Self::collect_expr_vars(arg, vars);
                }
                for kw in &call.kwargs {
                    if let Some(value) = kw.value.as_ref() {
                        Self::collect_expr_vars(value, vars);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn store_variable_for_node(
        &mut self,
        node_id: &str,
        node_type: &str,
        name: &str,
        value: &WorkflowValue,
    ) {
        if node_type == "return" {
            self.store_variable(name, value);
            return;
        }

        if let Some(prefix) = Self::scope_prefix(node_id) {
            let scoped_key = Self::scoped_var_key(prefix, name);
            let bytes = workflow_value_to_proto_bytes(value);
            self.graph.variables.insert(scoped_key, bytes);
            return;
        }

        self.store_variable(name, value);
    }

    pub fn store_raw_variable_for_node(
        &mut self,
        node_id: &str,
        node_type: &str,
        name: &str,
        bytes: Vec<u8>,
    ) {
        if node_type == "return" {
            self.graph.variables.insert(name.to_string(), bytes);
            return;
        }

        if let Some(prefix) = Self::scope_prefix(node_id) {
            let scoped_key = Self::scoped_var_key(prefix, name);
            self.graph.variables.insert(scoped_key, bytes);
            return;
        }

        self.graph.variables.insert(name.to_string(), bytes);
    }

    fn scope_prefix(node_id: &str) -> Option<&str> {
        let (prefix, _) = node_id.rsplit_once(':')?;
        if prefix.contains("fn_call") {
            Some(prefix)
        } else {
            None
        }
    }

    fn parent_prefix(prefix: &str) -> Option<&str> {
        let (parent, _) = prefix.rsplit_once(':')?;
        if parent.contains("fn_call") {
            Some(parent)
        } else {
            None
        }
    }

    fn scoped_var_key(prefix: &str, name: &str) -> String {
        format!("{}{}{}", prefix, SCOPE_DELIM, name)
    }

    fn is_bind_node(node_id: &str) -> bool {
        node_id
            .rsplit(':')
            .next()
            .map(|segment| segment.starts_with("bind_"))
            .unwrap_or(false)
    }

    fn merge_scoped_vars(&self, scope: &mut Scope, prefix: &str) {
        for (name, bytes) in &self.graph.variables {
            let Some((key_prefix, var_name)) = name.split_once(SCOPE_DELIM) else {
                continue;
            };
            if key_prefix != prefix {
                continue;
            }
            if let Some(value) = workflow_value_from_proto_bytes(bytes) {
                scope.insert(var_name.to_string(), value);
            }
        }
    }

    /// Store a value in the variables map
    pub fn store_variable(&mut self, name: &str, value: &WorkflowValue) {
        let bytes = workflow_value_to_proto_bytes(value);
        self.graph.variables.insert(name.to_string(), bytes);
    }

    fn build_error_payload(message: &str) -> Vec<u8> {
        let mut values = HashMap::new();
        values.insert(
            "args".to_string(),
            WorkflowValue::Tuple(vec![WorkflowValue::String(message.to_string())]),
        );
        let error = WorkflowValue::Exception {
            exc_type: "RuntimeError".to_string(),
            module: "builtins".to_string(),
            message: message.to_string(),
            traceback: String::new(),
            values,
            type_hierarchy: vec![
                "RuntimeError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        };
        let args = WorkflowArguments {
            arguments: vec![crate::messages::proto::WorkflowArgument {
                key: "error".to_string(),
                value: Some(error.to_proto()),
            }],
        };
        encode_message(&args)
    }

    /// Evaluate a guard expression against current variables.
    fn evaluate_guard(&self, guard_expr: &ast::Expr, scope: &Scope) -> Result<bool, String> {
        match ExpressionEvaluator::evaluate(guard_expr, scope) {
            Ok(value) => Ok(ExpressionEvaluator::is_truthy(&value)),
            Err(e) => {
                warn!(error = %e, "Failed to evaluate guard expression");
                Err(e.to_string())
            }
        }
    }

    fn extract_exception_info(
        &mut self,
        node_id: &str,
        completion: &Completion,
    ) -> Option<ExceptionInfo> {
        let Some(result_bytes) = &completion.result else {
            return None;
        };

        let Ok(args) = decode_message::<WorkflowArguments>(result_bytes) else {
            return None;
        };

        for arg in args.arguments {
            if arg.key != "error" {
                continue;
            }

            let Some(value) = arg.value else {
                continue;
            };

            let value = WorkflowValue::from_proto(&value);
            if let WorkflowValue::Exception {
                exc_type,
                module,
                message,
                traceback,
                values,
                type_hierarchy,
            } = value
            {
                self.store_variable(
                    EXCEPTION_SCOPE_VAR,
                    &WorkflowValue::Exception {
                        exc_type: exc_type.clone(),
                        module: module.clone(),
                        message: message.clone(),
                        traceback: traceback.clone(),
                        values,
                        type_hierarchy: type_hierarchy.clone(),
                    },
                );

                return Some(ExceptionInfo {
                    error_type: exc_type,
                    error_module: module,
                    error_message: message,
                    traceback,
                    type_hierarchy,
                    source_node_id: node_id.to_string(),
                });
            }
        }

        None
    }

    /// Filter edges by evaluating their guards, returning only enabled edges.
    /// For guarded branches, only edges whose guards evaluate to true are enabled.
    /// For else edges, they're enabled only when no guarded edge was enabled.
    /// For edges without guards, they're always enabled.
    fn filter_edges_by_guards<'a>(
        &self,
        edges: &'a [DAGEdge],
        scope: &Scope,
        dag: &DAG,
    ) -> (Vec<&'a DAGEdge>, Vec<String>) {
        // Log all incoming edges for debugging
        trace!(
            edge_count = edges.len(),
            scope_vars_len = scope.len(),
            "filter_edges_by_guards called"
        );
        if tracing::enabled!(tracing::Level::TRACE) {
            let scope_vars = scope.keys().collect::<Vec<_>>();
            tracing::trace!(
                edge_count = edges.len(),
                scope_vars = ?scope_vars,
                "filter_edges_by_guards called"
            );
            for edge in edges {
                tracing::trace!(
                    source = %edge.source,
                    target = %edge.target,
                    has_guard_expr = edge.guard_expr.is_some(),
                    is_else = edge.is_else,
                    is_loop_back = edge.is_loop_back,
                    guard_string = ?edge.guard_string,
                    edge_type = ?edge.edge_type,
                    "Processing edge in filter_edges_by_guards"
                );
            }
        }

        // Separate edges by type
        let mut guarded_edges: Vec<&DAGEdge> = Vec::new();
        let mut else_edge: Option<&DAGEdge> = None;
        let mut unguarded_edges: Vec<&DAGEdge> = Vec::new();
        let mut guard_errors: Vec<String> = Vec::new();

        for edge in edges {
            if edge.edge_type != EdgeType::StateMachine {
                continue;
            }

            if edge.is_else {
                else_edge = Some(edge);
            } else if edge.guard_expr.is_some() {
                guarded_edges.push(edge);
            } else {
                // No guard - always enabled (but check for guard_string as fallback)
                if let Some(guard_str) = &edge.guard_string {
                    // Guard string like "__loop_i < len(items)" - try to evaluate
                    // This is a fallback for edges that have string-based guards
                    if let Some(dag_node) = dag.nodes.get(&edge.source)
                        && let Some(guard_expr) = &dag_node.guard_expr
                    {
                        match self.evaluate_guard(guard_expr, scope) {
                            Ok(true) => guarded_edges.push(edge),
                            Ok(false) => {}
                            Err(e) => guard_errors.push(format!("Guard evaluation failed: {}", e)),
                        }
                        continue;
                    }
                    // Can't evaluate string guard, treat as enabled
                    trace!(guard_string = %guard_str, "Edge has string guard, treating as enabled");
                    unguarded_edges.push(edge);
                } else {
                    unguarded_edges.push(edge);
                }
            }
        }

        let mut enabled: Vec<&DAGEdge> = Vec::new();

        // Evaluate guarded edges
        let mut any_guard_passed = false;
        for edge in &guarded_edges {
            if let Some(guard_expr) = &edge.guard_expr {
                match self.evaluate_guard(guard_expr, scope) {
                    Ok(true) => {
                        trace!(
                            source = %edge.source,
                            target = %edge.target,
                            "Guard expression passed"
                        );
                        enabled.push(edge);
                        any_guard_passed = true;
                    }
                    Ok(false) => {
                        trace!(
                            source = %edge.source,
                            target = %edge.target,
                            "Guard expression failed"
                        );
                    }
                    Err(e) => guard_errors.push(format!("Guard evaluation failed: {}", e)),
                }
            }
        }

        // If no guarded edge passed and there's an else edge, enable it
        if !any_guard_passed && let Some(else_e) = else_edge {
            trace!(
                source = %else_e.source,
                target = %else_e.target,
                "Enabling else edge (no guards passed)"
            );
            enabled.push(else_e);
        }

        // Always include unguarded edges (normal control flow)
        enabled.extend(unguarded_edges);

        (enabled, guard_errors)
    }

    /// Get the inputs for a node (gathered from variables based on DAG dependencies)
    pub fn get_inputs_for_node(&self, node_id: &str, dag: &DAG) -> Option<Vec<u8>> {
        let exec_node = self.graph.nodes.get(node_id);
        let template_id = exec_node.map(|n| n.template_id.as_str()).unwrap_or(node_id);
        let dag_node = dag.nodes.get(template_id)?;

        // Build kwargs from the DAG node's kwarg expressions when available.
        let mut kwargs = WorkflowArguments { arguments: vec![] };
        let mut required_vars = HashSet::new();
        if let Some(kwarg_exprs) = &dag_node.kwarg_exprs {
            for expr in kwarg_exprs.values() {
                Self::collect_expr_vars(expr, &mut required_vars);
            }
        } else if let Some(kwarg_map) = &dag_node.kwargs {
            for var_ref in kwarg_map.values() {
                required_vars.insert(var_ref.trim_start_matches('$').to_string());
            }
        }

        let scope = self.build_minimal_scope_for_node(
            node_id,
            exec_node,
            dag_node.spread_loop_var.as_deref(),
            &required_vars,
        );

        if let Some(kwarg_exprs) = &dag_node.kwarg_exprs {
            for (key, expr) in kwarg_exprs {
                match ExpressionEvaluator::evaluate(expr, &scope) {
                    Ok(value) => {
                        kwargs
                            .arguments
                            .push(crate::messages::proto::WorkflowArgument {
                                key: key.clone(),
                                value: Some(value.to_proto()),
                            });
                    }
                    Err(e) => {
                        warn!(
                            node_id = %node_id,
                            key = %key,
                            error = %e,
                            "Failed to evaluate kwarg expression"
                        );
                    }
                }
            }
        } else if let Some(kwarg_map) = &dag_node.kwargs {
            for (key, var_ref) in kwarg_map {
                // var_ref is like "$variable_name"
                let var_name = var_ref.trim_start_matches('$');
                if let Some(value) = scope.get(var_name) {
                    kwargs
                        .arguments
                        .push(crate::messages::proto::WorkflowArgument {
                            key: key.clone(),
                            value: Some(value.to_proto()),
                        });
                }
            }
        }

        Some(encode_message(&kwargs))
    }

    /// Check if the workflow is already complete or failed.
    ///
    /// This is used to detect completion when there are no new pending completions,
    /// e.g., after a failed completion DB write (lease expired) caused the instance
    /// to stay in-memory but not be archived.
    pub fn check_workflow_completion(&self, dag: &DAG) -> BatchCompletionResult {
        let mut result = BatchCompletionResult::default();

        // Check for uncaught exceptions (workflow failed)
        for node in self.graph.nodes.values() {
            if node.status == NodeStatus::Exhausted as i32 {
                result.workflow_failed = true;
                result.error_message = node.error.clone();
                return result;
            }
        }

        // Check for workflow completion (entry function output node completed)
        let entry_function = dag
            .nodes
            .values()
            .find(|n| n.is_input)
            .and_then(|n| n.function_name.clone());

        for node in dag.nodes.values() {
            let is_entry_output = node.is_output
                && entry_function
                    .as_deref()
                    .map(|name| node.function_name.as_deref() == Some(name))
                    .unwrap_or(true);

            if is_entry_output
                && let Some(exec_node) = self.graph.nodes.get(&node.id)
                && exec_node.status == NodeStatus::Completed as i32
            {
                result.workflow_completed = true;

                // Build result payload from the "result" variable
                if let Some(result_value_bytes) = self.graph.variables.get("result") {
                    let value = if let Ok(value) = decode_message::<
                        crate::messages::proto::WorkflowArgumentValue,
                    >(result_value_bytes)
                    {
                        value
                    } else if let Some(workflow_value) =
                        crate::inline_executor::extract_result_value(result_value_bytes)
                    {
                        workflow_value.to_proto()
                    } else {
                        warn!("Failed to decode result variable, storing null payload");
                        WorkflowValue::Null.to_proto()
                    };
                    let args = WorkflowArguments {
                        arguments: vec![crate::messages::proto::WorkflowArgument {
                            key: "result".to_string(),
                            value: Some(value),
                        }],
                    };
                    result.result_payload = Some(encode_message(&args));
                } else {
                    let args = WorkflowArguments {
                        arguments: vec![crate::messages::proto::WorkflowArgument {
                            key: "result".to_string(),
                            value: Some(WorkflowValue::Null.to_proto()),
                        }],
                    };
                    result.result_payload = Some(encode_message(&args));
                }
                return result;
            }
        }

        result
    }

    /// Apply a batch of completions and compute newly ready nodes
    pub fn apply_completions_batch(
        &mut self,
        completions: Vec<Completion>,
        dag: &DAG,
    ) -> BatchCompletionResult {
        // Log entry for debugging
        debug!(
            completion_count = completions.len(),
            "apply_completions_batch called"
        );
        if tracing::enabled!(tracing::Level::TRACE) {
            let completion_ids: Vec<_> = completions.iter().map(|c| c.node_id.as_str()).collect();
            tracing::trace!(
                completion_count = completions.len(),
                node_ids = ?completion_ids,
                "apply_completions_batch called"
            );
        }

        let mut result = BatchCompletionResult::default();
        let helper = DAGHelper::new(dag);
        let now_ms = Utc::now().timestamp_millis();
        let mut exception_routes: HashMap<String, Vec<DAGEdge>> = HashMap::new();

        // 1. Mark all completed nodes and record attempts
        for completion in &completions {
            let edge_source_id = self
                .graph
                .nodes
                .get(&completion.node_id)
                .map(|n| n.template_id.clone())
                .unwrap_or_else(|| completion.node_id.clone());
            let can_retry = self
                .graph
                .nodes
                .get(&completion.node_id)
                .map(|n| n.attempt_number < n.max_retries)
                .unwrap_or(false);
            let mut exception_info = None;
            if !completion.success && !can_retry {
                exception_info = self.extract_exception_info(&completion.node_id, completion);
            }

            let node_type = self
                .graph
                .nodes
                .get(&completion.node_id)
                .and_then(|exec| dag.nodes.get(&exec.template_id))
                .map(|n| n.node_type.clone())
                .unwrap_or_default();

            if let Some(node) = self.graph.nodes.get_mut(&completion.node_id) {
                // Compute started_at from worker_duration if available (most accurate),
                // otherwise fall back to previously set value or current time
                let started_at = completion
                    .worker_duration_ms
                    .map(|wd| now_ms - wd)
                    .or(node.started_at_ms)
                    .unwrap_or(now_ms);

                // Set started_at_ms now that we have accurate timing
                if node.started_at_ms.is_none() {
                    node.started_at_ms = Some(started_at);
                }

                let attempt = AttemptRecord {
                    attempt_number: node.attempt_number,
                    worker_id: completion.worker_id.clone(),
                    started_at_ms: started_at,
                    completed_at_ms: now_ms,
                    duration_ms: completion.duration_ms,
                    success: completion.success,
                    result: completion.result.clone(),
                    error: completion.error.clone(),
                    error_type: completion.error_type.clone(),
                };
                node.attempts.push(attempt);

                if completion.success {
                    node.status = NodeStatus::Completed as i32;
                    node.result = completion.result.clone();
                    node.completed_at_ms = Some(now_ms);
                    node.duration_ms = Some(completion.duration_ms);
                    node.worker_id = None;

                    // Store result in variables if node has targets
                    // The result_bytes is a WorkflowArguments containing {key: "result", value: <actual_value>}
                    // We need to extract the actual WorkflowArgumentValue and store that
                    if !node.targets.is_empty()
                        && let Some(result_bytes) = &completion.result
                    {
                        let workflow_value = if result_bytes.is_empty() {
                            warn!(
                                node_id = %completion.node_id,
                                "Empty result payload; skipping variable storage"
                            );
                            None
                        } else {
                            let decoded =
                                crate::inline_executor::extract_result_value(result_bytes);
                            if decoded.is_none() {
                                warn!(
                                    node_id = %completion.node_id,
                                    "Failed to decode result value; skipping variable storage"
                                );
                            }
                            decoded
                        };

                        if let Some(workflow_value) = workflow_value {
                            let targets = node.targets.clone();

                            // Unpack tuples/lists when there are multiple targets
                            if targets.len() > 1 {
                                match &workflow_value {
                                    WorkflowValue::Tuple(items) | WorkflowValue::List(items) => {
                                        for (target, item) in targets.iter().zip(items.iter()) {
                                            self.store_variable_for_node(
                                                &completion.node_id,
                                                &node_type,
                                                target,
                                                item,
                                            );
                                        }
                                    }
                                    _ => {
                                        // Non-iterable value with multiple targets - store as-is
                                        warn!(
                                            node_id = %completion.node_id,
                                            targets = ?targets,
                                            "Value is not iterable for tuple unpacking"
                                        );
                                        for target in &targets {
                                            self.store_variable_for_node(
                                                &completion.node_id,
                                                &node_type,
                                                target,
                                                &workflow_value,
                                            );
                                        }
                                    }
                                }
                            } else {
                                // Single target - store entire value
                                for target in &targets {
                                    self.store_variable_for_node(
                                        &completion.node_id,
                                        &node_type,
                                        target,
                                        &workflow_value,
                                    );
                                }
                            }
                        }
                    }
                } else {
                    // Check if we can retry
                    if can_retry {
                        node.attempt_number += 1;
                        node.status = NodeStatus::Pending as i32;
                        node.worker_id = None;
                        self.graph.ready_queue.push(completion.node_id.clone());
                        if !result.newly_ready.contains(&completion.node_id) {
                            result.newly_ready.push(completion.node_id.clone());
                        }
                    } else {
                        if exception_info.is_none()
                            && let Some(error_type) = &completion.error_type
                        {
                            exception_info = Some(ExceptionInfo {
                                error_type: error_type.clone(),
                                error_module: String::new(),
                                error_message: completion.error.clone().unwrap_or_default(),
                                traceback: String::new(),
                                type_hierarchy: vec![error_type.clone()],
                                source_node_id: completion.node_id.clone(),
                            });
                        }

                        if let Some(info) = exception_info.clone() {
                            self.graph
                                .exceptions
                                .insert(completion.node_id.clone(), info);
                        }

                        let exception_edges: Vec<DAGEdge> = helper
                            .get_outgoing_edges(&edge_source_id)
                            .iter()
                            .filter(|edge| {
                                edge.edge_type == EdgeType::StateMachine
                                    && edge.exception_types.is_some()
                            })
                            .map(|edge| (*edge).clone())
                            .collect();

                        let matching_exception_edges: Vec<DAGEdge> = exception_edges
                            .into_iter()
                            .filter(|edge| {
                                let Some(types) = edge.exception_types.as_ref() else {
                                    return false;
                                };

                                if types.is_empty() {
                                    return true;
                                }

                                if let Some(info) = &exception_info {
                                    if info.type_hierarchy.iter().any(|t| types.contains(t)) {
                                        return true;
                                    }
                                    return types.contains(&info.error_type);
                                }

                                completion
                                    .error_type
                                    .as_ref()
                                    .map(|t| types.contains(t))
                                    .unwrap_or(false)
                            })
                            .collect();

                        let matching_exception_edges = if !matching_exception_edges.is_empty() {
                            let max_depth = matching_exception_edges
                                .iter()
                                .map(|edge| edge.exception_depth.unwrap_or(0))
                                .max()
                                .unwrap_or(0);
                            let edges_at_max_depth: Vec<_> = matching_exception_edges
                                .into_iter()
                                .filter(|edge| edge.exception_depth.unwrap_or(0) == max_depth)
                                .collect();
                            // Select only the first matching handler - Python semantics require
                            // that only the first matching except handler executes
                            if let Some(first) = edges_at_max_depth.into_iter().next() {
                                vec![first]
                            } else {
                                vec![]
                            }
                        } else {
                            matching_exception_edges
                        };

                        if !matching_exception_edges.is_empty() {
                            node.status = NodeStatus::Caught as i32;
                            node.error = completion.error.clone();
                            node.error_type = completion.error_type.clone();
                            node.completed_at_ms = Some(now_ms);
                            exception_routes
                                .insert(completion.node_id.clone(), matching_exception_edges);
                        } else {
                            node.status = NodeStatus::Exhausted as i32;
                            node.error = completion.error.clone();
                            node.error_type = completion.error_type.clone();
                            node.completed_at_ms = Some(now_ms);

                            result.workflow_failed = true;
                            result.error_message = completion.error.clone();
                        }
                    }
                }
            }
        }

        // 2. Build scope for guard evaluation
        let is_completed = |status: i32| {
            matches!(
                NodeStatus::try_from(status).unwrap_or(NodeStatus::Unspecified),
                NodeStatus::Completed | NodeStatus::Caught
            )
        };

        // 3. Find successors that should become ready, evaluating guards
        let mut ready_successors: Vec<(String, Option<Vec<Vec<u8>>>)> = Vec::new();

        for completion in &completions {
            let edge_source_id = self
                .graph
                .nodes
                .get(&completion.node_id)
                .map(|n| n.template_id.as_str())
                .unwrap_or(completion.node_id.as_str());
            let outgoing_edges: Vec<DAGEdge> = if completion.success {
                // Get all outgoing state machine edges from the completed node
                helper
                    .get_state_machine_successors(edge_source_id)
                    .into_iter()
                    .cloned()
                    .collect()
            } else if let Some(edges) = exception_routes.get(&completion.node_id) {
                edges.clone()
            } else {
                continue;
            };

            trace!(
                source_node = %completion.node_id,
                outgoing_edge_count = outgoing_edges.len(),
                "Found outgoing edges for completed node"
            );

            // Evaluate guards to determine which edges are enabled
            let mut guard_vars = HashSet::new();
            for edge in &outgoing_edges {
                if let Some(expr) = edge.guard_expr.as_ref() {
                    Self::collect_expr_vars(expr, &mut guard_vars);
                }
            }
            let exec_node = self.graph.nodes.get(&completion.node_id);
            let loop_var = exec_node
                .and_then(|node| dag.nodes.get(&node.template_id))
                .and_then(|node| node.spread_loop_var.as_deref());
            let scope = self.build_minimal_scope_for_node(
                &completion.node_id,
                exec_node,
                loop_var,
                &guard_vars,
            );
            let (enabled_edges, guard_errors) =
                self.filter_edges_by_guards(&outgoing_edges, &scope, dag);
            if !guard_errors.is_empty() {
                let message = format!(
                    "Guard evaluation failed during startup: {}",
                    guard_errors.join("; ")
                );
                result.workflow_failed = true;
                result.error_message = Some(message.clone());
                result.result_payload = Some(Self::build_error_payload(&message));
                return result;
            }

            if tracing::enabled!(tracing::Level::TRACE) {
                let enabled_targets = enabled_edges
                    .iter()
                    .map(|e| e.target.as_str())
                    .collect::<Vec<_>>();
                tracing::trace!(
                    source_node = %completion.node_id,
                    enabled_edge_count = enabled_edges.len(),
                    enabled_targets = ?enabled_targets,
                    "Filtered enabled edges"
                );
            }

            for edge in enabled_edges {
                let successor_id = &edge.target;

                // Handle loop-back edges: reset node status to allow re-visitation
                if edge.is_loop_back {
                    // Get current iteration index before reset
                    let current_idx = self
                        .graph
                        .nodes
                        .get(successor_id)
                        .and_then(|n| n.loop_index)
                        .unwrap_or(0);

                    self.reset_nodes_for_loop(successor_id, &edge.source, dag, current_idx);

                    if let Some(successor) = self.graph.nodes.get_mut(successor_id) {
                        // Increment loop_index for tracking
                        let new_idx = current_idx + 1;
                        successor.loop_index = Some(new_idx);
                        trace!(
                            node_id = %successor_id,
                            loop_index = new_idx,
                            "Reset node for loop iteration"
                        );

                        // Check for infinite loop
                        if new_idx > MAX_LOOP_ITERATIONS {
                            let message = format!(
                                "Loop exceeded maximum iterations ({}) at node '{}'. \
                                 This may indicate an infinite loop (e.g., `while True:`).",
                                MAX_LOOP_ITERATIONS, successor_id
                            );
                            warn!(
                                node_id = %successor_id,
                                iterations = new_idx,
                                max = MAX_LOOP_ITERATIONS,
                                "Loop exceeded max iterations, terminating workflow"
                            );
                            result.workflow_failed = true;
                            result.error_message = Some(message.clone());
                            result.result_payload = Some(Self::build_error_payload(&message));
                            return result;
                        }
                    }
                }

                let successor = match self.graph.nodes.get(successor_id) {
                    Some(n) => n,
                    None => {
                        debug!(successor_id = %successor_id, "Successor node not found in execution graph");
                        continue;
                    }
                };

                // Check current status
                let status =
                    NodeStatus::try_from(successor.status).unwrap_or(NodeStatus::Unspecified);
                trace!(
                    successor_id = %successor_id,
                    status = ?status,
                    "Checking successor status"
                );
                if status != NodeStatus::Blocked {
                    trace!(
                        successor_id = %successor_id,
                        status = ?status,
                        "Skipping successor - not blocked"
                    );
                    continue;
                }

                // Exception handler: if the edge has exception_types, the handler is ready
                // immediately because the exception has already occurred and been matched.
                // We don't need to wait for other potential exception sources.
                if edge.exception_types.is_some() {
                    trace!(
                        successor_id = %successor_id,
                        exception_types = ?edge.exception_types,
                        "Exception handler ready via exception edge"
                    );
                    if !ready_successors.iter().any(|(id, _)| id == successor_id) {
                        ready_successors.push((successor_id.clone(), None));
                        trace!(successor_id = %successor_id, "Added exception handler to ready_successors");
                    }
                    continue;
                }

                // Check if this is a barrier (has waiting_for)
                if !successor.waiting_for.is_empty() {
                    // Count completed predecessors
                    let completed = successor
                        .waiting_for
                        .iter()
                        .filter(|id| {
                            self.graph
                                .nodes
                                .get(*id)
                                .map(|n| is_completed(n.status))
                                .unwrap_or(false)
                        })
                        .count();

                    if completed == successor.waiting_for.len() {
                        // All predecessors done - barrier is ready
                        let aggregated: Vec<Vec<u8>> = successor
                            .waiting_for
                            .iter()
                            .filter_map(|id| {
                                self.graph.nodes.get(id).and_then(|n| n.result.clone())
                            })
                            .collect();

                        if !ready_successors.iter().any(|(id, _)| id == successor_id) {
                            ready_successors.push((successor_id.clone(), Some(aggregated)));
                        }
                    }
                } else {
                    // Regular node - check if predecessors are ready
                    // Some nodes (joins after conditionals) only require N predecessors, not all
                    let template_id = &successor.template_id;
                    let incoming: Vec<&DAGEdge> = helper
                        .get_incoming_edges(template_id)
                        .iter()
                        .filter(|edge| {
                            edge.edge_type == EdgeType::StateMachine
                                && edge.exception_types.is_none()
                        })
                        .copied()
                        .collect();

                    trace!(
                        successor_id = %successor_id,
                        template_id = %template_id,
                        incoming_edge_count = incoming.len(),
                        "Checking incoming edges for successor"
                    );

                    // Check if this node has a join_required_count override
                    let join_required = dag
                        .nodes
                        .get(template_id)
                        .and_then(|n| n.join_required_count)
                        .map(|c| c as usize);

                    // Count completed predecessors and total non-loop-back edges
                    let mut completed_count = 0usize;
                    let mut total_non_loopback = 0usize;

                    for inc_edge in incoming {
                        // Skip loop-back edges when checking readiness (they don't block forward progress)
                        if inc_edge.is_loop_back {
                            trace!(
                                successor_id = %successor_id,
                                edge_source = %inc_edge.source,
                                "Skipping loop-back edge"
                            );
                            continue;
                        }

                        total_non_loopback += 1;

                        let source_status = self
                            .graph
                            .nodes
                            .get(&inc_edge.source)
                            .map(|n| is_completed(n.status))
                            .unwrap_or(false);
                        trace!(
                            successor_id = %successor_id,
                            edge_source = %inc_edge.source,
                            source_completed = source_status,
                            "Checking incoming edge source status"
                        );

                        if source_status {
                            completed_count += 1;
                        }
                    }

                    // Determine if the node is ready based on join_required_count or all-edges logic
                    let is_ready = if let Some(required) = join_required {
                        // Join node with required count - ready when at least N predecessors complete
                        completed_count >= required
                    } else {
                        // Regular node - all non-loop-back edges must be done
                        completed_count == total_non_loopback
                    };

                    trace!(
                        successor_id = %successor_id,
                        completed = completed_count,
                        total = total_non_loopback,
                        join_required = ?join_required,
                        is_ready = is_ready,
                        "Incoming edges check result"
                    );

                    if is_ready && !ready_successors.iter().any(|(id, _)| id == successor_id) {
                        ready_successors.push((successor_id.clone(), None));
                        trace!(successor_id = %successor_id, "Added successor to ready_successors");
                    }
                }
            }
        }

        // 4. Update completed_count for barrier nodes
        for (successor_id, _) in &ready_successors {
            if let Some(successor) = self.graph.nodes.get(successor_id)
                && !successor.waiting_for.is_empty()
            {
                let completed = successor
                    .waiting_for
                    .iter()
                    .filter(|id| {
                        self.graph
                            .nodes
                            .get(*id)
                            .map(|n| is_completed(n.status))
                            .unwrap_or(false)
                    })
                    .count() as i32;

                if let Some(successor_mut) = self.graph.nodes.get_mut(successor_id) {
                    successor_mut.completed_count = completed;
                }
            }
        }

        // 5. Mark nodes as ready and store aggregated results
        for (successor_id, _aggregated) in ready_successors {
            if let Some(successor) = self.graph.nodes.get_mut(&successor_id) {
                successor.status = NodeStatus::Pending as i32;
            }

            result.newly_ready.push(successor_id.clone());
            self.graph.ready_queue.push(successor_id);
        }

        // 4. Check for workflow completion (entry function output node only)
        let entry_function = dag
            .nodes
            .values()
            .find(|n| n.is_input)
            .and_then(|n| n.function_name.clone());

        for node in dag.nodes.values() {
            let is_entry_output = node.is_output
                && entry_function
                    .as_deref()
                    .map(|name| node.function_name.as_deref() == Some(name))
                    .unwrap_or(true);

            if is_entry_output
                && let Some(exec_node) = self.graph.nodes.get(&node.id)
                && exec_node.status == NodeStatus::Completed as i32
            {
                result.workflow_completed = true;

                // Build result payload from the "result" variable
                // The Python side expects WorkflowArguments with key "result"
                if let Some(result_value_bytes) = self.graph.variables.get("result") {
                    // result_value_bytes is a WorkflowArgumentValue
                    // Wrap it in WorkflowArguments with key "result"
                    let value = if let Ok(value) = decode_message::<
                        crate::messages::proto::WorkflowArgumentValue,
                    >(result_value_bytes)
                    {
                        value
                    } else if let Some(workflow_value) =
                        crate::inline_executor::extract_result_value(result_value_bytes)
                    {
                        workflow_value.to_proto()
                    } else {
                        warn!("Failed to decode result variable, storing null payload");
                        WorkflowValue::Null.to_proto()
                    };
                    let args = WorkflowArguments {
                        arguments: vec![crate::messages::proto::WorkflowArgument {
                            key: "result".to_string(),
                            value: Some(value),
                        }],
                    };
                    result.result_payload = Some(encode_message(&args));
                } else {
                    let args = WorkflowArguments {
                        arguments: vec![crate::messages::proto::WorkflowArgument {
                            key: "result".to_string(),
                            value: Some(WorkflowValue::Null.to_proto()),
                        }],
                    };
                    result.result_payload = Some(encode_message(&args));
                }
            }
        }

        result
    }

    /// Reset nodes for the next loop iteration.
    /// Archived nodes keep their execution_id, which is used to retrieve their payloads.
    fn reset_nodes_for_loop(
        &mut self,
        loop_head_id: &str,
        loop_back_source: &str,
        dag: &DAG,
        iteration_idx: i32,
    ) {
        let nodes_in_loop = Self::collect_loop_nodes(loop_head_id, loop_back_source, dag);
        if nodes_in_loop.is_empty() {
            return;
        }

        for node_id in &nodes_in_loop {
            // Preserve completed iteration by moving to indexed node ID
            if let Some(mut node) = self.graph.nodes.remove(node_id) {
                // Only preserve if the node actually executed (has timing data)
                if node.started_at_ms.is_some() {
                    let iter_node_id = format!("{}[iter_{}]", node_id, iteration_idx);
                    // Set parent and iteration info on the archived node
                    // Note: execution_id is preserved, which allows payload hydration to work
                    node.parent_execution_id = Some(node_id.clone());
                    node.iteration_index = Some(iteration_idx);
                    self.graph.nodes.insert(iter_node_id, node.clone());
                }

                // Create fresh node for next iteration (execution_id will be set by mark_running)
                let fresh_node = ExecutionNode {
                    template_id: node.template_id.clone(),
                    spread_index: node.spread_index,
                    status: NodeStatus::Blocked as i32,
                    worker_id: None,
                    started_at_ms: None,
                    completed_at_ms: None,
                    duration_ms: None,
                    inputs: None,
                    result: None,
                    error: None,
                    error_type: None,
                    waiting_for: Vec::new(),
                    completed_count: 0,
                    attempt_number: 0,
                    max_retries: node.max_retries,
                    attempts: Vec::new(),
                    timeout_seconds: node.timeout_seconds,
                    timeout_retry_limit: node.timeout_retry_limit,
                    backoff: node.backoff.clone(),
                    loop_index: node.loop_index,
                    loop_accumulators: node.loop_accumulators.clone(),
                    targets: node.targets.clone(),
                    node_kind: node.node_kind,
                    parent_execution_id: None,
                    iteration_index: None,
                    execution_id: None,
                };
                self.graph.nodes.insert(node_id.clone(), fresh_node);
            }
        }

        self.graph
            .ready_queue
            .retain(|id| !nodes_in_loop.contains(id));
    }

    fn collect_loop_nodes(
        loop_head_id: &str,
        loop_back_source: &str,
        dag: &DAG,
    ) -> HashSet<String> {
        let forward = Self::reachable_from(loop_head_id, dag);
        let backward = Self::reachable_to(loop_back_source, dag);
        forward
            .intersection(&backward)
            .cloned()
            .collect::<HashSet<String>>()
    }

    fn reachable_from(start: &str, dag: &DAG) -> HashSet<String> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut stack = vec![start.to_string()];
        visited.insert(start.to_string());

        while let Some(current) = stack.pop() {
            for edge in dag.edges.iter().filter(|edge| {
                edge.edge_type == EdgeType::StateMachine
                    && !edge.is_loop_back
                    && edge.source == current
            }) {
                if visited.insert(edge.target.clone()) {
                    stack.push(edge.target.clone());
                }
            }
        }

        visited
    }

    fn reachable_to(target: &str, dag: &DAG) -> HashSet<String> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut stack = vec![target.to_string()];
        visited.insert(target.to_string());

        while let Some(current) = stack.pop() {
            for edge in dag.edges.iter().filter(|edge| {
                edge.edge_type == EdgeType::StateMachine
                    && !edge.is_loop_back
                    && edge.target == current
            }) {
                if visited.insert(edge.source.clone()) {
                    stack.push(edge.source.clone());
                }
            }
        }

        visited
    }

    /// Expand a spread node into N concrete execution nodes
    pub fn expand_spread(
        &mut self,
        spread_node_id: &str,
        items: Vec<WorkflowValue>,
        dag: &DAG,
    ) -> Vec<String> {
        let dag_node = match dag.nodes.get(spread_node_id) {
            Some(n) => n,
            None => {
                warn!("Spread node {} not found in DAG", spread_node_id);
                return Vec::new();
            }
        };

        let barrier_id = match &dag_node.aggregates_to {
            Some(id) => id.clone(),
            None => {
                warn!("Spread node {} has no aggregates_to", spread_node_id);
                return Vec::new();
            }
        };

        let loop_var = dag_node.spread_loop_var.as_deref().unwrap_or("item");

        // Create N concrete execution nodes
        let expanded_ids: Vec<String> = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| {
                let node_id = format!("{}[{}]", spread_node_id, i);

                // Get template node's config
                let template = self.graph.nodes.get(spread_node_id).cloned();

                let exec_node = ExecutionNode {
                    template_id: spread_node_id.to_string(),
                    spread_index: Some(i as i32),
                    status: NodeStatus::Pending as i32,
                    worker_id: None,
                    started_at_ms: None,
                    completed_at_ms: None,
                    duration_ms: None,
                    inputs: None,
                    result: None,
                    error: None,
                    error_type: None,
                    waiting_for: Vec::new(),
                    completed_count: 0,
                    attempt_number: 0,
                    max_retries: template.as_ref().map(|t| t.max_retries).unwrap_or(3),
                    attempts: Vec::new(),
                    timeout_seconds: template.as_ref().map(|t| t.timeout_seconds).unwrap_or(300),
                    timeout_retry_limit: 3,
                    backoff: template.and_then(|t| t.backoff),
                    loop_index: None,
                    loop_accumulators: None,
                    targets: vec![],
                    // Spread children are action calls with the spread template as parent
                    node_kind: NodeKind::Action as i32,
                    parent_execution_id: Some(spread_node_id.to_string()),
                    iteration_index: None,
                    execution_id: None, // Will be set by mark_running
                };

                self.graph.nodes.insert(node_id.clone(), exec_node);
                self.graph.ready_queue.push(node_id.clone());

                // Store the spread item as a variable for this iteration
                let var_name = format!("{}[{}].{}", spread_node_id, i, loop_var);
                let item_bytes = workflow_value_to_proto_bytes(&item);
                self.graph.variables.insert(var_name, item_bytes);

                node_id
            })
            .collect();

        // Update barrier to wait for all expanded nodes
        if let Some(barrier) = self.graph.nodes.get_mut(&barrier_id) {
            barrier.waiting_for = expanded_ids.clone();
            barrier.completed_count = 0;
        }

        debug!(
            spread_node = %spread_node_id,
            barrier = %barrier_id,
            count = expanded_ids.len(),
            "Expanded spread into concrete nodes"
        );

        expanded_ids
    }

    /// Check if there are any nodes still running or pending
    pub fn has_pending_work(&self) -> bool {
        self.graph.nodes.values().any(|n| {
            let status = NodeStatus::try_from(n.status).unwrap_or(NodeStatus::Unspecified);
            matches!(status, NodeStatus::Pending | NodeStatus::Running)
        }) || !self.graph.ready_queue.is_empty()
    }

    /// Get all nodes currently marked as running (for recovery after crash)
    pub fn get_running_nodes(&self) -> Vec<String> {
        self.graph
            .nodes
            .iter()
            .filter(|(_, n)| n.status == NodeStatus::Running as i32)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Reset running nodes to pending (for crash recovery)
    pub fn recover_running_nodes(&mut self) -> Vec<String> {
        let running_ids: Vec<String> = self.get_running_nodes();
        let mut updated = Vec::new();

        for node_id in running_ids {
            if let Some(node) = self.graph.nodes.get_mut(&node_id) {
                if node.worker_id.as_deref() == Some(SLEEP_WORKER_ID) {
                    debug!(
                        node_id = %node_id,
                        "Preserving durable sleep node during recovery"
                    );
                    continue;
                }
                // Check if we can retry
                if node.attempt_number < node.max_retries {
                    node.attempt_number += 1;
                    node.status = NodeStatus::Pending as i32;
                    node.worker_id = None;
                    node.started_at_ms = None;
                    self.graph.ready_queue.push(node_id.clone());
                    updated.push(node_id.clone());
                    debug!(node_id = %node_id, "Recovered running node to pending");
                } else {
                    node.status = NodeStatus::Exhausted as i32;
                    node.error = Some("Owner crashed, max retries exceeded".to_string());
                    updated.push(node_id.clone());
                    debug!(node_id = %node_id, "Running node exhausted retries");
                }
            }
        }

        updated
    }

    /// Find completed nodes whose successors haven't been advanced.
    ///
    /// This detects the "limbo state" where a node completed successfully but
    /// the runner crashed before `apply_completions_batch` could determine the
    /// next nodes. Returns synthetic [`Completion`] objects that can be
    /// re-processed through the normal completion flow to advance the workflow.
    pub fn find_stalled_completions(&self, dag: &DAG) -> Vec<Completion> {
        let helper = DAGHelper::new(dag);
        let ready_set: HashSet<&str> = self.graph.ready_queue.iter().map(|s| s.as_str()).collect();
        let mut stalled = Vec::new();

        for (node_id, node) in &self.graph.nodes {
            if node.status != NodeStatus::Completed as i32 {
                continue;
            }

            // Skip archived loop iteration nodes - they completed in a previous iteration
            // and their successors in the DAG now refer to fresh nodes for the next iteration.
            // Processing stalled completions for archived nodes would incorrectly add
            // duplicate attempts with 0ms duration.
            // Note: We check iteration_index specifically because parent_execution_id is also
            // set for spread children, which SHOULD be processed for stalled completions.
            if node.iteration_index.is_some() {
                continue;
            }

            // Check if any outgoing state-machine edge leads to a BLOCKED
            // successor that isn't already in the ready_queue.
            let has_stalled_successor = helper
                .get_state_machine_successors(&node.template_id)
                .iter()
                .any(|edge| {
                    if ready_set.contains(edge.target.as_str()) {
                        return false;
                    }
                    self.graph
                        .nodes
                        .get(&edge.target)
                        .map(|n| n.status == NodeStatus::Blocked as i32)
                        .unwrap_or(false)
                });

            if has_stalled_successor {
                debug!(
                    node_id = %node_id,
                    "Found stalled completion: node completed but successor still blocked"
                );
                stalled.push(Completion {
                    node_id: node_id.clone(),
                    success: true,
                    result: node.result.clone(),
                    error: None,
                    error_type: None,
                    worker_id: "recovery".to_string(),
                    duration_ms: 0,
                    worker_duration_ms: Some(0),
                });
            }
        }

        if !stalled.is_empty() {
            debug!(
                stalled_count = stalled.len(),
                "Found stalled completions for recovery"
            );
        }

        stalled
    }
}

impl Default for ExecutionState {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowState {
    pub fn rehydrate(
        instance: InstanceRehydrate,
        action_nodes_archive: &[ActionNodeArchive],
    ) -> Result<RehydrateResult, WorkflowStateError> {
        let inputs: WorkflowArguments = instance
            .input_payload
            .as_ref()
            .map(|b| decode_message(b))
            .transpose()?
            .unwrap_or_default();

        let mut inner = match instance.execution_graph.as_ref() {
            Some(bytes) => ExecutionState::from_bytes(bytes)?,
            None => ExecutionState::new(),
        };

        if instance.execution_graph.is_none() {
            inner.initialize_from_dag(&instance.dag, &inputs);
        }

        let mut state = WorkflowState {
            instance_id: instance.instance_id,
            dag: instance.dag,
            inner,
            updated_executions: HashMap::new(),
        };

        if instance.execution_graph.is_some() && !action_nodes_archive.is_empty() {
            let payloads: Vec<PayloadTuple> = action_nodes_archive
                .iter()
                .map(|archive| {
                    (
                        archive.execution_id.clone(),
                        archive.inputs.clone(),
                        archive.result.clone(),
                    )
                })
                .collect();
            state.inner.hydrate_payloads(&payloads);
        }

        state.inner.rehydrate_variables(&state.dag, &inputs);

        let recovered = state.inner.recover_running_nodes();
        for node_id in recovered {
            state.record_execution_update(&node_id);
        }

        let stalled = state.inner.find_stalled_completions(&state.dag);
        let mut update = if !stalled.is_empty() {
            state.apply_completions(stalled)
        } else {
            WorkflowStateUpdate::default()
        };

        let ready_snapshot = state.inner.peek_ready_queue();
        if !ready_snapshot.is_empty() {
            let ready_update = state.process_ready_nodes(ready_snapshot);
            update.merge(ready_update);
        }

        let completion = state.inner.check_workflow_completion(&state.dag);
        update.merge_completion(&completion);

        Ok(RehydrateResult { state, update })
    }

    pub fn finished_action(&mut self, update: ActionUpdate) -> WorkflowStateUpdate {
        match update {
            ActionUpdate::Started {
                node_id,
                worker_id,
                inputs,
            } => {
                self.inner.mark_running(&node_id, &worker_id, inputs);
                if let Some(node) = self.inner.graph.nodes.get_mut(&node_id)
                    && NodeKind::try_from(node.node_kind) == Ok(NodeKind::Sleep)
                    && node.started_at_ms.is_none()
                {
                    node.started_at_ms = Some(Utc::now().timestamp_millis());
                }
                self.record_execution_update(&node_id);
                WorkflowStateUpdate::default()
            }
            ActionUpdate::Completed(completion) => self.apply_completions(vec![completion]),
        }
    }

    pub fn get_updated_executions(&mut self) -> Vec<ActionExecutionUpdate> {
        std::mem::take(&mut self.updated_executions)
            .into_values()
            .collect()
    }

    pub fn get_state(&self) -> WorkflowStateSnapshot {
        let mut executions = HashMap::new();
        for (node_id, node) in &self.inner.graph.nodes {
            executions.insert(node_id.clone(), self.execution_metadata(node_id, node));
        }

        let completion = self.inner.check_workflow_completion(&self.dag);

        WorkflowStateSnapshot {
            instance_id: self.instance_id,
            ready_queue: self.inner.graph.ready_queue.clone(),
            next_wakeup_time: self.inner.graph.next_wakeup_time,
            executions,
            encoded_graph: self.inner.to_bytes_fully_stripped(),
            workflow_completed: completion.workflow_completed,
            workflow_failed: completion.workflow_failed,
            result_payload: completion.result_payload,
            error_message: completion.error_message,
        }
    }

    fn apply_completions(&mut self, completions: Vec<Completion>) -> WorkflowStateUpdate {
        let mut update = WorkflowStateUpdate::default();
        let mut completion_contexts: HashMap<String, (Option<String>, i32)> = HashMap::new();

        for completion in &completions {
            if let Some(payload) = self.capture_payload(completion) {
                update.payloads.push(payload);
            }
            if let Some(node) = self.inner.graph.nodes.get(&completion.node_id) {
                completion_contexts.insert(
                    completion.node_id.clone(),
                    (node.execution_id.clone(), node.attempt_number),
                );
            }
        }

        let result = self
            .inner
            .apply_completions_batch(completions.clone(), &self.dag);

        let now_ms = Utc::now().timestamp_millis();
        for completion in &completions {
            let Some((execution_id, attempt_number)) =
                completion_contexts.get(&completion.node_id).cloned()
            else {
                continue;
            };
            let Some(node) = self.inner.graph.nodes.get(&completion.node_id) else {
                continue;
            };
            let Some(exec_id) = execution_id.clone() else {
                continue;
            };

            let node_status = NodeStatus::try_from(node.status).unwrap_or(NodeStatus::Unspecified);
            let status = if completion.success {
                NodeStatus::Completed
            } else {
                match node_status {
                    NodeStatus::Caught => NodeStatus::Caught,
                    NodeStatus::Exhausted => NodeStatus::Exhausted,
                    NodeStatus::Pending => NodeStatus::Failed,
                    other => other,
                }
            };

            let started_at_ms = completion
                .worker_duration_ms
                .map(|wd| now_ms - wd)
                .or(node.started_at_ms);

            let exec_update = ActionExecutionUpdate {
                instance_id: self.instance_id,
                node_id: completion.node_id.clone(),
                action_id: node.template_id.clone(),
                execution_id: Some(exec_id.clone()),
                status,
                attempt_number,
                max_retries: node.max_retries,
                worker_id: Some(completion.worker_id.clone()),
                started_at_ms,
                completed_at_ms: Some(now_ms),
                duration_ms: Some(completion.duration_ms),
                parent_execution_id: node.parent_execution_id.clone(),
                spread_index: node.spread_index,
                loop_index: node.loop_index,
                waiting_for: node.waiting_for.clone(),
                completed_count: node.completed_count,
                node_kind: NodeKind::try_from(node.node_kind).unwrap_or(NodeKind::Unspecified),
                error: completion.error.clone(),
                error_type: completion.error_type.clone(),
                timeout_seconds: node.timeout_seconds,
                timeout_retry_limit: node.timeout_retry_limit,
                backoff: node.backoff.clone().unwrap_or_default(),
                sleep_wakeup_time_ms: self.sleep_wakeup_time_ms(node),
            };

            self.updated_executions.insert(exec_id, exec_update);

            if !completion.success
                && node_status == NodeStatus::Pending
                && let Some(node_mut) = self.inner.graph.nodes.get_mut(&completion.node_id)
            {
                node_mut.execution_id = None;
                node_mut.started_at_ms = None;
                node_mut.inputs = None;
            }
        }

        update.merge_completion(&result);

        if !result.newly_ready.is_empty() {
            let ready_update = self.process_ready_nodes(result.newly_ready);
            update.merge(ready_update);
        }

        update
    }

    fn process_ready_nodes(&mut self, node_ids: Vec<String>) -> WorkflowStateUpdate {
        let mut update = WorkflowStateUpdate::default();
        let mut queue: VecDeque<String> = node_ids.into();

        while let Some(node_id) = queue.pop_front() {
            let Some(exec_node) = self.inner.graph.nodes.get(&node_id) else {
                continue;
            };
            let node_kind =
                NodeKind::try_from(exec_node.node_kind).unwrap_or(NodeKind::Unspecified);
            let template_id = exec_node.template_id.clone();
            let Some(dag_node) = self.dag.nodes.get(&template_id).cloned() else {
                continue;
            };

            let is_worker_action = dag_node.module_name.is_some() && dag_node.action_name.is_some();

            if node_kind == NodeKind::Sleep {
                let inputs = self.inner.get_inputs_for_node(&node_id, &self.dag);
                update
                    .ready_actions
                    .push(self.build_ready_action(&node_id, exec_node, &dag_node, inputs));
                continue;
            }
            if node_kind == NodeKind::Action && is_worker_action {
                let inputs = self.inner.get_inputs_for_node(&node_id, &self.dag);
                update
                    .ready_actions
                    .push(self.build_ready_action(&node_id, exec_node, &dag_node, inputs));
                continue;
            }

            let inputs = self.inner.get_inputs_for_node(&node_id, &self.dag);
            self.inner
                .mark_running_no_queue_removal(&node_id, "inline", inputs.clone());
            self.inner.remove_from_ready_queue(&node_id);
            self.record_execution_update(&node_id);

            let completion = if node_kind == NodeKind::Spread {
                let mut completion_error = None;
                let items = if let Some(expr) = &dag_node.spread_collection_expr {
                    let scope = self.inner.build_scope_for_node(&node_id);
                    match ExpressionEvaluator::evaluate(expr, &scope) {
                        Ok(WorkflowValue::List(items)) | Ok(WorkflowValue::Tuple(items)) => items,
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
                } else {
                    completion_error =
                        Some("Spread node missing collection expression".to_string());
                    Vec::new()
                };

                if completion_error.is_none() {
                    let expanded = self.inner.expand_spread(&template_id, items, &self.dag);
                    for child_id in expanded {
                        queue.push_back(child_id);
                    }
                }

                Completion {
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
                }
            } else {
                let result = crate::inline_executor::execute_inline_node(
                    &mut self.inner,
                    &self.dag,
                    &node_id,
                    &dag_node,
                );

                Completion {
                    node_id: node_id.clone(),
                    success: result.is_ok(),
                    result: result.ok(),
                    error: None,
                    error_type: None,
                    worker_id: "inline".to_string(),
                    duration_ms: 0,
                    worker_duration_ms: Some(0),
                }
            };

            if let Some(payload) = self.capture_payload(&completion) {
                update.payloads.push(payload);
            }

            let result = self
                .inner
                .apply_completions_batch(vec![completion.clone()], &self.dag);
            self.record_execution_update(&completion.node_id);
            update.merge_completion(&result);

            if !result.newly_ready.is_empty() {
                for next in result.newly_ready {
                    queue.push_back(next);
                }
            }
        }

        update
    }

    fn build_ready_action(
        &self,
        node_id: &str,
        exec_node: &ExecutionNode,
        dag_node: &DAGNode,
        inputs: Option<Vec<u8>>,
    ) -> ReadyAction {
        ReadyAction {
            node_id: node_id.to_string(),
            action_id: exec_node.template_id.clone(),
            node_kind: NodeKind::try_from(exec_node.node_kind).unwrap_or(NodeKind::Unspecified),
            module_name: dag_node.module_name.clone(),
            action_name: dag_node.action_name.clone(),
            inputs,
            timeout_seconds: exec_node.timeout_seconds as u32,
            max_retries: exec_node.max_retries as u32,
            attempt_number: exec_node.attempt_number as u32,
        }
    }

    fn capture_payload(&self, completion: &Completion) -> Option<ActionPayload> {
        let node = self.inner.graph.nodes.get(&completion.node_id)?;
        let execution_id = node.execution_id.clone()?;
        Some(ActionPayload {
            execution_id,
            action_id: node.template_id.clone(),
            inputs: node.inputs.clone(),
            result: completion.result.clone(),
        })
    }

    fn record_execution_update(&mut self, node_id: &str) {
        let Some(node) = self.inner.graph.nodes.get(node_id) else {
            return;
        };
        let Some(execution_id) = node.execution_id.clone() else {
            return;
        };
        let update = self.execution_update(node_id, node);
        self.updated_executions.insert(execution_id, update);
    }

    fn execution_metadata(&self, node_id: &str, node: &ExecutionNode) -> ActionExecutionMetadata {
        ActionExecutionMetadata {
            node_id: node_id.to_string(),
            action_id: node.template_id.clone(),
            execution_id: node.execution_id.clone(),
            status: NodeStatus::try_from(node.status).unwrap_or(NodeStatus::Unspecified),
            attempt_number: node.attempt_number,
            max_retries: node.max_retries,
            worker_id: node.worker_id.clone(),
            started_at_ms: node.started_at_ms,
            completed_at_ms: node.completed_at_ms,
            duration_ms: node.duration_ms,
            parent_execution_id: node.parent_execution_id.clone(),
            spread_index: node.spread_index,
            loop_index: node.loop_index,
            waiting_for: node.waiting_for.clone(),
            completed_count: node.completed_count,
            node_kind: NodeKind::try_from(node.node_kind).unwrap_or(NodeKind::Unspecified),
            error: node.error.clone(),
            error_type: node.error_type.clone(),
            timeout_seconds: node.timeout_seconds,
            timeout_retry_limit: node.timeout_retry_limit,
            backoff: node.backoff.clone().unwrap_or_default(),
            sleep_wakeup_time_ms: self.sleep_wakeup_time_ms(node),
        }
    }

    fn execution_update(&self, node_id: &str, node: &ExecutionNode) -> ActionExecutionUpdate {
        ActionExecutionUpdate {
            instance_id: self.instance_id,
            node_id: node_id.to_string(),
            action_id: node.template_id.clone(),
            execution_id: node.execution_id.clone(),
            status: NodeStatus::try_from(node.status).unwrap_or(NodeStatus::Unspecified),
            attempt_number: node.attempt_number,
            max_retries: node.max_retries,
            worker_id: node.worker_id.clone(),
            started_at_ms: node.started_at_ms,
            completed_at_ms: node.completed_at_ms,
            duration_ms: node.duration_ms,
            parent_execution_id: node.parent_execution_id.clone(),
            spread_index: node.spread_index,
            loop_index: node.loop_index,
            waiting_for: node.waiting_for.clone(),
            completed_count: node.completed_count,
            node_kind: NodeKind::try_from(node.node_kind).unwrap_or(NodeKind::Unspecified),
            error: node.error.clone(),
            error_type: node.error_type.clone(),
            timeout_seconds: node.timeout_seconds,
            timeout_retry_limit: node.timeout_retry_limit,
            backoff: node.backoff.clone().unwrap_or_default(),
            sleep_wakeup_time_ms: self.sleep_wakeup_time_ms(node),
        }
    }

    fn sleep_wakeup_time_ms(&self, exec_node: &ExecutionNode) -> Option<i64> {
        let started_at_ms = exec_node.started_at_ms?;
        let inputs = decode_workflow_arguments(exec_node.inputs.as_deref());
        let duration_ms = sleep_duration_ms_from_args(&inputs);
        Some(started_at_ms + duration_ms)
    }
}

impl WorkflowStateUpdate {
    fn merge(&mut self, other: WorkflowStateUpdate) {
        self.ready_actions.extend(other.ready_actions);
        self.payloads.extend(other.payloads);
        if other.workflow_completed {
            self.workflow_completed = true;
            self.result_payload = other.result_payload.clone();
        }
        if other.workflow_failed {
            self.workflow_failed = true;
            self.error_message = other.error_message;
            self.result_payload = other.result_payload;
        }
    }

    fn merge_completion(&mut self, result: &BatchCompletionResult) {
        if result.workflow_completed {
            self.workflow_completed = true;
            self.result_payload = result.result_payload.clone();
        }
        if result.workflow_failed {
            self.workflow_failed = true;
            self.error_message = result.error_message.clone();
            self.result_payload = result.result_payload.clone();
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_state_roundtrip() {
        let mut state = ExecutionState::new();
        state.graph.ready_queue.push("test_node".to_string());
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        assert_eq!(recovered.graph.ready_queue, vec!["test_node".to_string()]);
        assert!(recovered.graph.nodes.contains_key("test_node"));
    }

    #[test]
    fn test_execution_graph_compression_roundtrip() {
        let raw = vec![0u8; 4096];
        let encoded = encode_execution_graph_bytes(&raw);
        assert!(encoded.starts_with(&EXECUTION_GRAPH_MAGIC));
        assert!(encoded.len() < raw.len());

        let decoded = decode_execution_graph_bytes(&encoded).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn test_execution_graph_decode_uncompressed_bytes() {
        let mut state = ExecutionState::new();
        state.graph.ready_queue.push("uncompressed".to_string());
        state.graph.nodes.insert(
            "uncompressed".to_string(),
            ExecutionNode {
                template_id: "uncompressed".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );

        let raw = state.graph.encode_to_vec();
        let recovered = ExecutionState::from_bytes(&raw).unwrap();
        assert_eq!(
            recovered.graph.ready_queue,
            vec!["uncompressed".to_string()]
        );
        assert!(recovered.graph.nodes.contains_key("uncompressed"));
    }

    #[test]
    fn test_execution_graph_decode_rejects_unknown_header() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&EXECUTION_GRAPH_MAGIC);
        bytes.push(EXECUTION_GRAPH_VERSION.wrapping_add(1));
        bytes.push(EXECUTION_GRAPH_CODEC_ZSTD);
        bytes.extend_from_slice(&0u64.to_le_bytes());

        let err = decode_execution_graph_bytes(&bytes).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("unknown execution graph compression header"));
    }

    #[test]
    fn test_mark_running() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        state.graph.ready_queue.push("test_node".to_string());

        state.mark_running("test_node", "worker-1", None);

        let node = state.graph.nodes.get("test_node").unwrap();
        assert_eq!(node.status, NodeStatus::Running as i32);
        assert_eq!(node.worker_id, Some("worker-1".to_string()));
        assert!(state.graph.ready_queue.is_empty());
    }

    #[test]
    fn test_drain_ready_queue() {
        let mut state = ExecutionState::new();
        state.graph.ready_queue.push("node_a".to_string());
        state.graph.ready_queue.push("node_b".to_string());

        let ready = state.drain_ready_queue();

        assert_eq!(ready, vec!["node_a".to_string(), "node_b".to_string()]);
        assert!(state.graph.ready_queue.is_empty());
    }

    #[test]
    fn test_has_pending_work() {
        let mut state = ExecutionState::new();
        assert!(!state.has_pending_work());

        // Add a pending node
        state.graph.nodes.insert(
            "pending_node".to_string(),
            ExecutionNode {
                template_id: "pending_node".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        assert!(state.has_pending_work());

        // Mark as completed
        state.graph.nodes.get_mut("pending_node").unwrap().status = NodeStatus::Completed as i32;
        assert!(!state.has_pending_work());

        // Add to ready queue
        state.graph.ready_queue.push("another".to_string());
        assert!(state.has_pending_work());
    }

    #[test]
    fn test_get_running_nodes() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "running_1".to_string(),
            ExecutionNode {
                template_id: "running_1".to_string(),
                status: NodeStatus::Running as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "completed_1".to_string(),
            ExecutionNode {
                template_id: "completed_1".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "running_2".to_string(),
            ExecutionNode {
                template_id: "running_2".to_string(),
                status: NodeStatus::Running as i32,
                ..Default::default()
            },
        );

        let running = state.get_running_nodes();
        assert_eq!(running.len(), 2);
        assert!(running.contains(&"running_1".to_string()));
        assert!(running.contains(&"running_2".to_string()));
    }

    #[test]
    fn test_recover_running_nodes_with_retries() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "running_node".to_string(),
            ExecutionNode {
                template_id: "running_node".to_string(),
                status: NodeStatus::Running as i32,
                attempt_number: 0,
                max_retries: 3,
                worker_id: Some("old-worker".to_string()),
                ..Default::default()
            },
        );

        state.recover_running_nodes();

        let node = state.graph.nodes.get("running_node").unwrap();
        assert_eq!(node.status, NodeStatus::Pending as i32);
        assert_eq!(node.attempt_number, 1);
        assert!(node.worker_id.is_none());
        assert!(
            state
                .graph
                .ready_queue
                .contains(&"running_node".to_string())
        );
    }

    #[test]
    fn test_recover_running_nodes_exhausted() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "exhausted_node".to_string(),
            ExecutionNode {
                template_id: "exhausted_node".to_string(),
                status: NodeStatus::Running as i32,
                attempt_number: 3, // Already at max
                max_retries: 3,
                ..Default::default()
            },
        );

        state.recover_running_nodes();

        let node = state.graph.nodes.get("exhausted_node").unwrap();
        assert_eq!(node.status, NodeStatus::Exhausted as i32);
        assert!(node.error.is_some());
        assert!(
            !state
                .graph
                .ready_queue
                .contains(&"exhausted_node".to_string())
        );
    }

    #[test]
    fn test_get_node_status() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Running as i32,
                ..Default::default()
            },
        );

        assert_eq!(
            state.get_node_status("test_node"),
            Some(NodeStatus::Running)
        );
        assert_eq!(state.get_node_status("nonexistent"), None);
    }

    #[test]
    fn test_variables_storage() {
        let mut state = ExecutionState::new();

        // Store a variable
        let value_bytes = vec![1, 2, 3, 4];
        state
            .graph
            .variables
            .insert("my_var".to_string(), value_bytes.clone());

        // Verify it's stored
        assert_eq!(state.graph.variables.get("my_var"), Some(&value_bytes));

        // Roundtrip through serialization
        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.graph.variables.get("my_var"), Some(&value_bytes));
    }

    #[test]
    fn test_exceptions_storage() {
        let mut state = ExecutionState::new();

        state.graph.exceptions.insert(
            "node_1".to_string(),
            ExceptionInfo {
                error_type: "ValueError".to_string(),
                error_module: "builtins".to_string(),
                error_message: "invalid value".to_string(),
                traceback: "line 1".to_string(),
                type_hierarchy: vec!["ValueError".to_string(), "Exception".to_string()],
                source_node_id: "node_1".to_string(),
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let exc = recovered.graph.exceptions.get("node_1").unwrap();
        assert_eq!(exc.error_type, "ValueError");
        assert_eq!(exc.error_message, "invalid value");
    }

    #[test]
    fn test_build_error_payload_serializes_exception() {
        let payload = ExecutionState::build_error_payload("boom");
        let args: WorkflowArguments = decode_message(&payload).expect("decode payload");
        let entry = args
            .arguments
            .iter()
            .find(|arg| arg.key == "error")
            .expect("error entry");
        let value = entry.value.as_ref().expect("error value");
        let value = WorkflowValue::from_proto(value);

        match value {
            WorkflowValue::Exception {
                exc_type,
                module,
                message,
                values,
                type_hierarchy,
                ..
            } => {
                assert_eq!(exc_type, "RuntimeError");
                assert_eq!(module, "builtins");
                assert_eq!(message, "boom");
                assert_eq!(
                    type_hierarchy,
                    vec![
                        "RuntimeError".to_string(),
                        "Exception".to_string(),
                        "BaseException".to_string(),
                    ]
                );
                match values.get("args") {
                    Some(WorkflowValue::Tuple(items)) => {
                        assert_eq!(items, &vec![WorkflowValue::String("boom".to_string())]);
                    }
                    other => panic!("expected args tuple, got {other:?}"),
                }
            }
            other => panic!("expected exception payload, got {other:?}"),
        }
    }

    #[test]
    fn test_attempt_record_storage() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Completed as i32,
                attempts: vec![
                    AttemptRecord {
                        attempt_number: 0,
                        worker_id: "worker-1".to_string(),
                        started_at_ms: 1000,
                        completed_at_ms: 2000,
                        duration_ms: 1000,
                        success: false,
                        result: None,
                        error: Some("timeout".to_string()),
                        error_type: Some("TimeoutError".to_string()),
                    },
                    AttemptRecord {
                        attempt_number: 1,
                        worker_id: "worker-2".to_string(),
                        started_at_ms: 3000,
                        completed_at_ms: 4000,
                        duration_ms: 1000,
                        success: true,
                        result: Some(vec![42]),
                        error: None,
                        error_type: None,
                    },
                ],
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let node = recovered.graph.nodes.get("test_node").unwrap();
        assert_eq!(node.attempts.len(), 2);
        assert!(!node.attempts[0].success);
        assert!(node.attempts[1].success);
    }

    #[test]
    fn test_spread_index_storage() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "spread_1[0]".to_string(),
            ExecutionNode {
                template_id: "spread_1".to_string(),
                spread_index: Some(0),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "spread_1[1]".to_string(),
            ExecutionNode {
                template_id: "spread_1".to_string(),
                spread_index: Some(1),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let node0 = recovered.graph.nodes.get("spread_1[0]").unwrap();
        let node1 = recovered.graph.nodes.get("spread_1[1]").unwrap();
        assert_eq!(node0.spread_index, Some(0));
        assert_eq!(node1.spread_index, Some(1));
        assert_eq!(node0.template_id, "spread_1");
        assert_eq!(node1.template_id, "spread_1");
    }

    #[test]
    fn test_waiting_for_tracking() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "barrier".to_string(),
            ExecutionNode {
                template_id: "barrier".to_string(),
                status: NodeStatus::Blocked as i32,
                waiting_for: vec![
                    "predecessor_1".to_string(),
                    "predecessor_2".to_string(),
                    "predecessor_3".to_string(),
                ],
                completed_count: 1,
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let barrier = recovered.graph.nodes.get("barrier").unwrap();
        assert_eq!(barrier.waiting_for.len(), 3);
        assert_eq!(barrier.completed_count, 1);
    }

    #[test]
    fn test_backoff_config_storage() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Pending as i32,
                max_retries: 5,
                timeout_seconds: 120,
                backoff: Some(BackoffConfig {
                    kind: BackoffKind::Exponential as i32,
                    base_delay_ms: 1000,
                    multiplier: 2.0,
                }),
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let node = recovered.graph.nodes.get("test_node").unwrap();
        assert_eq!(node.max_retries, 5);
        assert_eq!(node.timeout_seconds, 120);
        let backoff = node.backoff.as_ref().unwrap();
        assert_eq!(backoff.kind, BackoffKind::Exponential as i32);
        assert_eq!(backoff.base_delay_ms, 1000);
        assert_eq!(backoff.multiplier, 2.0);
    }

    /// Test crash recovery with partial dispatch.
    ///
    /// Simulates: 10 nodes ready, dispatch 2, crash, recover.
    /// All 10 nodes should be recoverable because:
    /// - We peek (not drain) the ready_queue
    /// - mark_running removes dispatched nodes from ready_queue
    /// - Undispatched nodes remain in ready_queue
    /// - Running nodes are recovered to Pending on crash recovery
    #[test]
    fn test_crash_recovery_partial_dispatch() {
        let mut state = ExecutionState::new();

        // Create 10 pending nodes and add them to ready_queue
        for i in 0..10 {
            let node_id = format!("action_{}", i);
            state.graph.nodes.insert(
                node_id.clone(),
                ExecutionNode {
                    template_id: node_id.clone(),
                    status: NodeStatus::Pending as i32,
                    max_retries: 3,
                    ..Default::default()
                },
            );
            state.graph.ready_queue.push(node_id);
        }

        assert_eq!(state.graph.ready_queue.len(), 10);

        // CORRECT PATTERN: Peek, don't drain
        let ready_nodes = state.peek_ready_queue();
        assert_eq!(ready_nodes.len(), 10);
        // ready_queue is STILL intact
        assert_eq!(state.graph.ready_queue.len(), 10);

        // Dispatch only 2 nodes - mark_running removes them from ready_queue
        state.mark_running("action_0", "worker-0", None);
        state.mark_running("action_1", "worker-1", None);

        // Now ready_queue has 8 (the 2 dispatched were removed by mark_running)
        assert_eq!(state.graph.ready_queue.len(), 8);

        // Sync to DB
        let bytes = state.to_bytes();

        // CRASH and recover
        let mut recovered = ExecutionState::from_bytes(&bytes).unwrap();

        // Before recovery: 8 in ready_queue, 2 Running
        assert_eq!(recovered.graph.ready_queue.len(), 8);

        // Run recovery - resets Running nodes to Pending
        recovered.recover_running_nodes();

        // After recovery: ALL 10 should be in ready_queue
        // - 8 that were never dispatched (still in ready_queue)
        // - 2 that were Running -> recovered to Pending -> added to ready_queue
        assert_eq!(
            recovered.graph.ready_queue.len(),
            10,
            "All nodes should be recoverable"
        );

        // No orphaned Pending nodes
        let pending_not_in_queue: Vec<_> = recovered
            .graph
            .nodes
            .iter()
            .filter(|(node_id, node)| {
                node.status == NodeStatus::Pending as i32
                    && !recovered.graph.ready_queue.contains(*node_id)
            })
            .collect();

        assert!(
            pending_not_in_queue.is_empty(),
            "No Pending nodes should be orphaned"
        );
    }

    /// Helper to build a minimal DAG with sequential nodes connected by state-machine edges.
    fn build_sequential_dag(node_ids: &[&str]) -> DAG {
        use crate::dag::{DAGEdge, DAGNode};

        let mut nodes = HashMap::new();
        let mut edges = Vec::new();

        for (i, id) in node_ids.iter().enumerate() {
            let is_first = i == 0;
            let is_last = i == node_ids.len() - 1;
            let node_type = if is_first {
                "input"
            } else if is_last {
                "output"
            } else {
                "action_call"
            };

            let mut node = DAGNode::new(id.to_string(), node_type.to_string(), id.to_string());
            node.function_name = Some("run".to_string());
            node.is_input = is_first;
            node.is_output = is_last;
            if !is_first && !is_last {
                node.action_name = Some(format!("action_{}", id));
                node.module_name = Some("test_module".to_string());
            }
            node.target = Some("result".to_string());
            nodes.insert(id.to_string(), node);

            if i > 0 {
                edges.push(DAGEdge::state_machine(
                    node_ids[i - 1].to_string(),
                    id.to_string(),
                ));
            }
        }

        DAG {
            nodes,
            edges,
            entry_node: Some(node_ids[0].to_string()),
        }
    }

    #[test]
    fn test_find_stalled_completions_detects_limbo() {
        // Simulate the limbo state: node_b completed but successor node_c is still BLOCKED
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Completed as i32,
                result: Some(vec![1, 2, 3]),
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );
        // Ready queue is empty - simulating crash after completion but before successor computation
        assert!(state.graph.ready_queue.is_empty());

        let stalled = state.find_stalled_completions(&dag);

        // node_b should be identified as stalled (completed with blocked successor)
        assert_eq!(stalled.len(), 1);
        assert_eq!(stalled[0].node_id, "node_b");
        assert!(stalled[0].success);
        assert_eq!(stalled[0].result, Some(vec![1, 2, 3]));
        assert_eq!(stalled[0].worker_id, "recovery");
    }

    #[test]
    fn test_find_stalled_completions_no_stall_when_successor_already_ready() {
        // node_b completed and node_c is already PENDING in the ready_queue - no stall
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        state.graph.ready_queue.push("node_c".to_string());

        let stalled = state.find_stalled_completions(&dag);
        assert!(stalled.is_empty());
    }

    #[test]
    fn test_find_stalled_completions_ignores_running_nodes() {
        // node_b is RUNNING - this should be handled by recover_running_nodes, not us
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Running as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        let stalled = state.find_stalled_completions(&dag);
        assert!(stalled.is_empty());
    }

    #[test]
    fn test_find_stalled_completions_no_stall_when_all_successors_advanced() {
        // Both node_b and node_c completed - no blocked successors
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );

        let stalled = state.find_stalled_completions(&dag);
        assert!(stalled.is_empty());
    }

    #[test]
    fn test_stalled_completions_are_processable() {
        // Verify that stalled completions can be processed through apply_completions_batch
        // to advance the workflow
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Completed as i32,
                result: Some(vec![1, 2, 3]),
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        let stalled = state.find_stalled_completions(&dag);
        assert_eq!(stalled.len(), 1);

        // Process the stalled completions
        let result = state.apply_completions_batch(stalled, &dag);

        // node_c should now be PENDING in the ready_queue
        assert!(
            result.newly_ready.contains(&"node_c".to_string()),
            "node_c should be newly ready after recovery"
        );
        let node_c = state.graph.nodes.get("node_c").unwrap();
        assert_eq!(node_c.status, NodeStatus::Pending as i32);
        assert!(state.graph.ready_queue.contains(&"node_c".to_string()));
    }

    #[test]
    fn test_stalled_completions_roundtrip_recovery() {
        // Full roundtrip: create limbo state, serialize, deserialize, recover
        let dag = build_sequential_dag(&["input", "action", "output"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "input".to_string(),
            ExecutionNode {
                template_id: "input".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "action".to_string(),
            ExecutionNode {
                template_id: "action".to_string(),
                status: NodeStatus::Completed as i32,
                result: Some(vec![42]),
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "output".to_string(),
            ExecutionNode {
                template_id: "output".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        // Simulate crash: serialize to bytes (as if persisted to DB)
        let bytes = state.to_bytes();

        // Recovery: deserialize
        let mut recovered = ExecutionState::from_bytes(&bytes).unwrap();

        // recover_running_nodes finds nothing (no RUNNING nodes)
        recovered.recover_running_nodes();
        assert!(recovered.graph.ready_queue.is_empty());

        // find_stalled_completions detects the limbo
        let stalled = recovered.find_stalled_completions(&dag);
        assert_eq!(stalled.len(), 1);
        assert_eq!(stalled[0].node_id, "action");

        // Processing the stalled completions advances the workflow
        let result = recovered.apply_completions_batch(stalled, &dag);
        assert!(
            result.newly_ready.contains(&"output".to_string()),
            "output node should be ready after recovery"
        );
    }

    #[test]
    fn test_find_stalled_completions_skips_archived_loop_iterations() {
        // Test that archived loop iteration nodes (with iteration_index set) are skipped
        // by find_stalled_completions. This prevents phantom 0ms entries.
        use crate::dag::{DAGEdge, DAGNode};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        let mut edges = Vec::new();

        // Create a simple DAG with action_1 -> action_2
        let action_1 = DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "action_1".to_string(),
        );
        nodes.insert("action_1".to_string(), action_1);

        let action_2 = DAGNode::new(
            "action_2".to_string(),
            "action_call".to_string(),
            "action_2".to_string(),
        );
        nodes.insert("action_2".to_string(), action_2);

        edges.push(DAGEdge::state_machine(
            "action_1".to_string(),
            "action_2".to_string(),
        ));

        let dag = DAG {
            nodes,
            edges,
            entry_node: Some("action_1".to_string()),
        };

        // Create execution state with:
        // - action_1[iter_0]: archived, Completed (with iteration_index set)
        // - action_1: fresh node, Blocked
        // - action_2: Blocked
        let mut state = ExecutionState::new();

        // Archived iteration node - Completed with iteration_index
        state.graph.nodes.insert(
            "action_1[iter_0]".to_string(),
            ExecutionNode {
                template_id: "action_1".to_string(),
                status: NodeStatus::Completed as i32,
                started_at_ms: Some(1000),
                completed_at_ms: Some(2000),
                duration_ms: Some(1000),
                iteration_index: Some(0),
                parent_execution_id: Some("action_1".to_string()),
                ..Default::default()
            },
        );

        // Fresh node for next iteration - Blocked
        state.graph.nodes.insert(
            "action_1".to_string(),
            ExecutionNode {
                template_id: "action_1".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        // Successor node - Blocked
        state.graph.nodes.insert(
            "action_2".to_string(),
            ExecutionNode {
                template_id: "action_2".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        // find_stalled_completions should NOT find action_1[iter_0] as stalled
        // because it has iteration_index set (it's an archived loop iteration)
        let stalled = state.find_stalled_completions(&dag);
        assert!(
            stalled.is_empty(),
            "Should not find stalled completions for archived loop iteration nodes"
        );
    }

    #[test]
    fn test_parallel_join_completion() {
        // Test that an output node becomes ready when multiple parallel branches complete
        // This tests a scenario that could cause stalls if the join logic doesn't work correctly
        use crate::dag::{DAGEdge, DAGNode};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        let mut edges = Vec::new();

        // Input node
        let mut input = DAGNode::new(
            "input".to_string(),
            "input".to_string(),
            "input".to_string(),
        );
        input.is_input = true;
        input.function_name = Some("main".to_string());
        nodes.insert("input".to_string(), input);

        // Two parallel actions
        let mut action_a = DAGNode::new(
            "action_a".to_string(),
            "fn_call".to_string(),
            "action_a".to_string(),
        );
        action_a.module_name = Some("test".to_string());
        action_a.action_name = Some("do_a".to_string());
        action_a.function_name = Some("main".to_string());
        nodes.insert("action_a".to_string(), action_a);

        let mut action_b = DAGNode::new(
            "action_b".to_string(),
            "fn_call".to_string(),
            "action_b".to_string(),
        );
        action_b.module_name = Some("test".to_string());
        action_b.action_name = Some("do_b".to_string());
        action_b.function_name = Some("main".to_string());
        nodes.insert("action_b".to_string(), action_b);

        // Join node that requires both branches
        let mut join = DAGNode::new("join".to_string(), "join".to_string(), "join".to_string());
        join.function_name = Some("main".to_string());
        nodes.insert("join".to_string(), join);

        // Output node
        let mut output = DAGNode::new(
            "output".to_string(),
            "output".to_string(),
            "output".to_string(),
        );
        output.is_output = true;
        output.function_name = Some("main".to_string());
        nodes.insert("output".to_string(), output);

        // Edges: input -> action_a, input -> action_b, action_a -> join, action_b -> join, join -> output
        edges.push(DAGEdge::state_machine(
            "input".to_string(),
            "action_a".to_string(),
        ));
        edges.push(DAGEdge::state_machine(
            "input".to_string(),
            "action_b".to_string(),
        ));
        edges.push(DAGEdge::state_machine(
            "action_a".to_string(),
            "join".to_string(),
        ));
        edges.push(DAGEdge::state_machine(
            "action_b".to_string(),
            "join".to_string(),
        ));
        edges.push(DAGEdge::state_machine(
            "join".to_string(),
            "output".to_string(),
        ));

        let dag = DAG {
            nodes,
            edges,
            entry_node: Some("input".to_string()),
        };

        // Initialize execution state
        let mut state = ExecutionState::new();
        state.initialize_from_dag(&dag, &WorkflowArguments::default());

        // Simulate input completing
        let input_completion = Completion {
            node_id: "input".to_string(),
            success: true,
            result: None,
            error: None,
            error_type: None,
            worker_id: "test".to_string(),
            duration_ms: 0,
            worker_duration_ms: Some(0),
        };
        let result = state.apply_completions_batch(vec![input_completion], &dag);
        assert!(
            result.newly_ready.contains(&"action_a".to_string()),
            "action_a should be ready"
        );
        assert!(
            result.newly_ready.contains(&"action_b".to_string()),
            "action_b should be ready"
        );

        // Mark actions as running (simulating dispatch)
        state.mark_running("action_a", "worker1", None);
        state.mark_running("action_b", "worker2", None);

        // Complete action_a
        let action_a_completion = Completion {
            node_id: "action_a".to_string(),
            success: true,
            result: Some(vec![1, 2, 3]),
            error: None,
            error_type: None,
            worker_id: "worker1".to_string(),
            duration_ms: 100,
            worker_duration_ms: Some(100),
        };
        let result = state.apply_completions_batch(vec![action_a_completion], &dag);
        // Join shouldn't be ready yet - needs action_b
        assert!(
            !result.newly_ready.contains(&"join".to_string()),
            "join should NOT be ready yet (needs action_b)"
        );

        // Complete action_b
        let action_b_completion = Completion {
            node_id: "action_b".to_string(),
            success: true,
            result: Some(vec![4, 5, 6]),
            error: None,
            error_type: None,
            worker_id: "worker2".to_string(),
            duration_ms: 150,
            worker_duration_ms: Some(150),
        };
        let result = state.apply_completions_batch(vec![action_b_completion], &dag);
        // Now join should be ready
        assert!(
            result.newly_ready.contains(&"join".to_string()),
            "join should be ready after both branches complete"
        );

        // Process join as inline node (simulate what collect_ready_actions does)
        let join_node = state.graph.nodes.get_mut("join").unwrap();
        join_node.status = NodeStatus::Completed as i32;
        state.remove_from_ready_queue("join");

        // Complete join
        let join_completion = Completion {
            node_id: "join".to_string(),
            success: true,
            result: None,
            error: None,
            error_type: None,
            worker_id: "inline".to_string(),
            duration_ms: 0,
            worker_duration_ms: Some(0),
        };
        let result = state.apply_completions_batch(vec![join_completion], &dag);
        assert!(
            result.newly_ready.contains(&"output".to_string()),
            "output should be ready after join completes"
        );

        // Process output as inline node
        let output_node = state.graph.nodes.get_mut("output").unwrap();
        output_node.status = NodeStatus::Completed as i32;
        state.remove_from_ready_queue("output");

        // Complete output and check workflow completion
        let output_completion = Completion {
            node_id: "output".to_string(),
            success: true,
            result: None,
            error: None,
            error_type: None,
            worker_id: "inline".to_string(),
            duration_ms: 0,
            worker_duration_ms: Some(0),
        };
        let result = state.apply_completions_batch(vec![output_completion], &dag);
        assert!(
            result.workflow_completed,
            "workflow should be completed after output node completes"
        );
    }

    #[test]
    fn test_stalled_instance_detection_no_pending_work() {
        // Test the scenario where an instance has:
        // 1. No pending work (has_pending_work() returns false)
        // 2. No in-flight actions
        // 3. Workflow is not complete
        // This is the "stall" scenario that keeps the instance owned but making no progress
        use crate::dag::{DAGEdge, DAGNode};
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        let mut edges = Vec::new();

        // Simple workflow: input -> action -> output
        let mut input = DAGNode::new(
            "input".to_string(),
            "input".to_string(),
            "input".to_string(),
        );
        input.is_input = true;
        input.function_name = Some("main".to_string());
        nodes.insert("input".to_string(), input);

        let mut action = DAGNode::new(
            "action".to_string(),
            "fn_call".to_string(),
            "action".to_string(),
        );
        action.module_name = Some("test".to_string());
        action.action_name = Some("do_work".to_string());
        action.function_name = Some("main".to_string());
        nodes.insert("action".to_string(), action);

        let mut output = DAGNode::new(
            "output".to_string(),
            "output".to_string(),
            "output".to_string(),
        );
        output.is_output = true;
        output.function_name = Some("main".to_string());
        nodes.insert("output".to_string(), output);

        edges.push(DAGEdge::state_machine(
            "input".to_string(),
            "action".to_string(),
        ));
        edges.push(DAGEdge::state_machine(
            "action".to_string(),
            "output".to_string(),
        ));

        let dag = DAG {
            nodes,
            edges,
            entry_node: Some("input".to_string()),
        };

        // Create a state where action is Completed but output is still Blocked
        // This simulates a crash between action completing and output being advanced
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "input".to_string(),
            ExecutionNode {
                template_id: "input".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "action".to_string(),
            ExecutionNode {
                template_id: "action".to_string(),
                status: NodeStatus::Completed as i32,
                result: Some(vec![1, 2, 3]),
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "output".to_string(),
            ExecutionNode {
                template_id: "output".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        // Verify the state conditions for stall detection
        assert!(
            !state.has_pending_work(),
            "Should have no pending work (no Pending/Running nodes, empty ready_queue)"
        );
        assert!(
            state.graph.ready_queue.is_empty(),
            "Ready queue should be empty"
        );

        // Check workflow completion - should NOT be complete yet
        let status = state.check_workflow_completion(&dag);
        assert!(
            !status.workflow_completed,
            "Workflow should NOT be complete (output is Blocked)"
        );
        assert!(!status.workflow_failed, "Workflow should NOT be failed");

        // Find stalled completions - should find the action node
        let stalled = state.find_stalled_completions(&dag);
        assert_eq!(
            stalled.len(),
            1,
            "Should find 1 stalled completion (action has blocked successor)"
        );
        assert_eq!(
            stalled[0].node_id, "action",
            "Stalled completion should be for action node"
        );

        // Apply the stalled completion to advance the workflow
        let result = state.apply_completions_batch(stalled, &dag);
        assert!(
            result.newly_ready.contains(&"output".to_string()),
            "Output should become ready after reprocessing stalled completion"
        );

        // Now the output should be in ready_queue
        assert!(
            state.graph.ready_queue.contains(&"output".to_string()),
            "Output should be in ready_queue"
        );
        assert!(
            state.has_pending_work(),
            "Should now have pending work (output in ready_queue)"
        );
    }

    // =========================================================================
    // NodeKind Tests
    // =========================================================================

    #[test]
    fn test_node_kind_action_call() {
        let dag_node = DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "call action".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Action);
    }

    #[test]
    fn test_node_kind_sleep() {
        let mut dag_node = DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "sleep".to_string(),
        );
        dag_node.action_name = Some("sleep".to_string());
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Sleep);
    }

    #[test]
    fn test_node_kind_aggregator() {
        let mut dag_node = DAGNode::new(
            "agg_1".to_string(),
            "action_call".to_string(),
            "aggregator".to_string(),
        );
        dag_node.is_aggregator = true;
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Aggregator);
    }

    #[test]
    fn test_node_kind_spread() {
        let mut dag_node = DAGNode::new(
            "spread_1".to_string(),
            "action_call".to_string(),
            "spread".to_string(),
        );
        dag_node.is_spread = true;
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Spread);
    }

    #[test]
    fn test_node_kind_assignment() {
        let dag_node = DAGNode::new(
            "assign_1".to_string(),
            "assignment".to_string(),
            "x = 1".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Assignment);
    }

    #[test]
    fn test_node_kind_branch() {
        let dag_node = DAGNode::new(
            "branch_1".to_string(),
            "branch".to_string(),
            "if x".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Branch);
    }

    #[test]
    fn test_node_kind_join() {
        let dag_node = DAGNode::new("join_1".to_string(), "join".to_string(), "join".to_string());
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Join);
    }

    #[test]
    fn test_node_kind_fn_call() {
        let dag_node = DAGNode::new(
            "fn_call_1".to_string(),
            "fn_call".to_string(),
            "call fn".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::FnCall);
    }

    #[test]
    fn test_node_kind_return() {
        let dag_node = DAGNode::new(
            "return_1".to_string(),
            "return".to_string(),
            "return".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Return);
    }

    #[test]
    fn test_node_kind_break() {
        let dag_node = DAGNode::new(
            "break_1".to_string(),
            "break".to_string(),
            "break".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Break);
    }

    #[test]
    fn test_node_kind_input() {
        let dag_node = DAGNode::new(
            "input".to_string(),
            "input".to_string(),
            "input".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Input);
    }

    #[test]
    fn test_node_kind_output() {
        let dag_node = DAGNode::new(
            "output".to_string(),
            "output".to_string(),
            "output".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Output);
    }

    #[test]
    fn test_node_kind_unknown() {
        let dag_node = DAGNode::new(
            "unknown_1".to_string(),
            "unknown_type".to_string(),
            "unknown".to_string(),
        );
        assert_eq!(determine_node_kind(&dag_node), NodeKind::Unspecified);
    }

    #[test]
    fn test_node_kind_storage_roundtrip() {
        // Test that node_kind is properly stored and recovered
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "action_1".to_string(),
            ExecutionNode {
                template_id: "action_1".to_string(),
                status: NodeStatus::Pending as i32,
                node_kind: NodeKind::Action as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "sleep_1".to_string(),
            ExecutionNode {
                template_id: "sleep_1".to_string(),
                status: NodeStatus::Pending as i32,
                node_kind: NodeKind::Sleep as i32,
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let action = recovered.graph.nodes.get("action_1").unwrap();
        let sleep = recovered.graph.nodes.get("sleep_1").unwrap();
        assert_eq!(action.node_kind, NodeKind::Action as i32);
        assert_eq!(sleep.node_kind, NodeKind::Sleep as i32);
    }

    #[test]
    fn test_parent_execution_id_storage_roundtrip() {
        // Test that parent_execution_id is properly stored and recovered
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "spread_1[0]".to_string(),
            ExecutionNode {
                template_id: "spread_1".to_string(),
                spread_index: Some(0),
                status: NodeStatus::Pending as i32,
                node_kind: NodeKind::Action as i32,
                parent_execution_id: Some("spread_1".to_string()),
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let node = recovered.graph.nodes.get("spread_1[0]").unwrap();
        assert_eq!(node.parent_execution_id, Some("spread_1".to_string()));
    }

    #[test]
    fn test_iteration_index_storage_roundtrip() {
        // Test that iteration_index is properly stored and recovered
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "action_1[iter_0]".to_string(),
            ExecutionNode {
                template_id: "action_1".to_string(),
                status: NodeStatus::Completed as i32,
                node_kind: NodeKind::Action as i32,
                parent_execution_id: Some("action_1".to_string()),
                iteration_index: Some(0),
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "action_1[iter_1]".to_string(),
            ExecutionNode {
                template_id: "action_1".to_string(),
                status: NodeStatus::Completed as i32,
                node_kind: NodeKind::Action as i32,
                parent_execution_id: Some("action_1".to_string()),
                iteration_index: Some(1),
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let iter0 = recovered.graph.nodes.get("action_1[iter_0]").unwrap();
        let iter1 = recovered.graph.nodes.get("action_1[iter_1]").unwrap();
        assert_eq!(iter0.iteration_index, Some(0));
        assert_eq!(iter0.parent_execution_id, Some("action_1".to_string()));
        assert_eq!(iter1.iteration_index, Some(1));
        assert_eq!(iter1.parent_execution_id, Some("action_1".to_string()));
    }

    #[test]
    fn test_spread_expansion_sets_parent_and_kind() {
        // Build a minimal DAG with a spread node
        let mut dag = DAG {
            nodes: std::collections::HashMap::new(),
            edges: vec![],
            entry_node: Some("spread_1".to_string()),
        };

        let mut spread_node = DAGNode::new(
            "spread_1".to_string(),
            "action_call".to_string(),
            "spread".to_string(),
        );
        spread_node.is_spread = true;
        spread_node.spread_loop_var = Some("item".to_string());
        spread_node.aggregates_to = Some("agg_1".to_string());
        dag.nodes.insert("spread_1".to_string(), spread_node);

        let mut agg_node = DAGNode::new(
            "agg_1".to_string(),
            "action_call".to_string(),
            "aggregator".to_string(),
        );
        agg_node.is_aggregator = true;
        dag.nodes.insert("agg_1".to_string(), agg_node);

        // Initialize execution state
        let mut state = ExecutionState::new();
        state.initialize_from_dag(&dag, &WorkflowArguments::default());

        // Verify spread template has Spread kind
        let spread_exec = state.graph.nodes.get("spread_1").unwrap();
        assert_eq!(spread_exec.node_kind, NodeKind::Spread as i32);

        // Expand the spread
        let items = vec![
            WorkflowValue::Int(1),
            WorkflowValue::Int(2),
            WorkflowValue::Int(3),
        ];
        let _ = state.expand_spread("spread_1", items, &dag);

        // Verify expanded nodes have Action kind and parent set
        for i in 0..3 {
            let child_id = format!("spread_1[{}]", i);
            let child = state.graph.nodes.get(&child_id).unwrap();
            assert_eq!(
                child.node_kind,
                NodeKind::Action as i32,
                "Spread child should have Action kind"
            );
            assert_eq!(
                child.parent_execution_id,
                Some("spread_1".to_string()),
                "Spread child should have parent_execution_id set"
            );
            assert_eq!(
                child.spread_index,
                Some(i),
                "Spread child should have spread_index set"
            );
        }
    }

    #[test]
    fn test_initialize_from_dag_sets_node_kind() {
        // Build a DAG with various node types
        let mut dag = DAG {
            nodes: std::collections::HashMap::new(),
            edges: vec![],
            entry_node: Some("input".to_string()),
        };

        let input_node = DAGNode::new(
            "input".to_string(),
            "input".to_string(),
            "input".to_string(),
        );
        dag.nodes.insert("input".to_string(), input_node);

        let mut action_node = DAGNode::new(
            "action_1".to_string(),
            "action_call".to_string(),
            "call action".to_string(),
        );
        action_node.module_name = Some("my_module".to_string());
        action_node.action_name = Some("my_action".to_string());
        dag.nodes.insert("action_1".to_string(), action_node);

        let mut sleep_node = DAGNode::new(
            "sleep_1".to_string(),
            "action_call".to_string(),
            "sleep".to_string(),
        );
        sleep_node.action_name = Some("sleep".to_string());
        dag.nodes.insert("sleep_1".to_string(), sleep_node);

        let output_node = DAGNode::new(
            "output".to_string(),
            "output".to_string(),
            "output".to_string(),
        );
        dag.nodes.insert("output".to_string(), output_node);

        // Initialize execution state
        let mut state = ExecutionState::new();
        state.initialize_from_dag(&dag, &WorkflowArguments::default());

        // Verify node kinds are set correctly
        let input_exec = state.graph.nodes.get("input").unwrap();
        assert_eq!(input_exec.node_kind, NodeKind::Input as i32);

        let action_exec = state.graph.nodes.get("action_1").unwrap();
        assert_eq!(action_exec.node_kind, NodeKind::Action as i32);

        let sleep_exec = state.graph.nodes.get("sleep_1").unwrap();
        assert_eq!(sleep_exec.node_kind, NodeKind::Sleep as i32);

        let output_exec = state.graph.nodes.get("output").unwrap();
        assert_eq!(output_exec.node_kind, NodeKind::Output as i32);
    }
}
