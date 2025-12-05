//! Completion plan and unified readiness model types.
//!
//! This module contains the data structures used for the unified readiness model
//! where every node gets readiness tracking and is only enqueued when
//! `completed_count == required_count`.

use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{debug, warn};

use uuid::Uuid;

use crate::ast_evaluator::ExpressionEvaluator;
use crate::dag::{DAG, DAGNode, EdgeType};
use crate::dag_state::DAGHelper;
use crate::db::{ActionId, WorkflowInstanceId};
use crate::parser::ast;

// ============================================================================
// Subgraph Analysis Types
// ============================================================================

/// Result of analyzing the DAG subgraph reachable from a completed node.
#[derive(Debug, Default)]
pub struct SubgraphAnalysis {
    /// Nodes we can execute inline (assignments, expressions, control flow).
    /// Ordered for topological traversal.
    pub inline_nodes: Vec<String>,

    /// Frontier nodes where traversal stops (actions, barriers, output).
    /// These need inbox data written to them and readiness tracking.
    pub frontier_nodes: Vec<FrontierNode>,

    /// All node IDs in the subgraph (for batch inbox fetch).
    pub all_node_ids: HashSet<String>,
}

/// A frontier node where inline traversal stops.
#[derive(Debug, Clone)]
pub struct FrontierNode {
    /// Node ID in the DAG.
    pub node_id: String,

    /// Category determining how this node is handled.
    pub category: FrontierCategory,

    /// Number of StateMachine predecessors (for readiness tracking).
    pub required_count: i32,
}

/// Categories of frontier nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierCategory {
    /// External action dispatched to Python worker.
    Action,
    /// Barrier waiting for multiple predecessors (aggregator, parallel join).
    Barrier,
    /// Workflow completion (output/return node).
    Output,
}

// ============================================================================
// Completion Plan Types
// ============================================================================

/// A complete plan for what to write to the database when a node completes.
///
/// This is computed entirely in memory before any database writes occur.
/// The entire plan is then executed in a single atomic transaction.
#[derive(Debug)]
pub struct CompletionPlan {
    /// The node that completed.
    pub completed_node_id: String,

    /// Action ID (for action nodes).
    pub completed_action_id: Option<ActionId>,

    /// Delivery token for idempotent completion.
    pub delivery_token: Option<Uuid>,

    /// Whether the action succeeded.
    pub success: bool,

    /// Result payload from the worker.
    pub result_payload: Vec<u8>,

    /// Error message if failed.
    pub error_message: Option<String>,

    /// Inbox writes for frontier nodes.
    pub inbox_writes: Vec<InboxWrite>,

    /// Readiness increments for frontier nodes.
    pub readiness_increments: Vec<ReadinessIncrement>,

    /// Workflow instance completion (if output node reached).
    pub instance_completion: Option<InstanceCompletion>,
}

impl CompletionPlan {
    /// Create a new empty completion plan.
    pub fn new(completed_node_id: String) -> Self {
        Self {
            completed_node_id,
            completed_action_id: None,
            delivery_token: None,
            success: true,
            result_payload: Vec::new(),
            error_message: None,
            inbox_writes: Vec::new(),
            readiness_increments: Vec::new(),
            instance_completion: None,
        }
    }

    /// Set action completion details.
    pub fn with_action_completion(
        mut self,
        action_id: ActionId,
        delivery_token: Uuid,
        success: bool,
        result_payload: Vec<u8>,
        error_message: Option<String>,
    ) -> Self {
        self.completed_action_id = Some(action_id);
        self.delivery_token = Some(delivery_token);
        self.success = success;
        self.result_payload = result_payload;
        self.error_message = error_message;
        self
    }
}

/// A pending write to a node's inbox.
#[derive(Debug, Clone)]
pub struct InboxWrite {
    pub instance_id: WorkflowInstanceId,
    pub target_node_id: String,
    pub variable_name: String,
    pub value: JsonValue,
    pub source_node_id: String,
    pub spread_index: Option<i32>,
}

/// A readiness increment for a frontier node.
#[derive(Debug, Clone)]
pub struct ReadinessIncrement {
    /// The frontier node to increment readiness for.
    pub node_id: String,

    /// Number of StateMachine predecessors this node has.
    pub required_count: i32,

    /// Node type to use when enqueueing (action or barrier).
    pub node_type: NodeType,

    /// Module name (for action nodes).
    pub module_name: Option<String>,

    /// Action name (for action nodes).
    pub action_name: Option<String>,

    /// Dispatch payload (for action nodes).
    pub dispatch_payload: Option<Vec<u8>>,
}

/// Type of node for the action queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Action,
    Barrier,
}

impl NodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeType::Action => "action",
            NodeType::Barrier => "barrier",
        }
    }
}

/// Workflow instance completion details.
#[derive(Debug, Clone)]
pub struct InstanceCompletion {
    pub instance_id: WorkflowInstanceId,
    pub result_payload: Vec<u8>,
}

// ============================================================================
// Completion Result
// ============================================================================

/// Result of executing a completion plan.
#[derive(Debug, Default)]
pub struct CompletionResult {
    /// Whether this was a stale/duplicate completion.
    pub was_stale: bool,

    /// Nodes that became ready and were enqueued.
    pub newly_ready_nodes: Vec<String>,

    /// Whether the workflow completed.
    pub workflow_completed: bool,
}

impl CompletionResult {
    /// Create a stale completion result.
    pub fn stale() -> Self {
        Self {
            was_stale: true,
            ..Default::default()
        }
    }
}

// ============================================================================
// Inline Scope
// ============================================================================

/// Scope for inline node execution.
/// Maps variable names to their JSON values.
pub type InlineScope = HashMap<String, JsonValue>;

// ============================================================================
// Subgraph Analysis Functions
// ============================================================================

/// Categorize a DAG node for subgraph analysis.
fn categorize_node(node: &DAGNode) -> NodeCategory {
    // Output/return nodes are workflow completion points
    if node.is_output || node.node_type == "return" {
        return NodeCategory::Output;
    }

    // Aggregators are barriers (wait for multiple spread results)
    if node.is_aggregator {
        return NodeCategory::Barrier;
    }

    // Action calls (external) need worker execution
    // But fn_call nodes are internal function expansions - treat as inline
    if node.node_type == "action_call" && !node.is_fn_call {
        return NodeCategory::Action;
    }

    // Everything else is inline: assignments, expressions, control flow, fn_call
    NodeCategory::Inline
}

/// Internal categorization for subgraph analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeCategory {
    Inline,
    Action,
    Barrier,
    Output,
}

/// Count the number of StateMachine predecessors for a node.
fn count_sm_predecessors(dag: &DAG, node_id: &str) -> i32 {
    dag.edges
        .iter()
        .filter(|e| e.target == node_id && e.edge_type == EdgeType::StateMachine)
        .count() as i32
}

/// Analyze the subgraph reachable from a completed node.
///
/// Starting from the completed node, traverses via StateMachine edges to find:
/// - Inline nodes: Can be executed immediately in memory
/// - Frontier nodes: Actions, barriers, or output where traversal stops
///
/// The inline_nodes are returned in topological order suitable for execution.
pub fn analyze_subgraph(start_node_id: &str, dag: &DAG, helper: &DAGHelper) -> SubgraphAnalysis {
    let mut analysis = SubgraphAnalysis::default();
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();

    // Also include the start node in all_node_ids for inbox fetching
    analysis.all_node_ids.insert(start_node_id.to_string());

    // Start with successors of the completed node
    for edge in helper.get_state_machine_successors(start_node_id) {
        queue.push_back(edge.target.clone());
    }

    while let Some(node_id) = queue.pop_front() {
        if visited.contains(&node_id) {
            continue;
        }
        visited.insert(node_id.clone());
        analysis.all_node_ids.insert(node_id.clone());

        let node = match dag.nodes.get(&node_id) {
            Some(n) => n,
            None => continue,
        };

        match categorize_node(node) {
            NodeCategory::Inline => {
                // Add to inline nodes for execution
                analysis.inline_nodes.push(node_id.clone());

                // Continue traversal to successors
                for edge in helper.get_state_machine_successors(&node_id) {
                    if !visited.contains(&edge.target) {
                        queue.push_back(edge.target.clone());
                    }
                }
            }
            NodeCategory::Action => {
                let required_count = count_sm_predecessors(dag, &node_id);
                analysis.frontier_nodes.push(FrontierNode {
                    node_id,
                    category: FrontierCategory::Action,
                    required_count,
                });
                // Don't traverse past actions - they execute via worker
            }
            NodeCategory::Barrier => {
                let required_count = count_sm_predecessors(dag, &node_id);
                analysis.frontier_nodes.push(FrontierNode {
                    node_id,
                    category: FrontierCategory::Barrier,
                    required_count,
                });
                // Don't traverse past barriers - they wait for all predecessors
            }
            NodeCategory::Output => {
                let required_count = count_sm_predecessors(dag, &node_id);
                analysis.frontier_nodes.push(FrontierNode {
                    node_id,
                    category: FrontierCategory::Output,
                    required_count,
                });
                // Don't traverse past output - workflow completes
            }
        }
    }

    analysis
}

/// Check if the completed node is a direct StateMachine predecessor of the frontier node.
///
/// This is important for readiness tracking - we only increment readiness when
/// the completing node is a direct predecessor via StateMachine edge.
pub fn is_direct_predecessor(completed_node_id: &str, frontier_node_id: &str, dag: &DAG) -> bool {
    dag.edges.iter().any(|e| {
        e.source == completed_node_id
            && e.target == frontier_node_id
            && e.edge_type == EdgeType::StateMachine
    })
}

/// Find the last inline node in a chain that is a direct predecessor of a frontier node.
///
/// When we complete an action A and traverse inline nodes B -> C -> D to reach
/// frontier F, we need to know which node (D) is the direct predecessor for
/// readiness tracking.
pub fn find_direct_predecessor_in_path(
    inline_nodes: &[String],
    frontier_node_id: &str,
    completed_node_id: &str,
    dag: &DAG,
) -> String {
    // Check inline nodes in reverse order (last executed first)
    for node_id in inline_nodes.iter().rev() {
        if is_direct_predecessor(node_id, frontier_node_id, dag) {
            return node_id.clone();
        }
    }

    // If no inline node is a direct predecessor, the completed node itself is
    completed_node_id.to_string()
}

// ============================================================================
// Inline Subgraph Execution
// ============================================================================

/// Error type for completion processing.
#[derive(Debug, thiserror::Error)]
pub enum CompletionError {
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    #[error("Spread collection not an array: {0}")]
    SpreadNotArray(String),

    #[error("Missing spread metadata")]
    MissingSpreadMetadata,

    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// Execute the inline subgraph and build a completion plan.
///
/// This is the core function that:
/// 1. Executes inline nodes in memory using the provided scope
/// 2. Collects inbox writes for frontier nodes via DataFlow edges
/// 3. Builds readiness increments for frontier nodes
/// 4. Determines if any frontier is the workflow output
///
/// The subgraph provides the maximum possible reachable nodes. This function
/// does a BFS traversal, evaluating guard expressions to determine which
/// branches are actually taken. Only nodes reachable via passing guards
/// are executed and have their frontiers processed.
///
/// Returns a CompletionPlan ready to be executed as a single transaction.
pub fn execute_inline_subgraph(
    completed_node_id: &str,
    completed_result: JsonValue,
    subgraph: &SubgraphAnalysis,
    existing_inbox: &HashMap<String, HashMap<String, JsonValue>>,
    dag: &DAG,
    instance_id: WorkflowInstanceId,
) -> Result<CompletionPlan, CompletionError> {
    let mut plan = CompletionPlan::new(completed_node_id.to_string());
    let helper = DAGHelper::new(dag);

    // Initialize inline scope with completed node's result
    let mut inline_scope: InlineScope = HashMap::new();
    if let Some(node) = dag.nodes.get(completed_node_id)
        && let Some(ref target) = node.target
    {
        inline_scope.insert(target.clone(), completed_result.clone());
    }

    // Merge existing inbox data for the completed node
    if let Some(node_inbox) = existing_inbox.get(completed_node_id) {
        for (var, val) in node_inbox {
            inline_scope
                .entry(var.clone())
                .or_insert_with(|| val.clone());
        }
    }

    // BFS traversal with guard evaluation
    // Track which nodes we've visited and which inline nodes we've executed
    let mut visited: HashSet<String> = HashSet::new();
    let mut executed_inline: Vec<String> = Vec::new();
    let mut reachable_frontiers: HashSet<String> = HashSet::new();

    // Queue holds (node_id, is_from_completed_node)
    // Start with successors of the completed node
    let mut queue: VecDeque<(String, bool)> = VecDeque::new();
    for successor in helper.get_ready_successors(completed_node_id, None) {
        // Evaluate guard if present - skip branch if guard fails
        if let Some(ref guard) = successor.guard_expr
            && !evaluate_guard(Some(guard), &inline_scope, &successor.node_id)
        {
            continue;
        }
        queue.push_back((successor.node_id, true));
    }

    while let Some((node_id, _is_direct)) = queue.pop_front() {
        if visited.contains(&node_id) {
            continue;
        }
        visited.insert(node_id.clone());

        // Only process nodes that are in the subgraph
        if !subgraph.all_node_ids.contains(&node_id) {
            continue;
        }

        let node = match dag.nodes.get(&node_id) {
            Some(n) => n,
            None => continue,
        };

        // Merge this node's inbox from DB into scope
        if let Some(node_inbox) = existing_inbox.get(&node_id) {
            for (var, val) in node_inbox {
                inline_scope
                    .entry(var.clone())
                    .or_insert_with(|| val.clone());
            }
        }

        // Check if this is a frontier node
        let is_frontier = subgraph.frontier_nodes.iter().any(|f| f.node_id == node_id);

        if is_frontier {
            // Mark as reachable frontier - we'll process it after BFS
            reachable_frontiers.insert(node_id.clone());
            // Don't traverse past frontier nodes
        } else {
            // This is an inline node - execute it
            let result = execute_inline_node(node, &inline_scope);
            if let Some(ref target) = node.target {
                inline_scope.insert(target.clone(), result);
            }
            executed_inline.push(node_id.clone());

            // Continue to successors, evaluating guards
            for successor in helper.get_ready_successors(&node_id, None) {
                if visited.contains(&successor.node_id) {
                    continue;
                }

                // Evaluate guard if present - skip branch if guard fails
                if let Some(ref guard) = successor.guard_expr
                    && !evaluate_guard(Some(guard), &inline_scope, &successor.node_id)
                {
                    debug!(
                        node_id = %node_id,
                        successor_id = %successor.node_id,
                        "guard failed, skipping branch"
                    );
                    continue;
                }

                queue.push_back((successor.node_id, false));
            }
        }
    }

    debug!(
        completed_node_id = %completed_node_id,
        executed_inline_count = executed_inline.len(),
        reachable_frontiers_count = reachable_frontiers.len(),
        reachable_frontiers = ?reachable_frontiers,
        "BFS traversal complete"
    );

    // Process only the reachable frontier nodes
    for frontier in &subgraph.frontier_nodes {
        // Skip frontiers that weren't reachable via passing guards
        if !reachable_frontiers.contains(&frontier.node_id) {
            continue;
        }
        let frontier_node = match dag.nodes.get(&frontier.node_id) {
            Some(n) => n,
            None => continue,
        };

        // Collect DataFlow writes from inline scope to this frontier node
        let writes = collect_data_flow_writes(&frontier.node_id, &inline_scope, dag, instance_id);
        plan.inbox_writes.extend(writes);

        // Find which node in our path is the direct predecessor of this frontier
        let predecessor_id = find_direct_predecessor_in_path(
            &executed_inline,
            &frontier.node_id,
            completed_node_id,
            dag,
        );

        // Only add readiness increment if we have a direct edge to this frontier
        if is_direct_predecessor(&predecessor_id, &frontier.node_id, dag) {
            match frontier.category {
                FrontierCategory::Action => {
                    // Build the dispatch payload for this action
                    let mut action_inbox = existing_inbox
                        .get(&frontier.node_id)
                        .cloned()
                        .unwrap_or_default();

                    // Merge inline scope via DataFlow edges
                    merge_data_flow_into_inbox(
                        &frontier.node_id,
                        &inline_scope,
                        dag,
                        &mut action_inbox,
                    );

                    let dispatch_payload = build_action_payload(frontier_node, &action_inbox)?;

                    plan.readiness_increments.push(ReadinessIncrement {
                        node_id: frontier.node_id.clone(),
                        required_count: frontier.required_count,
                        node_type: NodeType::Action,
                        module_name: frontier_node.module_name.clone(),
                        action_name: frontier_node.action_name.clone(),
                        dispatch_payload: Some(dispatch_payload),
                    });
                }
                FrontierCategory::Barrier => {
                    plan.readiness_increments.push(ReadinessIncrement {
                        node_id: frontier.node_id.clone(),
                        required_count: frontier.required_count,
                        node_type: NodeType::Barrier,
                        module_name: None,
                        action_name: None,
                        dispatch_payload: None,
                    });
                }
                FrontierCategory::Output => {
                    // Workflow complete - get result
                    // Return nodes typically don't have DataFlow edges - they get their value
                    // from the result being propagated through the inline scope.
                    //
                    // Priority for finding the result:
                    // 1. DataFlow edges to this node (if any exist)
                    // 2. Existing inbox for this node
                    // 3. The inline scope (propagated result from completed node)
                    let mut output_inbox = existing_inbox
                        .get(&frontier.node_id)
                        .cloned()
                        .unwrap_or_default();

                    // Try to merge via DataFlow edges (might not find anything for return nodes)
                    merge_data_flow_into_inbox(
                        &frontier.node_id,
                        &inline_scope,
                        dag,
                        &mut output_inbox,
                    );

                    // If we found nothing via DataFlow, use the inline scope directly
                    // This handles return statements that reference variables in scope
                    let result_value = if !output_inbox.is_empty() {
                        // Found via DataFlow or existing inbox
                        output_inbox
                            .get("result")
                            .or_else(|| output_inbox.values().next())
                            .cloned()
                            .unwrap_or(JsonValue::Null)
                    } else {
                        // No DataFlow edges - get result from inline scope
                        // Try "result" first (common convention from action target),
                        // then the node's target variable, then first available value
                        inline_scope
                            .get("result")
                            .or_else(|| {
                                // Try to find by the completed node's target variable
                                if let Some(completed) = dag.nodes.get(completed_node_id) {
                                    if let Some(ref target) = completed.target {
                                        inline_scope.get(target)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .or_else(|| inline_scope.values().next())
                            .cloned()
                            .unwrap_or(JsonValue::Null)
                    };

                    debug!(
                        output_node_id = %frontier.node_id,
                        result_value = ?result_value,
                        "determined workflow result"
                    );

                    plan.instance_completion = Some(InstanceCompletion {
                        instance_id,
                        result_payload: serialize_workflow_result(&result_value)?,
                    });
                }
            }
        }
    }

    Ok(plan)
}

/// Execute a single inline node and return its result.
fn execute_inline_node(node: &DAGNode, _scope: &InlineScope) -> JsonValue {
    // Most inline nodes don't produce meaningful values themselves
    // The actual data flows through DataFlow edges from their predecessors
    match node.node_type.as_str() {
        "assignment" => JsonValue::Null,
        "input" | "output" => JsonValue::Null,
        "return" => JsonValue::Null,
        "conditional" | "branch" => JsonValue::Bool(true),
        "join" => JsonValue::Null,
        "aggregator" => JsonValue::Array(vec![]),
        _ => JsonValue::Null,
    }
}

/// Evaluate a guard expression to determine if a branch should be taken.
///
/// Returns true if the guard passes (branch should be taken), false otherwise.
fn evaluate_guard(guard_expr: Option<&ast::Expr>, scope: &InlineScope, successor_id: &str) -> bool {
    let Some(guard) = guard_expr else {
        // No guard expression - always pass
        return true;
    };

    match ExpressionEvaluator::evaluate(guard, scope) {
        Ok(val) => {
            let is_true = match &val {
                JsonValue::Bool(b) => *b,
                JsonValue::Null => false,
                JsonValue::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
                JsonValue::String(s) => !s.is_empty(),
                JsonValue::Array(a) => !a.is_empty(),
                JsonValue::Object(o) => !o.is_empty(),
            };
            debug!(
                successor_id = %successor_id,
                guard_expr = ?guard,
                result = ?val,
                is_true = is_true,
                "evaluated guard expression"
            );
            is_true
        }
        Err(e) => {
            warn!(
                successor_id = %successor_id,
                error = %e,
                "failed to evaluate guard expression, assuming false"
            );
            false
        }
    }
}

/// Collect DataFlow edge writes from inline scope to a target node.
fn collect_data_flow_writes(
    target_node_id: &str,
    inline_scope: &InlineScope,
    dag: &DAG,
    instance_id: WorkflowInstanceId,
) -> Vec<InboxWrite> {
    let mut writes = Vec::new();

    for edge in &dag.edges {
        if edge.target == target_node_id
            && edge.edge_type == EdgeType::DataFlow
            && let Some(ref var_name) = edge.variable
            && let Some(value) = inline_scope.get(var_name)
        {
            writes.push(InboxWrite {
                instance_id,
                target_node_id: target_node_id.to_string(),
                variable_name: var_name.clone(),
                value: value.clone(),
                source_node_id: edge.source.clone(),
                spread_index: None,
            });
        }
    }

    writes
}

/// Merge DataFlow edge data into an inbox HashMap.
fn merge_data_flow_into_inbox(
    target_node_id: &str,
    inline_scope: &InlineScope,
    dag: &DAG,
    inbox: &mut HashMap<String, JsonValue>,
) {
    // Find all DataFlow edges to this target
    let df_edges: Vec<_> = dag
        .edges
        .iter()
        .filter(|e| e.target == target_node_id && e.edge_type == EdgeType::DataFlow)
        .collect();

    debug!(
        target_node_id = %target_node_id,
        dataflow_edges_count = df_edges.len(),
        dataflow_edges = ?df_edges.iter().map(|e| (&e.source, &e.variable)).collect::<Vec<_>>(),
        inline_scope_keys = ?inline_scope.keys().collect::<Vec<_>>(),
        "merging data flow edges"
    );

    for edge in df_edges {
        if let Some(ref var_name) = edge.variable {
            if let Some(value) = inline_scope.get(var_name) {
                debug!(
                    target_node_id = %target_node_id,
                    var_name = %var_name,
                    value = ?value,
                    "matched dataflow edge with scope"
                );
                inbox.insert(var_name.clone(), value.clone());
            } else {
                debug!(
                    target_node_id = %target_node_id,
                    var_name = %var_name,
                    "dataflow edge variable not in scope"
                );
            }
        }
    }
}

/// Build action dispatch payload from node kwargs and inbox.
fn build_action_payload(
    node: &DAGNode,
    inbox: &HashMap<String, JsonValue>,
) -> Result<Vec<u8>, CompletionError> {
    let mut payload_map = serde_json::Map::new();

    if let Some(ref kwargs) = node.kwargs {
        for (key, value_str) in kwargs {
            let resolved = resolve_kwarg_value(value_str, inbox);
            payload_map.insert(key.clone(), resolved);
        }
    }

    Ok(serde_json::to_vec(&JsonValue::Object(payload_map))?)
}

/// Resolve a kwarg value string to a JSON value.
fn resolve_kwarg_value(value_str: &str, inbox: &HashMap<String, JsonValue>) -> JsonValue {
    // Variable reference
    if let Some(var_name) = value_str.strip_prefix('$') {
        return inbox.get(var_name).cloned().unwrap_or(JsonValue::Null);
    }

    // Try to parse as JSON
    match serde_json::from_str(value_str) {
        Ok(v) => v,
        Err(_) => JsonValue::String(value_str.to_string()),
    }
}

/// Serialize workflow result as protobuf WorkflowArguments.
fn serialize_workflow_result(result: &JsonValue) -> Result<Vec<u8>, CompletionError> {
    use crate::messages::proto;
    use prost::Message;

    let arguments = vec![proto::WorkflowArgument {
        key: "result".to_string(),
        value: Some(json_to_proto_value(result)),
    }];

    let workflow_args = proto::WorkflowArguments { arguments };
    Ok(workflow_args.encode_to_vec())
}

/// Convert JSON value to protobuf WorkflowArgumentValue.
fn json_to_proto_value(value: &JsonValue) -> crate::messages::proto::WorkflowArgumentValue {
    use crate::messages::proto::{self, workflow_argument_value::Kind};

    let kind = match value {
        JsonValue::Null => Kind::Primitive(proto::PrimitiveWorkflowArgument {
            kind: Some(proto::primitive_workflow_argument::Kind::NullValue(0)),
        }),
        JsonValue::Bool(b) => Kind::Primitive(proto::PrimitiveWorkflowArgument {
            kind: Some(proto::primitive_workflow_argument::Kind::BoolValue(*b)),
        }),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::IntValue(i)),
                })
            } else if let Some(f) = n.as_f64() {
                Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::DoubleValue(f)),
                })
            } else {
                Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::DoubleValue(0.0)),
                })
            }
        }
        JsonValue::String(s) => Kind::Primitive(proto::PrimitiveWorkflowArgument {
            kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                s.clone(),
            )),
        }),
        JsonValue::Array(arr) => {
            let items = arr.iter().map(json_to_proto_value).collect();
            Kind::ListValue(proto::WorkflowListArgument { items })
        }
        JsonValue::Object(obj) => {
            let entries = obj
                .iter()
                .map(|(k, v)| proto::WorkflowArgument {
                    key: k.clone(),
                    value: Some(json_to_proto_value(v)),
                })
                .collect();
            Kind::DictValue(proto::WorkflowDictArgument { entries })
        }
    };

    proto::WorkflowArgumentValue { kind: Some(kind) }
}
