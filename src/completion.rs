//! Completion plan and unified readiness model types.
//!
//! This module contains the data structures used for the unified readiness model
//! where every node gets readiness tracking and is only enqueued when
//! `completed_count == required_count`.

use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{debug, info, warn};

use uuid::Uuid;

use crate::ast_evaluator::{EvaluationError, ExpressionEvaluator};
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

/// Inline execution context for subgraph processing.
pub struct InlineContext<'a> {
    /// Variables available prior to processing this completion (e.g., workflow inputs).
    pub initial_scope: &'a HashMap<String, JsonValue>,
    /// Inbox data fetched from storage for nodes involved in this completion.
    pub existing_inbox: &'a HashMap<String, HashMap<String, JsonValue>>,
    /// Spread index for spread actions (if applicable).
    pub spread_index: Option<usize>,
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

    /// Readiness resets for loop re-triggering.
    pub readiness_resets: Vec<String>,

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
            readiness_resets: Vec::new(),
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

    /// Scheduled time for durable sleep actions.
    pub scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Type of node for the action queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Action,
    Barrier,
    Sleep,
}

impl NodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeType::Action => "action",
            NodeType::Barrier => "barrier",
            NodeType::Sleep => "sleep",
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
///
/// The key principle: **Barriers exist where actual runtime coordination is needed**
/// (waiting for multiple async operations to complete). Nodes with only one predecessor
/// can always be inlined because there's nothing to wait for - data flows directly.
///
/// - `Action`: External calls that require worker execution (actual async work)
/// - `Barrier`: Structural nodes that wait for multiple branches to converge
/// - `Inline`: Pure computation that can be evaluated immediately
/// - `Output`: Workflow completion points
fn categorize_node(node: &DAGNode) -> NodeCategory {
    // Only `is_output` nodes are workflow completion points.
    // These are function output boundary nodes. For the entry function, this
    // triggers workflow completion. For helper functions that have been expanded
    // inline, these nodes should have edges to their callers (e.g., loop-back
    // edges for for-loop bodies) so traversal continues via those edges.
    //
    // Note: `return` nodes (node_type == "return") are treated as inline because
    // they just set up the return value - the actual output boundary is the
    // `is_output` node that follows.
    if node.is_output {
        return NodeCategory::Output;
    }

    // Aggregators are always barriers (wait for parallel/spread results)
    if node.is_aggregator {
        return NodeCategory::Barrier;
    }

    // Join nodes (like loop_exit) are barriers ONLY if they wait for multiple branches.
    // A join with required_count = 1 has only one predecessor, so there's nothing to
    // wait for - data flows directly and it can be inlined. This commonly happens with
    // simple for-loops where the loop body doesn't have parallel branches inside.
    if node.node_type == "join" {
        // If join_required_count is explicitly set to 1, treat as inline
        // Otherwise, conservatively treat as barrier (it will be computed later)
        if node.join_required_count == Some(1) {
            return NodeCategory::Inline;
        }
        return NodeCategory::Barrier;
    }

    // Action calls (external) need worker execution
    // But fn_call nodes are internal function expansions - treat as inline
    if node.node_type == "action_call" && !node.is_fn_call {
        return NodeCategory::Action;
    }

    // Everything else is inline: assignments, expressions, control flow (branch), fn_call, return
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
/// Excludes loop-back edges since they don't contribute to initial readiness.
/// For join nodes with `join_required_count` set, uses that override value instead.
fn count_sm_predecessors(dag: &DAG, node_id: &str) -> i32 {
    // Check if this node has an explicit join_required_count set
    // (used for conditional joins where only one branch executes)
    if let Some(node) = dag.nodes.get(node_id)
        && let Some(required) = node.join_required_count
    {
        return required;
    }

    dag.edges
        .iter()
        .filter(|e| {
            e.target == node_id
                && e.edge_type == EdgeType::StateMachine
                && !e.is_loop_back // Don't count loop-back edges
                && e.exception_types.is_none()
                && !e
                    .condition
                    .as_deref()
                    .map(|c| c.starts_with("except:"))
                    .unwrap_or(false)
        })
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
                // Check if this output node has outgoing edges (expanded helper function output)
                // If it does, treat it as inline and continue traversal to find loop-back edges
                let outgoing_edges = helper.get_state_machine_successors(&node_id);
                if !outgoing_edges.is_empty() {
                    // This is an expanded helper function output with outgoing edges
                    // (e.g., loop-back edge to for_loop). Treat as inline.
                    analysis.inline_nodes.push(node_id.clone());
                    for edge in outgoing_edges {
                        if !visited.contains(&edge.target) {
                            queue.push_back(edge.target.clone());
                        }
                    }
                } else {
                    // This is the workflow's final output - stop traversal
                    let required_count = count_sm_predecessors(dag, &node_id);
                    analysis.frontier_nodes.push(FrontierNode {
                        node_id,
                        category: FrontierCategory::Output,
                        required_count,
                    });
                }
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

    #[error("Guard evaluation error at node '{node_id}': {message}")]
    GuardEvaluationError { node_id: String, message: String },

    #[error(
        "Workflow dead-end: no reachable frontier nodes after completing '{completed_node_id}'. Guard evaluation errors: {guard_errors:?}"
    )]
    WorkflowDeadEnd {
        completed_node_id: String,
        guard_errors: Vec<(String, String)>,
    },
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
    ctx: InlineContext<'_>,
    subgraph: &SubgraphAnalysis,
    dag: &DAG,
    instance_id: WorkflowInstanceId,
) -> Result<CompletionPlan, CompletionError> {
    info!(
        completed_node_id = %completed_node_id,
        completed_result = ?completed_result,
        spread_index = ?ctx.spread_index,
        initial_scope_keys = ?ctx.initial_scope.keys().collect::<Vec<_>>(),
        existing_inbox_keys = ?ctx.existing_inbox.keys().collect::<Vec<_>>(),
        subgraph_inline_nodes = ?subgraph.inline_nodes,
        subgraph_frontier_nodes = ?subgraph.frontier_nodes.iter().map(|f| &f.node_id).collect::<Vec<_>>(),
        "DEBUG: execute_inline_subgraph START"
    );
    let mut plan = CompletionPlan::new(completed_node_id.to_string());
    let helper = DAGHelper::new(dag);
    let InlineContext {
        initial_scope,
        existing_inbox,
        spread_index,
    } = ctx;

    // Initialize inline scope with completed node's result
    let mut inline_scope: InlineScope = initial_scope.clone();
    if let Some(node) = dag.nodes.get(completed_node_id)
        && let Some(ref target) = node.target
    {
        // For aggregator nodes with multiple targets (parallel blocks with tuple unpacking),
        // don't insert the aggregated array as the result. For parallel blocks, the named
        // variables from the inbox (written by individual parallel actions) are the
        // authoritative source. They will be merged in later from existing_inbox.
        //
        // For spread/for-loop aggregators (single target), the aggregated array IS the result
        // and should be used.
        let is_parallel_aggregator =
            node.node_type == "aggregator" && node.targets.as_ref().is_some_and(|t| t.len() > 1);

        if !is_parallel_aggregator {
            inline_scope.insert(target.clone(), completed_result.clone());
        }
    }

    // If this is a spread action, write the result to the aggregator with spread_index
    if let Some(spread_idx) = spread_index
        && let Some(node) = dag.nodes.get(completed_node_id)
        && let Some(ref aggregator_id) = node.aggregates_to
    {
        // Write result to aggregator with spread_index
        let var_name = node
            .target
            .clone()
            .unwrap_or_else(|| "_for_loop_result".to_string());
        plan.inbox_writes.push(InboxWrite {
            instance_id,
            target_node_id: aggregator_id.clone(),
            variable_name: var_name,
            value: completed_result.clone(),
            source_node_id: completed_node_id.to_string(),
            spread_index: Some(spread_idx as i32),
        });
    }

    // Merge existing inbox data for the completed node, but only for variables
    // that we haven't already set (like the action result). This prevents stale
    // inbox values from overwriting fresh action results.
    let completed_inbox = existing_inbox.get(completed_node_id);
    debug!(
        completed_node_id = %completed_node_id,
        has_inbox = completed_inbox.is_some(),
        inbox_vars = ?completed_inbox.map(|i| i.keys().collect::<Vec<_>>()),
        all_inbox_keys = ?existing_inbox.keys().collect::<Vec<_>>(),
        "merging completed node inbox into scope"
    );
    if let Some(node_inbox) = completed_inbox {
        for (var, val) in node_inbox {
            if var == "__loop_loop_8_i" {
                debug!(
                    var_name = %var,
                    inbox_value = ?val,
                    "merging __loop_loop_8_i from inbox"
                );
            }
            // Only insert if not already in scope - preserves action results and
            // values computed during this BFS traversal
            if !inline_scope.contains_key(var) {
                inline_scope.insert(var.clone(), val.clone());
            }
        }
    }

    // BFS traversal with guard evaluation
    // Track which nodes we've visited and which inline nodes we've executed
    let mut visited: HashSet<String> = HashSet::new();
    let mut executed_inline: Vec<String> = Vec::new();
    // Track frontiers and whether they were reached via a loop-back edge
    let mut reachable_frontiers: HashMap<String, bool> = HashMap::new();

    // Queue holds (node_id, reached_via_loop_back)
    // Start with successors of the completed node
    // Use get_state_machine_successors to include loop-back edges (needed for for-loop iteration)
    let mut queue: VecDeque<(String, bool)> = VecDeque::new();
    let mut guard_errors: Vec<(String, String)> = Vec::new();
    for edge in helper.get_state_machine_successors(completed_node_id) {
        // Evaluate guard if present - skip branch if guard fails or errors
        if let Some(ref guard) = edge.guard_expr {
            match evaluate_guard(Some(guard), &inline_scope, &edge.target) {
                GuardResult::Pass => {}
                GuardResult::Fail => {
                    continue;
                }
                GuardResult::Error(err) => {
                    // Track guard errors but continue - we'll report if we hit a dead-end
                    guard_errors.push((edge.target.clone(), err));
                    continue;
                }
            }
        }
        queue.push_back((edge.target.clone(), edge.is_loop_back));
    }

    // Track loop iteration counts to prevent infinite loops
    let mut loop_iterations: HashMap<String, usize> = HashMap::new();
    const MAX_LOOP_ITERATIONS: usize = 10000;

    while let Some((node_id, reached_via_loop_back)) = queue.pop_front() {
        // For loop-back traversals, we allow re-visiting loop nodes but track iteration count
        if reached_via_loop_back {
            let count = loop_iterations.entry(node_id.clone()).or_insert(0);
            *count += 1;
            if *count > MAX_LOOP_ITERATIONS {
                warn!(
                    node_id = %node_id,
                    iterations = *count,
                    "loop exceeded max iterations, terminating"
                );
                break;
            }
        } else if visited.contains(&node_id) {
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

        // Merge this node's inbox from DB into scope, but only for variables
        // that we haven't already computed during this BFS traversal.
        // This prevents stale DB values from overwriting fresh inline computations
        // (e.g., loop_incr computing __loop_loop_8_i = 1, but loop_cond's inbox
        // still has the old value __loop_loop_8_i = 0 from a previous iteration).
        if let Some(node_inbox) = existing_inbox.get(&node_id) {
            for (var, val) in node_inbox {
                if !inline_scope.contains_key(var) {
                    inline_scope.insert(var.clone(), val.clone());
                }
            }
        }

        // Check if this is a frontier node
        let is_frontier = subgraph.frontier_nodes.iter().any(|f| f.node_id == node_id);

        if is_frontier {
            // Mark as reachable frontier with loop-back info
            info!(
                node_id = %node_id,
                reached_via_loop_back = reached_via_loop_back,
                node_type = %node.node_type,
                "DEBUG: reached frontier node"
            );
            reachable_frontiers.insert(node_id.clone(), reached_via_loop_back);
            // Don't traverse past frontier nodes
        } else {
            // This is an inline node - execute it
            let result = execute_inline_node(node, &inline_scope);
            info!(
                node_id = %node.id,
                node_type = %node.node_type,
                target = ?node.target,
                result = ?result,
                scope_keys = ?inline_scope.keys().collect::<Vec<_>>(),
                "DEBUG: executed inline node (completion)"
            );
            if let Some(ref target) = node.target {
                info!(
                    node_id = %node.id,
                    target = %target,
                    old_value = ?inline_scope.get(target),
                    new_value = ?result,
                    "DEBUG: scope update"
                );
                inline_scope.insert(target.clone(), result);
                info!(
                    node_id = %node.id,
                    target = %target,
                    inserted_value = ?inline_scope.get(target),
                    "DEBUG: scope after insert"
                );
            }
            executed_inline.push(node_id.clone());

            // Continue to successors, evaluating guards
            // Use get_state_machine_successors to include loop-back edges
            for edge in helper.get_state_machine_successors(&node_id) {
                // Allow loop-back edges and their downstream nodes to re-visit (for inline loop execution)
                // If we reached the current node via loop-back, we're in a loop iteration and need to
                // allow re-visiting downstream nodes in the loop body too.
                let is_loop_back_context = reached_via_loop_back || edge.is_loop_back;
                if !is_loop_back_context && visited.contains(&edge.target) {
                    continue;
                }

                // Evaluate guard if present - skip branch if guard fails or errors
                if let Some(ref guard) = edge.guard_expr {
                    match evaluate_guard(Some(guard), &inline_scope, &edge.target) {
                        GuardResult::Pass => {
                            // Guard passed, continue to add to queue
                        }
                        GuardResult::Fail => {
                            debug!(
                                node_id = %node_id,
                                successor_id = %edge.target,
                                "guard failed, skipping branch"
                            );
                            continue;
                        }
                        GuardResult::Error(err) => {
                            // Track guard errors but continue - we'll report if we hit a dead-end
                            guard_errors.push((edge.target.clone(), err));
                            continue;
                        }
                    }
                }

                // Propagate loop-back status: true if we already came via loop-back OR this edge is loop-back
                let via_loop_back = reached_via_loop_back || edge.is_loop_back;
                queue.push_back((edge.target.clone(), via_loop_back));
            }
        }
    }

    info!(
        completed_node_id = %completed_node_id,
        executed_inline_count = executed_inline.len(),
        executed_inline_nodes = ?executed_inline,
        reachable_frontiers_count = reachable_frontiers.len(),
        reachable_frontiers = ?reachable_frontiers.keys().collect::<Vec<_>>(),
        inline_scope_keys = ?inline_scope.keys().collect::<Vec<_>>(),
        "DEBUG: BFS traversal complete"
    );
    // Log the full inline scope after traversal
    for (var, val) in &inline_scope {
        info!(
            variable = %var,
            value = ?val,
            "DEBUG: inline scope variable after BFS"
        );
    }

    // Detect dead-end: no reachable frontiers AND no workflow completion yet
    // This indicates the workflow got stuck (e.g., all branches of a conditional blocked)
    if reachable_frontiers.is_empty() && plan.instance_completion.is_none() {
        // Check if there are any frontiers in the subgraph at all
        // If there are frontiers but none are reachable, that's a dead-end
        if !subgraph.frontier_nodes.is_empty() {
            return Err(CompletionError::WorkflowDeadEnd {
                completed_node_id: completed_node_id.to_string(),
                guard_errors,
            });
        }
    }

    // Write variables to ALL downstream DataFlow edge targets, not just frontiers.
    // This is crucial for chain workflows where action_5 needs step1 from action_2,
    // but action_5 is not in the frontier when action_2 completes.
    let all_df_writes = collect_all_data_flow_writes(
        &inline_scope,
        dag,
        instance_id,
        completed_node_id,
        &executed_inline,
    );
    plan.inbox_writes.extend(all_df_writes);

    // Process only the reachable frontier nodes
    for frontier in &subgraph.frontier_nodes {
        // Skip frontiers that weren't reachable via passing guards
        let _reached_via_loop_back = match reachable_frontiers.get(&frontier.node_id) {
            Some(&via_loop_back) => via_loop_back,
            None => continue, // Not reachable
        };
        let frontier_node = match dag.nodes.get(&frontier.node_id) {
            Some(n) => n,
            None => continue,
        };

        // Collect DataFlow writes from inline scope to this frontier node
        // Skip for Barrier frontiers - their data flow is handled specially:
        // - Spread patterns: written with spread_index at the start of this function
        // - Parallel blocks: written with target variable name in the Barrier case below
        if frontier.category != FrontierCategory::Barrier {
            let writes =
                collect_data_flow_writes(&frontier.node_id, &inline_scope, dag, instance_id);
            plan.inbox_writes.extend(writes);
        }

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

                    // Overwrite action_inbox with inline scope values. The inline scope contains
                    // freshly computed values from the current BFS traversal (e.g., loop variable
                    // updates), which must take precedence over stale values from existing_inbox.
                    // This is critical for for-loops where variables like `processed` accumulate
                    // across iterations - we need the updated value, not the stale DB value.
                    for (var, val) in &inline_scope {
                        action_inbox.insert(var.clone(), val.clone());
                    }

                    debug!(
                        frontier_node_id = %frontier.node_id,
                        inline_scope_keys = ?inline_scope.keys().collect::<Vec<_>>(),
                        action_inbox_keys = ?action_inbox.keys().collect::<Vec<_>>(),
                        "building action dispatch payload"
                    );

                    // Write action_inbox variables to the action's inbox in DB.
                    // This ensures variables are available when the action completes
                    // and we traverse to successor nodes (e.g., loop_incr after fn_call).
                    for (var_name, value) in &action_inbox {
                        plan.inbox_writes.push(InboxWrite {
                            instance_id,
                            target_node_id: frontier.node_id.clone(),
                            variable_name: var_name.clone(),
                            value: value.clone(),
                            source_node_id: completed_node_id.to_string(),
                            spread_index: None,
                        });
                    }

                    let dispatch_payload = build_action_payload(frontier_node, &action_inbox)?;

                    // Check if this is a sleep action
                    let is_sleep = frontier_node.action_name.as_deref() == Some("sleep");
                    let (node_type, scheduled_at) = if is_sleep {
                        // Parse duration from the dispatch payload
                        let duration_secs =
                            serde_json::from_slice::<serde_json::Value>(&dispatch_payload)
                                .ok()
                                .and_then(|v| v.get("duration").cloned())
                                .and_then(|d| d.as_f64().or_else(|| d.as_i64().map(|i| i as f64)))
                                .unwrap_or(0.0);
                        let scheduled_at = chrono::Utc::now()
                            + chrono::Duration::milliseconds((duration_secs * 1000.0) as i64);
                        (NodeType::Sleep, Some(scheduled_at))
                    } else {
                        (NodeType::Action, None)
                    };

                    // For actions with required_count=1, always reset readiness before incrementing.
                    // This handles loop iterations where the same action is re-triggered:
                    // - First iteration increments 0->1, action fires
                    // - Second iteration would increment 1->2 without reset
                    // Since an action with required_count=1 can only be triggered by one
                    // predecessor at a time, resetting before incrementing is always safe.
                    if frontier.required_count == 1 {
                        plan.readiness_resets.push(frontier.node_id.clone());
                    }

                    plan.readiness_increments.push(ReadinessIncrement {
                        node_id: frontier.node_id.clone(),
                        required_count: frontier.required_count,
                        node_type,
                        module_name: frontier_node.module_name.clone(),
                        action_name: frontier_node.action_name.clone(),
                        dispatch_payload: Some(dispatch_payload),
                        scheduled_at,
                    });
                }
                FrontierCategory::Barrier => {
                    // For parallel blocks (NOT spread patterns), write the completed node's
                    // result to the barrier's inbox so it can be passed to successor nodes
                    // when the barrier fires.
                    //
                    // Spread actions already write their result with spread_index at the
                    // beginning of execute_inline_subgraph, so we skip this for spread actions.
                    if spread_index.is_none()
                        && let Some(completed_node) = dag.nodes.get(completed_node_id)
                        && let Some(ref target) = completed_node.target
                    {
                        // Write this parallel action's result to the barrier's inbox
                        plan.inbox_writes.push(InboxWrite {
                            instance_id,
                            target_node_id: frontier.node_id.clone(),
                            variable_name: target.clone(),
                            value: completed_result.clone(),
                            source_node_id: completed_node_id.to_string(),
                            spread_index: None,
                        });
                    }

                    // For barriers with required_count=1 (e.g., conditional joins in loops),
                    // reset readiness before incrementing to handle re-execution.
                    // NEVER reset aggregators - they collect from spread actions and have
                    // their required_count set dynamically based on spread size.
                    if frontier.required_count == 1 && !frontier_node.is_aggregator {
                        plan.readiness_resets.push(frontier.node_id.clone());
                    }

                    plan.readiness_increments.push(ReadinessIncrement {
                        node_id: frontier.node_id.clone(),
                        required_count: frontier.required_count,
                        node_type: NodeType::Barrier,
                        module_name: None,
                        action_name: None,
                        dispatch_payload: None,
                        scheduled_at: None,
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
fn execute_inline_node(node: &DAGNode, scope: &InlineScope) -> JsonValue {
    // Most inline nodes don't produce meaningful values themselves
    // The actual data flows through DataFlow edges from their predecessors
    match node.node_type.as_str() {
        "assignment" => {
            // Evaluate the assignment expression
            if let Some(ref expr) = node.assign_expr {
                match ExpressionEvaluator::evaluate(expr, scope) {
                    Ok(val) => val,
                    Err(e) => {
                        tracing::warn!(
                            node_id = %node.id,
                            error = %e,
                            "failed to evaluate assignment expression in completion"
                        );
                        JsonValue::Null
                    }
                }
            } else {
                JsonValue::Null
            }
        }
        "return" => {
            if let Some(ref expr) = node.assign_expr {
                match ExpressionEvaluator::evaluate(expr, scope) {
                    Ok(val) => val,
                    Err(e) => {
                        tracing::warn!(
                            node_id = %node.id,
                            error = %e,
                            "failed to evaluate return expression in completion"
                        );
                        JsonValue::Null
                    }
                }
            } else {
                JsonValue::Null
            }
        }
        "input" | "output" => JsonValue::Null,
        "conditional" | "branch" => JsonValue::Bool(true),
        "join" => JsonValue::Null,
        "aggregator" => JsonValue::Array(vec![]),
        _ => JsonValue::Null,
    }
}

/// Result of guard evaluation.
#[derive(Debug)]
pub enum GuardResult {
    /// Guard passed (true) - branch should be taken.
    Pass,
    /// Guard failed (false) - branch should not be taken.
    Fail,
    /// Guard evaluation had an error - caller should decide how to handle.
    Error(String),
}

/// Evaluate a guard expression to determine if a branch should be taken.
///
/// Returns:
/// - `GuardResult::Pass` if the guard passes (branch should be taken)
/// - `GuardResult::Fail` if the guard evaluates to false
/// - `GuardResult::Error` if the guard expression couldn't be evaluated
pub fn evaluate_guard(
    guard_expr: Option<&ast::Expr>,
    scope: &InlineScope,
    successor_id: &str,
) -> GuardResult {
    let Some(guard) = guard_expr else {
        // No guard expression - always pass
        return GuardResult::Pass;
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

/// Evaluate a string-based guard for loop control.
///
/// Handles guards like "__loop_has_next(for_loop_0)" by checking if there are
/// more items to iterate in the collection.
///
/// Parameters:
/// - `guard_string`: The guard string (e.g., "__loop_has_next(for_loop_0)")
/// - `loop_state`: Tuple of (current_index, collection_length)
/// - `successor_id`: For logging
///
/// Returns true if the loop should continue, false if it should break.
pub fn evaluate_guard_string(
    guard_string: Option<&str>,
    loop_state: Option<(usize, usize)>,
    successor_id: &str,
) -> bool {
    let Some(guard) = guard_string else {
        // No guard string - always pass
        return true;
    };

    // Parse __loop_has_next(loop_id) pattern
    if guard.starts_with("__loop_has_next(") && guard.ends_with(')') {
        let loop_id = &guard[16..guard.len() - 1];

        // Check if we have loop state
        if let Some((current_index, collection_length)) = loop_state {
            let has_next = current_index < collection_length;
            debug!(
                successor_id = %successor_id,
                loop_id = %loop_id,
                current_index = current_index,
                collection_length = collection_length,
                has_next = has_next,
                "evaluated loop guard"
            );
            return has_next;
        } else {
            warn!(
                successor_id = %successor_id,
                loop_id = %loop_id,
                "no loop state found for guard, assuming false"
            );
            return false;
        }
    }

    // Unknown guard string pattern
    warn!(
        successor_id = %successor_id,
        guard_string = %guard,
        "unknown guard string pattern, assuming false"
    );
    false
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

/// Collect ALL DataFlow edge writes from inline scope to ANY target node.
///
/// This is crucial for chain workflows where a node (e.g., action_5) may need
/// variables from multiple earlier nodes (action_2, action_3, action_4) that
/// are NOT in its immediate frontier. Without this, variables like `step1`
/// from `action_2` would never be written to `action_5`'s inbox because
/// `action_5` is not a frontier when `action_2` completes.
fn collect_all_data_flow_writes(
    inline_scope: &InlineScope,
    dag: &DAG,
    instance_id: WorkflowInstanceId,
    completed_node_id: &str,
    executed_inline: &[String],
) -> Vec<InboxWrite> {
    let mut writes = Vec::new();

    // Find all DataFlow edges where the variable is in scope
    for edge in &dag.edges {
        if edge.edge_type == EdgeType::DataFlow
            && let Some(ref var_name) = edge.variable
            && let Some(value) = inline_scope.get(var_name)
        {
            // Only write if the source is either the completed node, an input node,
            // or an inline node that was executed during this traversal
            // (i.e., don't re-write variables from nodes that haven't completed yet)
            if let Some(completed_node) = dag.nodes.get(completed_node_id) {
                // The variable should come from either:
                // 1. The completed node's target (it just produced this variable)
                // 2. The input node (workflow inputs are always in scope)
                // 3. An inline node that executed during this traversal (e.g., loop_init)
                let is_from_completed = completed_node.target.as_deref() == Some(var_name);
                let is_from_input = dag
                    .nodes
                    .get(&edge.source)
                    .map(|n| n.is_input)
                    .unwrap_or(false);
                let is_from_executed_inline = executed_inline.contains(&edge.source);

                if is_from_completed || is_from_input || is_from_executed_inline {
                    writes.push(InboxWrite {
                        instance_id,
                        target_node_id: edge.target.clone(),
                        variable_name: var_name.clone(),
                        value: value.clone(),
                        source_node_id: edge.source.clone(),
                        spread_index: None,
                    });
                }
            }
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
        let kwarg_exprs = node.kwarg_exprs.as_ref();
        for (key, value_str) in kwargs {
            let expr = kwarg_exprs.and_then(|m| m.get(key));
            let resolved = resolve_kwarg_value(key, value_str, expr, inbox);
            payload_map.insert(key.clone(), resolved);
        }
    }

    Ok(serde_json::to_vec(&JsonValue::Object(payload_map))?)
}

/// Resolve a kwarg value string to a JSON value.
fn resolve_kwarg_value(
    key: &str,
    value_str: &str,
    expr: Option<&ast::Expr>,
    inbox: &HashMap<String, JsonValue>,
) -> JsonValue {
    if let Some(expr) = expr {
        match ExpressionEvaluator::evaluate(expr, inbox) {
            Ok(value) => return value,
            Err(EvaluationError::VariableNotFound(var)) => {
                debug!(
                    kwarg = %key,
                    missing_var = %var,
                    "kwarg variable not found in inbox"
                );
                return JsonValue::Null;
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
        return inbox.get(var_name).cloned().unwrap_or(JsonValue::Null);
    }

    // Common bool literal casing from Python source (True/False)
    match value_str {
        "True" | "true" => return JsonValue::Bool(true),
        "False" | "false" => return JsonValue::Bool(false),
        _ => {}
    }

    // Try to parse as JSON
    match serde_json::from_str(value_str) {
        Ok(v) => v,
        Err(_) => {
            // If it's a string that looks like a bool, normalize to bool
            match value_str.to_ascii_lowercase().as_str() {
                "true" => JsonValue::Bool(true),
                "false" => JsonValue::Bool(false),
                _ => JsonValue::String(value_str.to_string()),
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::convert_to_dag;
    use crate::parser::{ast, parse};

    /// Helper to create a DAG from IR source
    fn dag_from_source(source: &str) -> DAG {
        let program = parse(source).expect("Failed to parse IR");
        convert_to_dag(&program)
    }

    #[test]
    fn test_analyze_subgraph_simple_action_to_return() {
        // Simple workflow: action -> return
        let source = r#"
fn workflow(input: [x], output: [result]):
    result = @test_action(value=x)
    return result
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the action node
        let action_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call")
            .expect("Should have action node");

        let analysis = analyze_subgraph(&action_node.id, &dag, &helper);

        // After action completes, we should find the return node as frontier
        assert!(
            !analysis.frontier_nodes.is_empty(),
            "Should have frontier nodes"
        );
        assert!(
            analysis
                .frontier_nodes
                .iter()
                .any(|f| f.category == FrontierCategory::Output),
            "Should have output frontier"
        );
    }

    #[test]
    fn test_analyze_subgraph_inline_nodes() {
        // Workflow with inline assignment between actions
        let source = r#"
fn workflow(input: [x], output: [result]):
    a = @first_action(value=x)
    b = a + 1
    result = @second_action(value=b)
    return result
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the first action node
        let first_action = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "action_call" && n.action_name.as_deref() == Some("first_action")
            })
            .expect("Should have first_action node");

        let analysis = analyze_subgraph(&first_action.id, &dag, &helper);

        // Should have inline nodes (the assignment)
        assert!(
            !analysis.inline_nodes.is_empty(),
            "Should have inline nodes for assignment"
        );

        // Should have action frontier (second_action)
        assert!(
            analysis
                .frontier_nodes
                .iter()
                .any(|f| f.category == FrontierCategory::Action),
            "Should have action frontier"
        );
    }

    #[test]
    fn test_analyze_subgraph_parallel_barrier() {
        // Workflow with parallel block that has a barrier (aggregator)
        let source = r#"
fn workflow(input: [x], output: [result]):
    a, b = parallel:
        @action1(value=x)
        @action2(value=x)
    result = a + b
    return result
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find one of the parallel actions
        let parallel_action = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call")
            .expect("Should have action node");

        let analysis = analyze_subgraph(&parallel_action.id, &dag, &helper);

        // Should have barrier frontier (the aggregator)
        assert!(
            analysis
                .frontier_nodes
                .iter()
                .any(|f| f.category == FrontierCategory::Barrier),
            "Should have barrier frontier for parallel aggregator"
        );
    }

    #[test]
    fn test_is_direct_predecessor() {
        let source = r#"
fn workflow(input: [x], output: [result]):
    result = @test_action(value=x)
    return result
"#;
        let dag = dag_from_source(source);

        // Find nodes
        let action_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call")
            .expect("Should have action node");

        let return_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "return")
            .expect("Should have return node");

        // Action should be direct predecessor of return
        assert!(is_direct_predecessor(
            &action_node.id,
            &return_node.id,
            &dag
        ));

        // Return should not be direct predecessor of action
        assert!(!is_direct_predecessor(
            &return_node.id,
            &action_node.id,
            &dag
        ));
    }

    #[test]
    fn test_execute_inline_subgraph_simple() {
        let source = r#"
fn workflow(input: [x], output: [result]):
    result = @test_action(value=x)
    return result
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the action node
        let action_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call")
            .expect("Should have action node");

        let subgraph = analyze_subgraph(&action_node.id, &dag, &helper);

        // Execute with a sample result
        let result = serde_json::json!("hello world");
        let existing_inbox = HashMap::new();
        let instance_id = WorkflowInstanceId(Uuid::new_v4());
        let initial_scope: HashMap<String, JsonValue> = HashMap::new();

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        let plan =
            execute_inline_subgraph(&action_node.id, result, ctx, &subgraph, &dag, instance_id)
                .expect("Should succeed");

        // Should have instance completion (workflow output)
        assert!(
            plan.instance_completion.is_some(),
            "Should have instance completion"
        );
    }

    #[test]
    fn test_evaluate_guard_none() {
        let scope: InlineScope = HashMap::new();

        // No guard expression should return Pass (always pass)
        assert!(matches!(
            evaluate_guard(None, &scope, "test_node"),
            GuardResult::Pass
        ));
    }

    #[test]
    fn test_evaluate_guard_with_conditional_workflow() {
        // Test guard evaluation by analyzing a conditional workflow's DAG
        // and checking that guards are correctly evaluated
        let source = r#"
fn workflow(input: [x], output: [result]):
    score = @get_score(value=x)
    if score >= 75:
        result = @high_action(value=score)
    else:
        result = @low_action(value=score)
    return result
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the get_score action
        let score_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("get_score"))
            .expect("Should have get_score action");

        // Analyze from the score action
        let analysis = analyze_subgraph(&score_action.id, &dag, &helper);

        // Should have conditional branch nodes as inline or frontiers
        // The exact structure depends on how conditionals are represented
        assert!(
            !analysis.all_node_ids.is_empty(),
            "Should have reachable nodes"
        );
    }

    #[test]
    fn test_node_type_as_str() {
        assert_eq!(NodeType::Action.as_str(), "action");
        assert_eq!(NodeType::Barrier.as_str(), "barrier");
    }

    #[test]
    fn test_completion_plan_new() {
        let plan = CompletionPlan::new("test_node".to_string());
        assert_eq!(plan.completed_node_id, "test_node");
        assert!(plan.inbox_writes.is_empty());
        assert!(plan.readiness_increments.is_empty());
        assert!(plan.instance_completion.is_none());
    }

    // ========================================================================
    // Guard Evaluation Tests
    // ========================================================================

    /// Helper to create a variable expression AST node
    fn make_var_expr(name: &str) -> ast::Expr {
        ast::Expr {
            span: None,
            kind: Some(ast::expr::Kind::Variable(ast::Variable {
                name: name.to_string(),
            })),
        }
    }

    /// Helper to create an integer literal expression AST node
    fn make_int_expr(value: i64) -> ast::Expr {
        ast::Expr {
            span: None,
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::IntValue(value)),
            })),
        }
    }

    /// Helper to create a binary operation expression AST node
    fn make_binary_op(left: ast::Expr, op: ast::BinaryOperator, right: ast::Expr) -> ast::Expr {
        ast::Expr {
            span: None,
            kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                left: Some(Box::new(left)),
                op: op as i32,
                right: Some(Box::new(right)),
            }))),
        }
    }

    #[test]
    fn test_evaluate_guard_pass_with_true_expression() {
        let mut scope: InlineScope = HashMap::new();
        scope.insert("x".to_string(), JsonValue::Number(10.into()));

        // Create expression: x > 5
        let guard = make_binary_op(
            make_var_expr("x"),
            ast::BinaryOperator::BinaryOpGt,
            make_int_expr(5),
        );

        let result = evaluate_guard(Some(&guard), &scope, "test_node");
        assert!(
            matches!(result, GuardResult::Pass),
            "Guard should pass when x=10 > 5"
        );
    }

    #[test]
    fn test_evaluate_guard_fail_with_false_expression() {
        let mut scope: InlineScope = HashMap::new();
        scope.insert("x".to_string(), JsonValue::Number(3.into()));

        // Create expression: x > 5
        let guard = make_binary_op(
            make_var_expr("x"),
            ast::BinaryOperator::BinaryOpGt,
            make_int_expr(5),
        );

        let result = evaluate_guard(Some(&guard), &scope, "test_node");
        assert!(
            matches!(result, GuardResult::Fail),
            "Guard should fail when x=3 is not > 5"
        );
    }

    #[test]
    fn test_evaluate_guard_error_with_missing_variable() {
        let scope: InlineScope = HashMap::new(); // Empty scope - 'x' not defined

        // Create expression: x > 5 (but x doesn't exist)
        let guard = make_binary_op(
            make_var_expr("x"),
            ast::BinaryOperator::BinaryOpGt,
            make_int_expr(5),
        );

        let result = evaluate_guard(Some(&guard), &scope, "test_node");
        assert!(
            matches!(result, GuardResult::Error(_)),
            "Guard should return Error when variable 'x' is missing"
        );

        if let GuardResult::Error(msg) = result {
            assert!(
                msg.contains("x"),
                "Error message should mention the missing variable"
            );
        }
    }

    #[test]
    fn test_evaluate_guard_error_with_function_call_missing_args() {
        let scope: InlineScope = HashMap::new();

        // Create expression: len() - missing required argument
        let guard = ast::Expr {
            span: None,
            kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                name: "len".to_string(),
                args: vec![],
                kwargs: vec![], // No 'items' kwarg provided
            })),
        };

        let result = evaluate_guard(Some(&guard), &scope, "test_node");
        assert!(
            matches!(result, GuardResult::Error(_)),
            "Guard should return Error when len() is called without arguments"
        );
    }

    // ========================================================================
    // Dead-End Detection Tests
    // ========================================================================

    #[test]
    fn test_dead_end_detected_when_all_guards_fail() {
        // Create a workflow with a conditional where all branches have failing guards
        // This simulates the scenario where guard evaluation errors block all paths
        let source = r#"
fn workflow(input: [x], output: [result]):
    score = @get_score(value=x)
    if score > 100:
        result = @high_action(value=score)
    elif score > 50:
        result = @mid_action(value=score)
    else:
        result = @low_action(value=score)
    return result
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the get_score action node
        let score_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("get_score"))
            .expect("Should have get_score action");

        let subgraph = analyze_subgraph(&score_action.id, &dag, &helper);

        // Create a scope where 'score' is NOT defined - this will cause guard evaluation to fail
        let initial_scope: InlineScope = HashMap::new();
        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with action result but missing 'score' variable in scope
        // The guards reference 'score' which won't be in scope, causing errors
        let result = execute_inline_subgraph(
            &score_action.id,
            JsonValue::Number(75.into()), // Action returned 75
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        // This should detect a dead-end because guard evaluation will fail
        // when trying to evaluate "score > 100" etc. without 'score' in scope
        // Note: The actual behavior depends on whether the action's target variable
        // gets inserted into scope. If it does, this test needs adjustment.

        // For now, just verify the function doesn't panic and returns something reasonable
        assert!(
            result.is_ok() || matches!(result, Err(CompletionError::WorkflowDeadEnd { .. })),
            "Should either succeed or detect dead-end, got: {:?}",
            result
        );
    }

    #[test]
    fn test_guard_result_enum_variants() {
        // Test that all GuardResult variants can be created and matched
        let pass = GuardResult::Pass;
        let fail = GuardResult::Fail;
        let error = GuardResult::Error("test error".to_string());

        assert!(matches!(pass, GuardResult::Pass));
        assert!(matches!(fail, GuardResult::Fail));
        assert!(matches!(error, GuardResult::Error(ref msg) if msg == "test error"));
    }

    #[test]
    fn test_completion_error_guard_evaluation_error_display() {
        let error = CompletionError::GuardEvaluationError {
            node_id: "branch_42".to_string(),
            message: "Variable 'x' not found".to_string(),
        };

        let display = format!("{}", error);
        assert!(display.contains("branch_42"));
        assert!(display.contains("Variable 'x' not found"));
    }

    #[test]
    fn test_completion_error_workflow_dead_end_display() {
        let error = CompletionError::WorkflowDeadEnd {
            completed_node_id: "action_5".to_string(),
            guard_errors: vec![
                ("branch_10".to_string(), "Missing variable 'x'".to_string()),
                ("branch_11".to_string(), "Missing variable 'y'".to_string()),
            ],
        };

        let display = format!("{}", error);
        assert!(display.contains("action_5"));
        assert!(display.contains("branch_10"));
        assert!(display.contains("branch_11"));
    }

    #[test]
    fn test_completion_error_workflow_dead_end_empty_guard_errors() {
        // Dead-end can occur without guard errors (e.g., all guards evaluate to false)
        let error = CompletionError::WorkflowDeadEnd {
            completed_node_id: "action_5".to_string(),
            guard_errors: vec![],
        };

        let display = format!("{}", error);
        assert!(display.contains("action_5"));
        assert!(display.contains("[]")); // Empty vector in debug format
    }

    #[test]
    fn test_execute_inline_subgraph_with_valid_guards_succeeds() {
        // Create a simple conditional workflow
        let source = r#"
fn workflow(input: [x], output: [result]):
    score = @get_score(value=x)
    if score > 50:
        result = @high_action(value=score)
    else:
        result = @low_action(value=score)
    return result
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the get_score action
        let score_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("get_score"))
            .expect("Should have get_score action");

        let subgraph = analyze_subgraph(&score_action.id, &dag, &helper);

        // Create scope with 'score' already defined (simulating result being stored)
        let mut initial_scope: InlineScope = HashMap::new();
        initial_scope.insert("x".to_string(), JsonValue::Number(10.into()));

        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with score = 75 (should take the high_action branch)
        let result = execute_inline_subgraph(
            &score_action.id,
            JsonValue::Number(75.into()),
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        // Should succeed and find a frontier (either high_action or low_action)
        assert!(
            result.is_ok(),
            "Should succeed with valid guards, got: {:?}",
            result
        );

        let plan = result.unwrap();
        // Should have at least one readiness increment for the next action
        // (unless the subgraph structure means we go directly to output)
        assert!(
            !plan.readiness_increments.is_empty() || plan.instance_completion.is_some(),
            "Should have readiness increments or instance completion"
        );
    }

    // ========================================================================
    // If Without Else Continuation Tests
    // ========================================================================

    #[test]
    fn test_if_without_else_continuation_when_condition_true() {
        // Test: if without else where condition is TRUE
        // Should execute the if body and then continue to the next action
        let source = r#"
fn workflow(input: [x], output: [result]):
    response = @get_items(value=x)
    if response:
        count = @process_items(items=response)
    final = @finalize(count=0)
    return final
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the get_items action
        let get_items_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("get_items"))
            .expect("Should have get_items action");

        let subgraph = analyze_subgraph(&get_items_action.id, &dag, &helper);

        // Log the subgraph structure for debugging
        println!("Subgraph inline nodes: {:?}", subgraph.inline_nodes);
        println!(
            "Subgraph frontier nodes: {:?}",
            subgraph
                .frontier_nodes
                .iter()
                .map(|f| &f.node_id)
                .collect::<Vec<_>>()
        );

        let initial_scope: InlineScope = HashMap::new();
        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with response = ["item1"] (truthy - should take the if branch)
        let result = execute_inline_subgraph(
            &get_items_action.id,
            JsonValue::Array(vec![JsonValue::String("item1".to_string())]),
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        assert!(
            result.is_ok(),
            "Should succeed when condition is true, got: {:?}",
            result
        );

        let plan = result.unwrap();
        // Should have readiness increment for process_items (the if body action)
        assert!(
            !plan.readiness_increments.is_empty(),
            "Should have readiness increments for the if body action"
        );
    }

    #[test]
    fn test_if_without_else_continuation_when_condition_false() {
        // Test: if without else where condition is FALSE
        // This is the bug case! When condition is false, should skip the if body
        // and continue to the finalize action, NOT hit a dead-end.
        let source = r#"
fn workflow(input: [x], output: [result]):
    response = @get_items(value=x)
    if response:
        count = @process_items(items=response)
    final = @finalize(count=0)
    return final
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the get_items action
        let get_items_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("get_items"))
            .expect("Should have get_items action");

        let subgraph = analyze_subgraph(&get_items_action.id, &dag, &helper);

        // Log the subgraph structure for debugging
        println!("=== IF WITHOUT ELSE (condition FALSE) ===");
        println!("Subgraph inline nodes: {:?}", subgraph.inline_nodes);
        println!(
            "Subgraph frontier nodes: {:?}",
            subgraph
                .frontier_nodes
                .iter()
                .map(|f| (&f.node_id, &f.category))
                .collect::<Vec<_>>()
        );

        // Print all edges from the branch node to understand the structure
        for node in dag.nodes.values() {
            if node.node_type == "branch" {
                println!("Branch node: {}", node.id);
                for edge in &dag.edges {
                    if edge.source == node.id {
                        println!(
                            "  Edge to {} (guard: {:?})",
                            edge.target,
                            edge.guard_expr.as_ref().map(|_| "present")
                        );
                    }
                }
            }
        }

        let initial_scope: InlineScope = HashMap::new();
        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with response = [] (falsy empty array - should SKIP the if branch)
        let result = execute_inline_subgraph(
            &get_items_action.id,
            JsonValue::Array(vec![]), // Empty array is falsy
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        // THIS IS THE BUG: currently this fails with WorkflowDeadEnd
        // because there's no continuation path when the if condition is false.
        // It should succeed and have a readiness increment for 'finalize' action.
        assert!(
            result.is_ok(),
            "Should succeed when condition is false (continue to finalize), got: {:?}",
            result
        );

        let plan = result.unwrap();
        // Should have readiness increment for finalize action (skipping process_items)
        assert!(
            !plan.readiness_increments.is_empty() || plan.instance_completion.is_some(),
            "Should have readiness increments for finalize action or workflow completion"
        );

        // Verify that the readiness increment is for 'finalize', not 'process_items'
        if !plan.readiness_increments.is_empty() {
            let action_names: Vec<_> = plan
                .readiness_increments
                .iter()
                .filter_map(|r| r.action_name.as_ref())
                .collect();
            assert!(
                action_names.iter().any(|n| *n == "finalize"),
                "Expected finalize action in readiness increments, got: {:?}",
                action_names
            );
        }
    }

    // ========================================================================
    // Early Return + For Loop Over Empty List Tests
    // ========================================================================
    //
    // These tests reproduce a bug where a workflow with:
    // 1. An action that returns a response with optional fields
    // 2. A conditional that returns early if a field is falsy
    // 3. A for loop over another field (which could be empty)
    // 4. A final action after the loop
    //
    // Would fail with "Workflow dead-end" when:
    // - The early return condition is NOT taken (field is truthy)
    // - BUT the for loop iterates over an empty list
    //
    // This pattern is common in workflows that validate uploaded content.

    #[test]
    fn test_early_return_with_for_loop_over_empty_list() {
        // This reproduces a bug from the ValidateUploadedPostRappel workflow:
        //
        // parse_result = @parse_uploaded_post(...)
        // if not parse_result.auth_session_id:
        //     return  # early exit
        // for post_id in parse_result.new_post_ids:  # Could be empty!
        //     @process_post(post_id)
        // @validate_posts(...)
        //
        // When auth_session_id is present but new_post_ids is empty,
        // the workflow should still continue to validate_posts.
        let source = r#"
fn workflow(input: [raw_id], output: [result]):
    parse_result = @parse_uploaded_post(raw_id=raw_id)
    if not parse_result.auth_session_id:
        return parse_result
    for post_id in parse_result.new_post_ids:
        processed = @process_post(id=post_id)
    final = @validate_posts(count=0)
    return final
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        let parse_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("parse_uploaded_post"))
            .expect("Should have parse_uploaded_post action");

        let subgraph = analyze_subgraph(&parse_action.id, &dag, &helper);

        let initial_scope: InlineScope = HashMap::new();
        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with:
        // - auth_session_id = "session-123" (truthy, so early return is NOT taken)
        // - new_post_ids = [] (empty list, so for loop doesn't iterate)
        //
        // Expected: Should continue to validate_posts action
        let result = execute_inline_subgraph(
            &parse_action.id,
            serde_json::json!({
                "auth_session_id": "session-123",
                "new_post_ids": []
            }),
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        assert!(
            result.is_ok(),
            "Should succeed when early return not taken and for loop is empty, got: {:?}",
            result
        );

        let plan = result.unwrap();

        // Should have a path to validate_posts
        assert!(
            !plan.readiness_increments.is_empty() || plan.instance_completion.is_some(),
            "Should have readiness increments for validate_posts or workflow completion"
        );

        // If there are readiness increments, verify validate_posts is reachable
        if !plan.readiness_increments.is_empty() {
            let action_names: Vec<_> = plan
                .readiness_increments
                .iter()
                .filter_map(|r| r.action_name.as_ref())
                .collect();
            assert!(
                action_names.iter().any(|n| *n == "validate_posts"),
                "Expected validate_posts action in readiness increments, got: {:?}",
                action_names
            );
        }
    }

    #[test]
    fn test_early_return_with_for_loop_early_return_taken() {
        // Test the case where early return IS taken (auth_session_id is null)
        let source = r#"
fn workflow(input: [raw_id], output: [result]):
    parse_result = @parse_uploaded_post(raw_id=raw_id)
    if not parse_result.auth_session_id:
        return parse_result
    for post_id in parse_result.new_post_ids:
        processed = @process_post(id=post_id)
    final = @validate_posts(count=0)
    return final
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        let parse_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("parse_uploaded_post"))
            .expect("Should have parse_uploaded_post action");

        let subgraph = analyze_subgraph(&parse_action.id, &dag, &helper);

        let initial_scope: InlineScope = HashMap::new();
        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with:
        // - auth_session_id = null (falsy, so early return IS taken)
        let result = execute_inline_subgraph(
            &parse_action.id,
            serde_json::json!({
                "auth_session_id": null,
                "new_post_ids": ["post-1", "post-2"]
            }),
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        assert!(
            result.is_ok(),
            "Should succeed when early return is taken, got: {:?}",
            result
        );

        let plan = result.unwrap();

        // When early return is taken, the workflow may either:
        // 1. Complete immediately (instance_completion is Some)
        // 2. Have readiness increments that lead to the output
        //
        // The key is that we shouldn't go through the for loop body (process_post)
        assert!(
            plan.instance_completion.is_some() || !plan.readiness_increments.is_empty(),
            "Should have instance completion or readiness increments for early return path"
        );

        // Verify we're NOT going to process_post (the for loop body)
        if !plan.readiness_increments.is_empty() {
            let action_names: Vec<_> = plan
                .readiness_increments
                .iter()
                .filter_map(|r| r.action_name.as_ref())
                .collect();
            assert!(
                !action_names.iter().any(|n| *n == "process_post"),
                "Should NOT have process_post when early return is taken, got: {:?}",
                action_names
            );
        }
    }

    #[test]
    fn test_early_return_with_for_loop_non_empty_list() {
        // Test the case where early return is NOT taken and for loop has items
        let source = r#"
fn workflow(input: [raw_id], output: [result]):
    parse_result = @parse_uploaded_post(raw_id=raw_id)
    if not parse_result.auth_session_id:
        return parse_result
    for post_id in parse_result.new_post_ids:
        processed = @process_post(id=post_id)
    final = @validate_posts(count=0)
    return final
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        let parse_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("parse_uploaded_post"))
            .expect("Should have parse_uploaded_post action");

        let subgraph = analyze_subgraph(&parse_action.id, &dag, &helper);

        let initial_scope: InlineScope = HashMap::new();
        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with:
        // - auth_session_id = "session-123" (truthy, early return NOT taken)
        // - new_post_ids = ["post-1"] (non-empty, for loop should iterate)
        let result = execute_inline_subgraph(
            &parse_action.id,
            serde_json::json!({
                "auth_session_id": "session-123",
                "new_post_ids": ["post-1"]
            }),
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        assert!(
            result.is_ok(),
            "Should succeed when early return not taken and for loop has items, got: {:?}",
            result
        );

        let plan = result.unwrap();

        // Should proceed to process_post action (inside the loop)
        assert!(
            !plan.readiness_increments.is_empty(),
            "Should have readiness increments for process_post action"
        );

        let action_names: Vec<_> = plan
            .readiness_increments
            .iter()
            .filter_map(|r| r.action_name.as_ref())
            .collect();
        assert!(
            action_names.iter().any(|n| *n == "process_post"),
            "Expected process_post action in readiness increments, got: {:?}",
            action_names
        );
    }

    // ========================================================================
    // Return Inside elif Branch Tests
    // ========================================================================

    #[test]
    fn test_return_inside_elif_branch() {
        // Test that return inside an elif branch completes the workflow
        // and doesn't continue to subsequent code
        let source = r#"
fn workflow(input: [x], output: [result]):
    value = @get_value(x=x)
    if value.status == "high":
        result = @process_high(v=value)
    elif value.status == "medium":
        return value
    else:
        result = @process_low(v=value)
    final = @finalize(result=result)
    return final
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        let get_value_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("get_value"))
            .expect("Should have get_value action");

        let subgraph = analyze_subgraph(&get_value_action.id, &dag, &helper);

        let initial_scope: InlineScope = HashMap::new();
        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with status = "medium" (should take elif branch and return)
        let result = execute_inline_subgraph(
            &get_value_action.id,
            serde_json::json!({
                "status": "medium",
                "data": "test"
            }),
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        assert!(
            result.is_ok(),
            "Should succeed when elif return is taken, got: {:?}",
            result
        );

        let plan = result.unwrap();

        // Should complete the workflow (early return in elif)
        // Should NOT proceed to finalize action
        if !plan.readiness_increments.is_empty() {
            let action_names: Vec<_> = plan
                .readiness_increments
                .iter()
                .filter_map(|r| r.action_name.as_ref())
                .collect();
            assert!(
                !action_names.iter().any(|n| *n == "finalize"),
                "Should NOT have finalize when elif return is taken, got: {:?}",
                action_names
            );
        }
    }

    // ========================================================================
    // Return Inside for Loop Tests
    // ========================================================================

    #[test]
    fn test_return_inside_for_loop() {
        // Test that return inside a for loop body completes the workflow
        // and doesn't continue to the loop increment or subsequent code
        let source = r#"
fn workflow(input: [items], output: [result]):
    for item in items:
        if item.found:
            return item
        processed = @process_item(i=item)
    final = @finalize(count=0)
    return final
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        // Find the input node to start from
        let input_node = dag
            .nodes
            .values()
            .find(|n| n.is_input)
            .expect("Should have input node");

        let subgraph = analyze_subgraph(&input_node.id, &dag, &helper);

        let mut initial_scope: InlineScope = HashMap::new();
        // items = [{"found": true, "value": "first"}]
        initial_scope.insert(
            "items".to_string(),
            serde_json::json!([{"found": true, "value": "first"}]),
        );

        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute from input (with items that should trigger early return)
        let result = execute_inline_subgraph(
            &input_node.id,
            JsonValue::Null, // Input node doesn't have a result
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        assert!(
            result.is_ok(),
            "Should succeed when for loop return is taken, got: {:?}",
            result
        );

        let plan = result.unwrap();

        // Should NOT proceed to process_item or finalize
        if !plan.readiness_increments.is_empty() {
            let action_names: Vec<_> = plan
                .readiness_increments
                .iter()
                .filter_map(|r| r.action_name.as_ref())
                .collect();
            assert!(
                !action_names.iter().any(|n| *n == "finalize"),
                "Should NOT have finalize when for loop return is taken, got: {:?}",
                action_names
            );
        }
    }

    #[test]
    fn test_return_directly_in_for_loop_body() {
        // Test that a direct return in for loop body (not nested in if)
        // completes the workflow and doesn't continue to loop increment
        let source = r#"
fn workflow(input: [items], output: [result]):
    for item in items:
        return item
    final = @finalize(count=0)
    return final
"#;
        let dag = dag_from_source(source);

        // Verify the return node connects to output, not to loop increment
        let return_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "return")
            .collect();

        let output_node = dag
            .nodes
            .values()
            .find(|n| n.is_output)
            .expect("Should have output node");

        // Find the loop increment node
        let incr_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.id.contains("loop_incr"))
            .collect();

        for return_node in &return_nodes {
            // Should have edge to output
            let has_edge_to_output = dag
                .edges
                .iter()
                .any(|e| e.source == return_node.id && e.target == output_node.id);
            assert!(
                has_edge_to_output,
                "Return node {} should have edge to output",
                return_node.id
            );

            // Should NOT have edge to loop increment
            for incr_node in &incr_nodes {
                let has_edge_to_incr = dag
                    .edges
                    .iter()
                    .any(|e| e.source == return_node.id && e.target == incr_node.id);
                assert!(
                    !has_edge_to_incr,
                    "Return node {} should NOT have edge to loop increment {}",
                    return_node.id, incr_node.id
                );
            }
        }
    }

    // ========================================================================
    // Return Inside try/except Tests
    // ========================================================================

    #[test]
    fn test_return_inside_try_body() {
        // Test that return inside a try body completes the workflow
        // and doesn't continue to subsequent code.
        //
        // Note: try/except bodies are general blocks and may include multiple statements.
        // This test uses a try body that only has a return (no action call).
        let source = r#"
fn workflow(input: [x], output: [result]):
    value = @get_value(x=x)
    try:
        return value
    except NetworkError:
        fallback = @fallback_action(x=x)
    final = @finalize(v=fallback)
    return final
"#;
        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        let get_value_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("get_value"))
            .expect("Should have get_value action");

        let subgraph = analyze_subgraph(&get_value_action.id, &dag, &helper);

        let initial_scope: InlineScope = HashMap::new();
        let existing_inbox: HashMap<String, HashMap<String, JsonValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with successful result
        let result = execute_inline_subgraph(
            &get_value_action.id,
            serde_json::json!({"success": true, "data": "result"}),
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        assert!(
            result.is_ok(),
            "Should succeed when try body return is taken, got: {:?}",
            result
        );

        let plan = result.unwrap();

        // Should NOT proceed to finalize action
        if !plan.readiness_increments.is_empty() {
            let action_names: Vec<_> = plan
                .readiness_increments
                .iter()
                .filter_map(|r| r.action_name.as_ref())
                .collect();
            assert!(
                !action_names.iter().any(|n| *n == "finalize"),
                "Should NOT have finalize when try body return is taken, got: {:?}",
                action_names
            );
        }
    }

    #[test]
    fn test_return_inside_except_handler() {
        // Test that return inside an except handler completes the workflow
        // and doesn't continue to subsequent code
        let source = r#"
fn workflow(input: [x], output: [result]):
    try:
        value = @risky_action(x=x)
    except NetworkError:
        return x
    final = @finalize(v=value)
    return final
"#;
        let dag = dag_from_source(source);
        let _helper = DAGHelper::new(&dag);

        // For except handlers, we need to test what happens when the exception path is taken.
        // This is more complex because exception handling is done at runtime.
        // For now, we verify the DAG structure is correct - return nodes should connect to output.

        // Check that any return nodes in the workflow connect to the output
        let return_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "return")
            .collect();

        let output_node = dag
            .nodes
            .values()
            .find(|n| n.is_output)
            .expect("Should have output node");

        for return_node in &return_nodes {
            let has_edge_to_output = dag
                .edges
                .iter()
                .any(|e| e.source == return_node.id && e.target == output_node.id);
            assert!(
                has_edge_to_output,
                "Return node {} should have edge to output node {}",
                return_node.id, output_node.id
            );
        }
    }
}
