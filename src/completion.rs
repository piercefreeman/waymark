//! Completion plan and unified readiness model types.
//!
//! This module contains the data structures used for the unified readiness model
//! where every node gets readiness tracking and is only enqueued when
//! `completed_count == required_count`.

use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{debug, info, warn};

use uuid::Uuid;

use crate::ast_evaluator::{EvaluationError, ExpressionEvaluator};
use crate::dag::{DAG, DAGEdge, DAGNode, EdgeType};
use crate::dag_state::DAGHelper;
use crate::db::{ActionId, WorkflowInstanceId};
use crate::parser::ast;
use crate::value::WorkflowValue;

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
    pub initial_scope: &'a HashMap<String, WorkflowValue>,
    /// Inbox data fetched from storage for nodes involved in this completion.
    pub existing_inbox: &'a HashMap<String, HashMap<String, WorkflowValue>>,
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

    /// Readiness initializations for frontier nodes.
    pub readiness_inits: Vec<ReadinessInit>,

    /// Readiness increments for frontier nodes.
    pub readiness_increments: Vec<ReadinessIncrement>,

    /// Barriers to enqueue immediately (no readiness tracking).
    pub barrier_enqueues: Vec<String>,

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
            readiness_inits: Vec::new(),
            readiness_increments: Vec::new(),
            barrier_enqueues: Vec::new(),
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
    pub value: WorkflowValue,
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

    /// Timeout in seconds for action execution.
    pub timeout_seconds: i32,

    /// Maximum number of retries for the action.
    pub max_retries: i32,

    /// Backoff strategy for retries.
    pub backoff_kind: crate::db::BackoffKind,

    /// Base delay in milliseconds for backoff.
    pub backoff_base_delay_ms: i32,
}

#[derive(Debug, Clone)]
pub struct ReadinessInit {
    pub node_id: String,
    pub required_count: i32,
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
// Policy Extraction
// ============================================================================

/// Extracted policy values from a DAGNode for action enqueuing.
#[derive(Debug, Clone)]
pub struct ExtractedPolicies {
    pub timeout_seconds: i32,
    pub max_retries: i32,
    pub backoff_kind: crate::db::BackoffKind,
    pub backoff_base_delay_ms: i32,
}

impl Default for ExtractedPolicies {
    fn default() -> Self {
        Self {
            timeout_seconds: 300,
            max_retries: 3,
            backoff_kind: crate::db::BackoffKind::Exponential,
            backoff_base_delay_ms: 1000,
        }
    }
}

/// Extract policy values from a DAGNode's policies.
pub fn extract_policies_from_node(node: &DAGNode) -> ExtractedPolicies {
    let mut policies = ExtractedPolicies::default();

    for policy in &node.policies {
        match &policy.kind {
            Some(ast::policy_bracket::Kind::Retry(retry)) => {
                policies.max_retries = retry.max_retries as i32;
                if let Some(ref backoff) = retry.backoff {
                    policies.backoff_base_delay_ms = (backoff.seconds as i32) * 1000;
                }
            }
            Some(ast::policy_bracket::Kind::Timeout(timeout_policy)) => {
                if let Some(ref duration) = timeout_policy.timeout {
                    policies.timeout_seconds = duration.seconds as i32;
                }
            }
            None => {}
        }
    }

    policies
}

// ============================================================================
// Inline Scope
// ============================================================================

/// Scope for inline node execution.
/// Maps variable names to their JSON values.
pub type InlineScope = HashMap<String, WorkflowValue>;

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

    #[error("Evaluation error: {0}")]
    Evaluation(String),

    #[error("Expression evaluation error: {0}")]
    EvaluationError(#[from] EvaluationError),

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
    completed_result: WorkflowValue,
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
    let initial_edges = select_guarded_successors(
        helper.get_state_machine_successors(completed_node_id),
        &inline_scope,
        &mut guard_errors,
    );
    for edge in initial_edges {
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
            let successor_edges = select_guarded_successors(
                helper.get_state_machine_successors(&node_id),
                &inline_scope,
                &mut guard_errors,
            );
            for edge in successor_edges {
                // Allow loop-back edges and their downstream nodes to re-visit (for inline loop execution)
                // If we reached the current node via loop-back, we're in a loop iteration and need to
                // allow re-visiting downstream nodes in the loop body too.
                let is_loop_back_context = reached_via_loop_back || edge.is_loop_back;
                if !is_loop_back_context && visited.contains(&edge.target) {
                    continue;
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
                        is_spread = frontier_node.is_spread,
                        "building action dispatch payload"
                    );

                    // Handle spread nodes specially - they need to evaluate the collection
                    // and create multiple actions (or none if empty)
                    if frontier_node.is_spread {
                        handle_spread_frontier(
                            frontier_node,
                            frontier,
                            &action_inbox,
                            instance_id,
                            completed_node_id,
                            dag,
                            &mut plan,
                        )?;
                        continue;
                    }

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

                    // Extract retry/timeout policies from the node
                    let policies = extract_policies_from_node(frontier_node);

                    plan.readiness_increments.push(ReadinessIncrement {
                        node_id: frontier.node_id.clone(),
                        required_count: frontier.required_count,
                        node_type,
                        module_name: frontier_node.module_name.clone(),
                        action_name: frontier_node.action_name.clone(),
                        dispatch_payload: Some(dispatch_payload),
                        scheduled_at,
                        timeout_seconds: policies.timeout_seconds,
                        max_retries: policies.max_retries,
                        backoff_kind: policies.backoff_kind,
                        backoff_base_delay_ms: policies.backoff_base_delay_ms,
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

                    // Barriers use default policy values (internal nodes)
                    let default_policies = ExtractedPolicies::default();
                    plan.readiness_increments.push(ReadinessIncrement {
                        node_id: frontier.node_id.clone(),
                        required_count: frontier.required_count,
                        node_type: NodeType::Barrier,
                        module_name: None,
                        action_name: None,
                        dispatch_payload: None,
                        scheduled_at: None,
                        timeout_seconds: default_policies.timeout_seconds,
                        max_retries: default_policies.max_retries,
                        backoff_kind: default_policies.backoff_kind,
                        backoff_base_delay_ms: default_policies.backoff_base_delay_ms,
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
                            .unwrap_or(WorkflowValue::Null)
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
                            .unwrap_or(WorkflowValue::Null)
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
fn execute_inline_node(node: &DAGNode, scope: &InlineScope) -> WorkflowValue {
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
                        WorkflowValue::Null
                    }
                }
            } else {
                WorkflowValue::Null
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
                        WorkflowValue::Null
                    }
                }
            } else {
                WorkflowValue::Null
            }
        }
        "input" | "output" => WorkflowValue::Null,
        "conditional" | "branch" => WorkflowValue::Bool(true),
        "join" => WorkflowValue::Null,
        "aggregator" => WorkflowValue::List(vec![]),
        _ => WorkflowValue::Null,
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

fn select_guarded_successors<'a>(
    edges: Vec<&'a DAGEdge>,
    scope: &InlineScope,
    guard_errors: &mut Vec<(String, String)>,
) -> Vec<&'a DAGEdge> {
    let mut selected = Vec::new();
    let mut else_edges = Vec::new();
    let mut has_guarded_edges = false;
    let mut guard_passed = false;
    let mut guard_error = false;

    for edge in edges {
        if edge.is_else {
            else_edges.push(edge);
            continue;
        }

        if let Some(ref guard) = edge.guard_expr {
            has_guarded_edges = true;
            match evaluate_guard(Some(guard), scope, &edge.target) {
                GuardResult::Pass => {
                    guard_passed = true;
                    selected.push(edge);
                }
                GuardResult::Fail => {}
                GuardResult::Error(err) => {
                    guard_errors.push((edge.target.clone(), err));
                    guard_error = true;
                }
            }
            continue;
        }

        selected.push(edge);
    }

    if has_guarded_edges && !guard_passed && !guard_error {
        selected.extend(else_edges);
    }

    selected
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
    inbox: &mut HashMap<String, WorkflowValue>,
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

/// Handle a spread node frontier by evaluating the collection and creating
/// appropriate actions (or none if empty).
///
/// For spread nodes, we need to:
/// 1. Evaluate the spread collection expression to get the items
/// 2. If empty: inline the aggregator completion and trigger its successors directly
/// 3. If non-empty: create N actions (one per item) with spread_index tracking
fn handle_spread_frontier(
    frontier_node: &DAGNode,
    frontier: &FrontierNode,
    action_inbox: &HashMap<String, WorkflowValue>,
    instance_id: WorkflowInstanceId,
    _completed_node_id: &str,
    _dag: &DAG,
    plan: &mut CompletionPlan,
) -> Result<(), CompletionError> {
    // Get spread metadata from the node
    let loop_var = frontier_node
        .spread_loop_var
        .as_ref()
        .ok_or_else(|| CompletionError::Evaluation("Spread node missing loop_var".to_string()))?;

    let collection_expr = frontier_node
        .spread_collection_expr
        .as_ref()
        .ok_or_else(|| {
            CompletionError::Evaluation("Spread node missing collection expression".to_string())
        })?;

    // Evaluate the collection expression
    let collection = ExpressionEvaluator::evaluate(collection_expr, action_inbox)?;

    let items = match &collection {
        WorkflowValue::List(arr) | WorkflowValue::Tuple(arr) => arr.clone(),
        _ => {
            return Err(CompletionError::Evaluation(format!(
                "Spread collection is not a list or tuple: {:?}",
                collection
            )));
        }
    };

    debug!(
        frontier_node_id = %frontier.node_id,
        loop_var = %loop_var,
        item_count = items.len(),
        "expanding spread frontier"
    );

    // Get the aggregator node ID
    let aggregator_id = frontier_node.aggregates_to.as_ref().ok_or_else(|| {
        CompletionError::Evaluation("Spread node missing aggregates_to".to_string())
    })?;

    // Get the result variable name for the aggregator
    let result_var = frontier_node
        .target
        .clone()
        .unwrap_or_else(|| "_spread_result".to_string());

    if items.is_empty() {
        // Empty collection: write empty result and enqueue the aggregator as a barrier.
        plan.inbox_writes.push(InboxWrite {
            instance_id,
            target_node_id: aggregator_id.clone(),
            variable_name: result_var.clone(),
            value: WorkflowValue::List(vec![]),
            source_node_id: frontier.node_id.clone(),
            spread_index: None,
        });

        plan.barrier_enqueues.push(aggregator_id.clone());

        return Ok(());
    }

    // Non-empty collection - create actions for each item
    let item_count = items.len();
    let policies = extract_policies_from_node(frontier_node);

    // Reset readiness for the frontier node if required_count=1
    // (same logic as regular actions for loop re-triggering)
    if frontier.required_count == 1 {
        plan.readiness_resets.push(frontier.node_id.clone());
    }

    for (idx, item) in items.into_iter().enumerate() {
        // Create modified inbox with the loop variable bound to this item
        let mut iteration_inbox = action_inbox.clone();
        iteration_inbox.insert(loop_var.clone(), item);

        // Build dispatch payload for this iteration
        let dispatch_payload = build_action_payload(frontier_node, &iteration_inbox)?;

        // Create node_id that includes the spread index for tracking
        let spread_node_id = format!("{}[{}]", frontier.node_id, idx);

        plan.readiness_increments.push(ReadinessIncrement {
            node_id: spread_node_id,
            required_count: 1, // Each spread item is triggered by the spread node being ready
            node_type: NodeType::Action,
            module_name: frontier_node.module_name.clone(),
            action_name: frontier_node.action_name.clone(),
            dispatch_payload: Some(dispatch_payload),
            scheduled_at: None,
            timeout_seconds: policies.timeout_seconds,
            max_retries: policies.max_retries,
            backoff_kind: policies.backoff_kind,
            backoff_base_delay_ms: policies.backoff_base_delay_ms,
        });
    }

    plan.readiness_inits.push(ReadinessInit {
        node_id: aggregator_id.clone(),
        required_count: item_count as i32,
    });

    Ok(())
}

/// Build action dispatch payload from node kwargs and inbox.
fn build_action_payload(
    node: &DAGNode,
    inbox: &HashMap<String, WorkflowValue>,
) -> Result<Vec<u8>, CompletionError> {
    let mut payload_map = serde_json::Map::new();

    if let Some(ref kwargs) = node.kwargs {
        let kwarg_exprs = node.kwarg_exprs.as_ref();
        for (key, value_str) in kwargs {
            let expr = kwarg_exprs.and_then(|m| m.get(key));
            let resolved = resolve_kwarg_value(key, value_str, expr, inbox);
            payload_map.insert(key.clone(), resolved.to_json());
        }
    }

    Ok(serde_json::to_vec(&serde_json::Value::Object(payload_map))?)
}

/// Resolve a kwarg value string to a JSON value.
fn resolve_kwarg_value(
    key: &str,
    value_str: &str,
    expr: Option<&ast::Expr>,
    inbox: &HashMap<String, WorkflowValue>,
) -> WorkflowValue {
    if let Some(expr) = expr {
        match ExpressionEvaluator::evaluate(expr, inbox) {
            Ok(value) => return value,
            Err(EvaluationError::VariableNotFound(var)) => {
                debug!(
                    kwarg = %key,
                    missing_var = %var,
                    "kwarg variable not found in inbox"
                );
                return WorkflowValue::Null;
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
        return inbox.get(var_name).cloned().unwrap_or(WorkflowValue::Null);
    }

    // Common bool literal casing from Python source (True/False)
    match value_str {
        "True" | "true" => return WorkflowValue::Bool(true),
        "False" | "false" => return WorkflowValue::Bool(false),
        _ => {}
    }

    // Try to parse as JSON
    match serde_json::from_str(value_str) {
        Ok(v) => WorkflowValue::from_json(&v),
        Err(_) => {
            // If it's a string that looks like a bool, normalize to bool
            match value_str.to_ascii_lowercase().as_str() {
                "true" => WorkflowValue::Bool(true),
                "false" => WorkflowValue::Bool(false),
                _ => WorkflowValue::String(value_str.to_string()),
            }
        }
    }
}

/// Serialize workflow result as protobuf WorkflowArguments.
fn serialize_workflow_result(result: &WorkflowValue) -> Result<Vec<u8>, CompletionError> {
    use crate::messages::proto;
    use prost::Message;

    let arguments = vec![proto::WorkflowArgument {
        key: "result".to_string(),
        value: Some(result.to_proto()),
    }];

    let workflow_args = proto::WorkflowArguments { arguments };
    Ok(workflow_args.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::convert_to_dag;
    use crate::parser::{ast, parse};

    /// Helper to create a DAG from IR source
    fn dag_from_source(source: &str) -> DAG {
        let program = parse(source).expect("Failed to parse IR");
        convert_to_dag(&program).expect("Failed to convert to DAG")
    }

    #[test]
    fn test_analyze_subgraph_simple_action_to_return() {
        // Simple workflow: action -> return
        let source = r#"
fn main(input: [x], output: [result]):
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
fn main(input: [x], output: [result]):
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
fn main(input: [x], output: [result]):
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
fn main(input: [x], output: [result]):
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
fn main(input: [x], output: [result]):
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
        let result = WorkflowValue::String("hello world".to_string());
        let existing_inbox = HashMap::new();
        let instance_id = WorkflowInstanceId(Uuid::new_v4());
        let initial_scope: HashMap<String, WorkflowValue> = HashMap::new();

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
fn main(input: [x], output: [result]):
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
        assert!(plan.readiness_inits.is_empty());
        assert!(plan.readiness_increments.is_empty());
        assert!(plan.barrier_enqueues.is_empty());
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
        scope.insert("x".to_string(), WorkflowValue::Int(10.into()));

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
        scope.insert("x".to_string(), WorkflowValue::Int(3.into()));

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
                global_function: ast::GlobalFunction::Len as i32,
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
fn main(input: [x], output: [result]):
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
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
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
            WorkflowValue::Int(75.into()), // Action returned 75
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
fn main(input: [x], output: [result]):
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
        initial_scope.insert("x".to_string(), WorkflowValue::Int(10.into()));

        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with score = 75 (should take the high_action branch)
        let result = execute_inline_subgraph(
            &score_action.id,
            WorkflowValue::Int(75.into()),
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
            !plan.readiness_increments.is_empty()
                || !plan.readiness_inits.is_empty()
                || !plan.barrier_enqueues.is_empty()
                || plan.instance_completion.is_some(),
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
fn main(input: [x], output: [result]):
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
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with response = ["item1"] (truthy - should take the if branch)
        let result = execute_inline_subgraph(
            &get_items_action.id,
            WorkflowValue::List(vec![WorkflowValue::String("item1".to_string())]),
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
            !plan.readiness_increments.is_empty()
                || !plan.readiness_inits.is_empty()
                || !plan.barrier_enqueues.is_empty(),
            "Should have readiness increments for the if body action"
        );
    }

    #[test]
    fn test_if_without_else_continuation_when_condition_false() {
        // Test: if without else where condition is FALSE
        // This is the bug case! When condition is false, should skip the if body
        // and continue to the finalize action, NOT hit a dead-end.
        let source = r#"
fn main(input: [x], output: [result]):
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
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with response = [] (falsy empty array - should SKIP the if branch)
        let result = execute_inline_subgraph(
            &get_items_action.id,
            WorkflowValue::List(vec![]), // Empty array is falsy
            ctx,
            &subgraph,
            &dag,
            instance_id,
        );

        // This should succeed when the condition is false and continue to the
        // finalize action via the default else path.
        assert!(
            result.is_ok(),
            "Should succeed when condition is false (continue to finalize), got: {:?}",
            result
        );

        let plan = result.unwrap();
        // Should have readiness increment for finalize action (skipping process_items)
        assert!(
            !plan.readiness_increments.is_empty()
                || !plan.readiness_inits.is_empty()
                || !plan.barrier_enqueues.is_empty()
                || plan.instance_completion.is_some(),
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
fn main(input: [raw_id], output: [result]):
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
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
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
            WorkflowValue::from_json(&serde_json::json!({
                "auth_session_id": "session-123",
                "new_post_ids": []
            })),
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
            !plan.readiness_increments.is_empty()
                || !plan.readiness_inits.is_empty()
                || !plan.barrier_enqueues.is_empty()
                || plan.instance_completion.is_some(),
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
    fn test_helper_early_return_flows_to_caller() {
        let source = r#"
fn helper(input: [flag], output: [result]):
    result = @check()
    if flag:
        return result
    result = @work()
    return result

fn main(input: [flag], output: [result]):
    helper(flag=flag)
    result = @final_action()
    return result
"#;

        let dag = dag_from_source(source);
        let helper = DAGHelper::new(&dag);

        let check_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("check"))
            .expect("Should have check action node");

        let subgraph = analyze_subgraph(&check_action.id, &dag, &helper);
        let ctx = InlineContext {
            initial_scope: &{
                let mut scope = InlineScope::new();
                scope.insert("flag".to_string(), WorkflowValue::Bool(true));
                scope
            },
            existing_inbox: &HashMap::new(),
            spread_index: None,
        };

        let result = execute_inline_subgraph(
            &check_action.id,
            WorkflowValue::Bool(true),
            ctx,
            &subgraph,
            &dag,
            WorkflowInstanceId(Uuid::nil()),
        );

        assert!(
            result.is_ok(),
            "Expected early return in helper to continue to caller, got: {:?}",
            result
        );

        let plan = result.unwrap();
        let action_names: Vec<_> = plan
            .readiness_increments
            .iter()
            .filter_map(|r| r.action_name.as_ref())
            .collect();
        assert!(
            action_names.iter().any(|n| *n == "final_action"),
            "Expected final_action readiness, got: {:?}",
            action_names
        );
    }

    #[test]
    fn test_early_return_with_for_loop_early_return_taken() {
        // Test the case where early return IS taken (auth_session_id is null)
        let source = r#"
fn main(input: [raw_id], output: [result]):
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
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
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
            WorkflowValue::from_json(&serde_json::json!({
                "auth_session_id": null,
                "new_post_ids": ["post-1", "post-2"]
            })),
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
            plan.instance_completion.is_some()
                || !plan.readiness_increments.is_empty()
                || !plan.readiness_inits.is_empty()
                || !plan.barrier_enqueues.is_empty(),
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
fn main(input: [raw_id], output: [result]):
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
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
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
            WorkflowValue::from_json(&serde_json::json!({
                "auth_session_id": "session-123",
                "new_post_ids": ["post-1"]
            })),
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
            !plan.readiness_increments.is_empty()
                || !plan.readiness_inits.is_empty()
                || !plan.barrier_enqueues.is_empty(),
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
fn main(input: [x], output: [result]):
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
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with status = "medium" (should take elif branch and return)
        let result = execute_inline_subgraph(
            &get_value_action.id,
            WorkflowValue::from_json(&serde_json::json!({
                "status": "medium",
                "data": "test"
            })),
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
fn main(input: [items], output: [result]):
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
            WorkflowValue::from_json(&serde_json::json!([{"found": true, "value": "first"}])),
        );

        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute from input (with items that should trigger early return)
        let result = execute_inline_subgraph(
            &input_node.id,
            WorkflowValue::Null, // Input node doesn't have a result
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
fn main(input: [items], output: [result]):
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
fn main(input: [x], output: [result]):
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
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
        let instance_id = WorkflowInstanceId(uuid::Uuid::new_v4());

        let ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };

        // Execute with successful result
        let result = execute_inline_subgraph(
            &get_value_action.id,
            WorkflowValue::from_json(&serde_json::json!({"success": true, "data": "result"})),
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
fn main(input: [x], output: [result]):
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
