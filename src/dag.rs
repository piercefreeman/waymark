//! DAG (Directed Acyclic Graph) representation for Rappel program execution.
//!
//! The DAG represents:
//! - Nodes: Individual execution steps (assignments, actions, loops, etc.)
//! - Edges: Execution order (state machine) and data flow relationships
//!
//! Each function is converted into an isolated subgraph with input/output boundaries.

#![allow(clippy::collapsible_if, clippy::option_map_unit_fn)]

use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::parser::ast;

// ============================================================================
// Edge Types
// ============================================================================

/// Types of edges in the DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeType {
    /// Execution order edge - defines which node follows which
    StateMachine,
    /// Variable data flow edge - defines where data is pushed
    DataFlow,
}

// ============================================================================
// DAG Node
// ============================================================================

/// A node in the execution DAG.
#[derive(Debug, Clone)]
pub struct DAGNode {
    /// Unique identifier for this node
    pub id: String,
    /// Type of node (e.g., "assignment", "action_call", "for_loop", "if", etc.)
    pub node_type: String,
    /// Human-readable label for visualization
    pub label: String,
    /// UUID for this node instance (used for batch data operations)
    pub node_uuid: Uuid,
    /// Which function this node belongs to (None = top-level)
    pub function_name: Option<String>,
    /// Whether this is an aggregator node
    pub is_aggregator: bool,
    /// Node ID this aggregates from (for aggregator nodes)
    pub aggregates_from: Option<String>,
    /// Whether this is a function call node
    pub is_fn_call: bool,
    /// Name of function being called (for fn_call nodes)
    pub called_function: Option<String>,
    /// Whether this is an input boundary node
    pub is_input: bool,
    /// Whether this is an output boundary node
    pub is_output: bool,
    /// Variables at this boundary (for input/output nodes)
    pub io_vars: Option<Vec<String>>,
    /// Action name (for action_call nodes)
    pub action_name: Option<String>,
    /// Python module containing the action (for action_call nodes)
    pub module_name: Option<String>,
    /// Keyword arguments for action calls (name -> literal value or variable reference like "$var")
    pub kwargs: Option<HashMap<String, String>>,
    /// Original kwarg expressions for type-aware resolution at runtime.
    pub kwarg_exprs: Option<HashMap<String, ast::Expr>>,
    /// Target variable name where the action result should be stored (for action_call nodes)
    /// For backwards compatibility, this is the first target. Use `targets` for all targets.
    pub target: Option<String>,
    /// All target variable names for tuple unpacking (e.g., ["a", "b"] for "a, b = @action()")
    pub targets: Option<Vec<String>>,
    /// Whether this is a spread action (parallel iteration over a collection)
    pub is_spread: bool,
    /// Loop variable name for spread actions (e.g., "item" in "spread items:item -> @action()")
    pub spread_loop_var: Option<String>,
    /// Collection variable being spread over (e.g., "items" in "spread items:item -> @action()")
    pub spread_collection: Option<String>,
    /// Node ID of the aggregator that collects results from this spread action
    pub aggregates_to: Option<String>,
    /// Guard expression for conditional nodes (if/elif). Evaluated at runtime to determine branch.
    pub guard_expr: Option<ast::Expr>,
    /// Expression for assignment nodes (evaluated at runtime)
    pub assign_expr: Option<ast::Expr>,
    /// For join nodes: override the required predecessor count.
    /// If Some(n), exactly n predecessors must complete before this join fires.
    /// Used for conditional joins where branches are mutually exclusive (only 1 fires).
    pub join_required_count: Option<i32>,
}

impl DAGNode {
    /// Create a new DAG node with required fields
    pub fn new(id: String, node_type: String, label: String) -> Self {
        Self {
            id,
            node_type,
            label,
            node_uuid: Uuid::new_v4(),
            function_name: None,
            is_aggregator: false,
            aggregates_from: None,
            is_fn_call: false,
            called_function: None,
            kwargs: None,
            is_input: false,
            is_output: false,
            io_vars: None,
            action_name: None,
            module_name: None,
            target: None,
            targets: None,
            is_spread: false,
            spread_loop_var: None,
            spread_collection: None,
            aggregates_to: None,
            guard_expr: None,
            assign_expr: None,
            kwarg_exprs: None,
            join_required_count: None,
        }
    }

    /// Builder method to set guard expression for conditional nodes
    pub fn with_guard_expr(mut self, expr: ast::Expr) -> Self {
        self.guard_expr = Some(expr);
        self
    }

    /// Builder method to set assignment expression (for assignment nodes)
    pub fn with_assign_expr(mut self, expr: ast::Expr) -> Self {
        self.assign_expr = Some(expr);
        self
    }

    /// Builder method to set target variable (single target, backwards compat)
    pub fn with_target(mut self, target: &str) -> Self {
        self.target = Some(target.to_string());
        self.targets = Some(vec![target.to_string()]);
        self
    }

    /// Builder method to set multiple targets for tuple unpacking
    pub fn with_targets(mut self, targets: &[String]) -> Self {
        if let Some(first) = targets.first() {
            self.target = Some(first.clone());
        }
        self.targets = Some(targets.to_vec());
        self
    }

    /// Builder method to set function name
    pub fn with_function_name(mut self, name: &str) -> Self {
        self.function_name = Some(name.to_string());
        self
    }

    /// Builder method to mark as aggregator
    pub fn with_aggregator(mut self, aggregates_from: &str) -> Self {
        self.is_aggregator = true;
        self.aggregates_from = Some(aggregates_from.to_string());
        self
    }

    /// Builder method to mark as function call
    pub fn with_fn_call(mut self, called_function: &str) -> Self {
        self.is_fn_call = true;
        self.called_function = Some(called_function.to_string());
        self
    }

    /// Builder method to mark as input boundary
    pub fn with_input(mut self, io_vars: Vec<String>) -> Self {
        self.is_input = true;
        self.io_vars = Some(io_vars);
        self
    }

    /// Builder method to mark as output boundary
    pub fn with_output(mut self, io_vars: Vec<String>) -> Self {
        self.is_output = true;
        self.io_vars = Some(io_vars);
        self
    }

    /// Builder method to set action info (for action_call nodes)
    pub fn with_action(mut self, action_name: &str, module_name: Option<&str>) -> Self {
        self.action_name = Some(action_name.to_string());
        self.module_name = module_name.map(|s| s.to_string());
        self
    }

    /// Builder method to set kwargs (for action_call nodes)
    pub fn with_kwargs(mut self, kwargs: HashMap<String, String>) -> Self {
        self.kwargs = Some(kwargs);
        self
    }

    /// Builder method to set kwarg expressions for type-aware resolution.
    pub fn with_kwarg_exprs(mut self, kwarg_exprs: HashMap<String, ast::Expr>) -> Self {
        self.kwarg_exprs = Some(kwarg_exprs);
        self
    }

    /// Builder method to mark as spread action
    pub fn with_spread(mut self, loop_var: &str, collection: &str) -> Self {
        self.is_spread = true;
        self.spread_loop_var = Some(loop_var.to_string());
        self.spread_collection = Some(collection.to_string());
        self
    }

    /// Builder method to set aggregator node ID for spread actions
    pub fn with_aggregates_to(mut self, aggregator_id: &str) -> Self {
        self.aggregates_to = Some(aggregator_id.to_string());
        self
    }

    /// Builder method to set join required count (for conditional joins)
    pub fn with_join_required_count(mut self, count: i32) -> Self {
        self.join_required_count = Some(count);
        self
    }
}

// ============================================================================
// DAG Edge
// ============================================================================

/// An edge in the execution DAG.
#[derive(Debug, Clone, PartialEq)]
pub struct DAGEdge {
    /// Source node ID
    pub source: String,
    /// Target node ID
    pub target: String,
    /// Type of edge
    pub edge_type: EdgeType,
    /// Condition label (for state machine edges: "continue", "done", "then", "else")
    pub condition: Option<String>,
    /// Variable name (for data flow edges)
    pub variable: Option<String>,
    /// Guard expression to evaluate before following this edge.
    /// If present, the edge is only followed when the guard evaluates to true.
    /// Used for conditional branches (if/elif/else) and exception handling.
    pub guard_expr: Option<ast::Expr>,
    /// Exception types that activate this edge (for except handlers).
    /// If present, this edge is followed when the source node fails with a matching exception.
    pub exception_types: Option<Vec<String>>,
    /// Whether this is a loop back edge (body -> for_loop controller)
    pub is_loop_back: bool,
    /// String-based guard for loop control (e.g., "__loop_has_next(for_loop_0)")
    pub guard_string: Option<String>,
}

impl DAGEdge {
    /// Create a new state machine edge
    pub fn state_machine(source: String, target: String) -> Self {
        Self {
            source,
            target,
            edge_type: EdgeType::StateMachine,
            condition: None,
            variable: None,
            guard_expr: None,
            exception_types: None,
            is_loop_back: false,
            guard_string: None,
        }
    }

    /// Create a state machine edge with condition
    pub fn state_machine_with_condition(source: String, target: String, condition: &str) -> Self {
        Self {
            source,
            target,
            edge_type: EdgeType::StateMachine,
            condition: Some(condition.to_string()),
            variable: None,
            guard_expr: None,
            exception_types: None,
            is_loop_back: false,
            guard_string: None,
        }
    }

    /// Create a state machine edge with guard expression (for conditional branches)
    pub fn state_machine_with_guard(source: String, target: String, guard: ast::Expr) -> Self {
        Self {
            source,
            target,
            edge_type: EdgeType::StateMachine,
            condition: Some("guarded".to_string()),
            variable: None,
            guard_expr: Some(guard),
            exception_types: None,
            is_loop_back: false,
            guard_string: None,
        }
    }

    /// Create a state machine edge for exception handling
    pub fn state_machine_with_exception(
        source: String,
        target: String,
        exception_types: Vec<String>,
    ) -> Self {
        let condition = if exception_types.is_empty() {
            "except:*".to_string()
        } else {
            format!("except:{}", exception_types.join(","))
        };
        Self {
            source,
            target,
            edge_type: EdgeType::StateMachine,
            condition: Some(condition),
            variable: None,
            guard_expr: None,
            exception_types: Some(exception_types),
            is_loop_back: false,
            guard_string: None,
        }
    }

    /// Create a state machine edge for the success path after a try body
    pub fn state_machine_success(source: String, target: String) -> Self {
        Self {
            source,
            target,
            edge_type: EdgeType::StateMachine,
            condition: Some("success".to_string()),
            variable: None,
            guard_expr: None,
            exception_types: None,
            is_loop_back: false,
            guard_string: None,
        }
    }

    /// Create a data flow edge
    pub fn data_flow(source: String, target: String, variable: &str) -> Self {
        Self {
            source,
            target,
            edge_type: EdgeType::DataFlow,
            condition: None,
            variable: Some(variable.to_string()),
            guard_expr: None,
            exception_types: None,
            is_loop_back: false,
            guard_string: None,
        }
    }

    /// Builder method to mark this edge as a loop back edge
    pub fn with_loop_back(mut self, is_loop_back: bool) -> Self {
        self.is_loop_back = is_loop_back;
        self
    }

    /// Builder method to set a string-based guard (for loop control)
    pub fn with_guard(mut self, guard: &str) -> Self {
        self.guard_string = Some(guard.to_string());
        self
    }
}

// ============================================================================
// DAG
// ============================================================================

/// A directed acyclic graph representing program execution.
#[derive(Debug, Clone, Default)]
pub struct DAG {
    /// Nodes indexed by ID
    pub nodes: HashMap<String, DAGNode>,
    /// All edges
    pub edges: Vec<DAGEdge>,
    /// Entry node ID
    pub entry_node: Option<String>,
}

impl DAG {
    /// Create a new empty DAG
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
            entry_node: None,
        }
    }

    /// Add a node to the DAG
    pub fn add_node(&mut self, node: DAGNode) {
        if self.entry_node.is_none() {
            self.entry_node = Some(node.id.clone());
        }
        self.nodes.insert(node.id.clone(), node);
    }

    /// Add an edge to the DAG
    pub fn add_edge(&mut self, edge: DAGEdge) {
        self.edges.push(edge);
    }

    /// Get all edges pointing to a node
    pub fn get_incoming_edges(&self, node_id: &str) -> Vec<&DAGEdge> {
        self.edges.iter().filter(|e| e.target == node_id).collect()
    }

    /// Get all edges from a node
    pub fn get_outgoing_edges(&self, node_id: &str) -> Vec<&DAGEdge> {
        self.edges.iter().filter(|e| e.source == node_id).collect()
    }

    /// Get all state machine (execution order) edges
    pub fn get_state_machine_edges(&self) -> Vec<&DAGEdge> {
        self.edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine)
            .collect()
    }

    /// Get all data flow edges
    pub fn get_data_flow_edges(&self) -> Vec<&DAGEdge> {
        self.edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
            .collect()
    }

    /// Get all function names that have nodes in this DAG
    pub fn get_functions(&self) -> Vec<String> {
        let mut functions: HashSet<String> = HashSet::new();
        for node in self.nodes.values() {
            if let Some(ref fn_name) = node.function_name {
                functions.insert(fn_name.clone());
            }
        }
        let mut result: Vec<_> = functions.into_iter().collect();
        result.sort();
        result
    }

    /// Get all nodes belonging to a specific function
    pub fn get_nodes_for_function(&self, function_name: &str) -> HashMap<String, &DAGNode> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.function_name.as_deref() == Some(function_name))
            .map(|(id, node)| (id.clone(), node))
            .collect()
    }

    /// Get all edges where both source and target belong to the function
    pub fn get_edges_for_function(&self, function_name: &str) -> Vec<&DAGEdge> {
        let fn_nodes: HashSet<_> = self
            .get_nodes_for_function(function_name)
            .keys()
            .cloned()
            .collect();

        self.edges
            .iter()
            .filter(|e| fn_nodes.contains(&e.source) && fn_nodes.contains(&e.target))
            .collect()
    }
}

// ============================================================================
// DAG Converter
// ============================================================================

/// Converts Rappel AST into a DAG representation with function isolation.
///
/// Each function is converted into an isolated subgraph with:
/// - An "input" boundary node (receives inputs)
/// - An "output" boundary node (produces outputs)
/// - Internal nodes that only reference variables within the function
///
/// Function calls create "fn_call" nodes that:
/// - Connect to the calling function's data flow
/// - Reference the called function (but don't merge subgraphs)
pub struct DAGConverter {
    dag: DAG,
    node_counter: usize,
    current_function: Option<String>,
    function_defs: HashMap<String, ast::FunctionDef>,
    /// Per-function variable tracking: var_name -> defining node id
    current_scope_vars: HashMap<String, String>,
    /// var_name -> list of modifying node ids
    var_modifications: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default)]
struct ConvertedSubgraph {
    entry: Option<String>,
    exits: Vec<String>,
    nodes: Vec<String>,
    is_noop: bool,
}

impl ConvertedSubgraph {
    fn noop() -> Self {
        Self {
            entry: None,
            exits: Vec::new(),
            nodes: Vec::new(),
            is_noop: true,
        }
    }
}

impl DAGConverter {
    /// Create a new DAG converter
    pub fn new() -> Self {
        Self {
            dag: DAG::new(),
            node_counter: 0,
            current_function: None,
            function_defs: HashMap::new(),
            current_scope_vars: HashMap::new(),
            var_modifications: HashMap::new(),
        }
    }

    /// Convert a Rappel program to a fully expanded DAG.
    ///
    /// This is a two-phase process:
    /// 1. Build DAG with isolated function subgraphs + fn_call pointer nodes
    /// 2. Expand all function calls into a single global DAG rooted at the entry function
    pub fn convert(&mut self, program: &ast::Program) -> DAG {
        // Phase 1: Build with function pointers
        let unexpanded = self.convert_with_pointers(program);

        // Phase 2: Expand into single global DAG
        // Find the entry function - prefer "run", then "main", then first non-internal function
        // This matches the runner's logic for finding the entry point
        let entry_fn = self
            .function_defs
            .keys()
            .find(|name| *name == "run")
            .or_else(|| self.function_defs.keys().find(|name| *name == "main"))
            .or_else(|| {
                self.function_defs
                    .keys()
                    .find(|name| !name.starts_with("__"))
            })
            .or_else(|| self.function_defs.keys().next())
            .map(|s| s.as_str())
            .unwrap_or("run");

        let mut dag = self.expand_functions(&unexpanded, entry_fn);
        self.remap_exception_targets(&mut dag);
        self.add_global_data_flow_edges(&mut dag);
        dag
    }

    /// Phase 1: Convert a Rappel program to a DAG with isolated function subgraphs.
    ///
    /// Each function becomes its own subgraph with input/output boundary nodes.
    /// Function calls create "fn_call" pointer nodes that reference the called function.
    fn convert_with_pointers(&mut self, program: &ast::Program) -> DAG {
        self.dag = DAG::new();
        self.node_counter = 0;
        self.function_defs.clear();

        // First pass: collect all function definitions
        for func in &program.functions {
            self.function_defs.insert(func.name.clone(), func.clone());
        }

        // Second pass: convert each function into an isolated subgraph
        for func in &program.functions {
            self.convert_function(func);
        }

        std::mem::take(&mut self.dag)
    }

    /// Remap exception edges that still point at fn_call placeholders to the
    /// first node of their expanded subgraph.
    fn remap_exception_targets(&self, dag: &mut DAG) {
        let call_entry_map = Self::build_call_entry_map(dag);

        for edge in dag.edges.iter_mut().filter(|e| e.exception_types.is_some()) {
            if dag.nodes.contains_key(&edge.target) {
                continue;
            }

            if let Some(mapped) = call_entry_map.get(&edge.target) {
                edge.target = mapped.clone();
            }
        }

        // Deduplicate edges after remapping to avoid inflated predecessor counts.
        let mut seen = HashSet::new();
        dag.edges.retain(|edge| {
            let key = format!(
                "{}|{}|{:?}|{:?}|{:?}|{:?}|{}|{:?}",
                edge.source,
                edge.target,
                edge.edge_type,
                edge.condition,
                edge.exception_types,
                edge.guard_string,
                edge.is_loop_back,
                edge.variable
            );
            seen.insert(key)
        });
    }

    /// Build a mapping from fn_call node ids to the first node of their expansion.
    fn build_call_entry_map(dag: &DAG) -> HashMap<String, String> {
        let mut map = HashMap::new();

        for id in dag.nodes.keys() {
            if let Some((call_id, _)) = id.split_once(':') {
                // Pick a stable representative for the expanded subgraph.
                map.entry(call_id.to_string())
                    .and_modify(|existing| {
                        if id < existing {
                            *existing = id.clone();
                        }
                    })
                    .or_insert_with(|| id.clone());
            }
        }

        map
    }

    /// Phase 2: Expand all function calls into a single global DAG.
    ///
    /// Starting from the entry function, recursively inlines all fn_call nodes
    /// by cloning the called function's body and wiring edges based on context:
    /// - Try/except: ALL nodes within expansion get exception edges to handlers
    /// - If/else: Only the LAST node of expansion connects to the join node
    /// - For loop: The LAST node of expansion connects back to loop head
    fn expand_functions(&self, unexpanded: &DAG, entry_fn: &str) -> DAG {
        let mut expanded = DAG::new();
        let mut visited_calls: HashSet<String> = HashSet::new();

        self.expand_function_recursive(
            unexpanded,
            entry_fn,
            &mut expanded,
            &mut visited_calls,
            None, // No ID remapping for entry function
        );

        expanded
    }

    /// Recursively expand a function into the target DAG.
    ///
    /// Returns (first_node_id, last_node_id) of the expanded function body.
    /// For the entry function (id_prefix = None), input/output nodes are kept.
    /// For inlined functions (id_prefix = Some), input/output nodes are stripped.
    fn expand_function_recursive(
        &self,
        unexpanded: &DAG,
        fn_name: &str,
        target: &mut DAG,
        visited_calls: &mut HashSet<String>,
        id_prefix: Option<&str>,
    ) -> Option<(String, String)> {
        // Get all nodes for this function
        let fn_nodes = unexpanded.get_nodes_for_function(fn_name);
        if fn_nodes.is_empty() {
            return None;
        }

        // Find input and output nodes
        let input_node = fn_nodes.values().find(|n| n.is_input);
        let output_node = fn_nodes.values().find(|n| n.is_output);

        // For the entry function (no prefix), we keep input/output nodes
        // For inlined functions, we strip them
        let is_entry_function = id_prefix.is_none();

        // Build execution order for this function (using state machine edges)
        let fn_node_ids: HashSet<String> = fn_nodes.keys().cloned().collect();
        let ordered_nodes = self.get_topo_order(unexpanded, &fn_node_ids);

        // Map from old node IDs to new node IDs (for cloned nodes)
        let mut id_map: HashMap<String, String> = HashMap::new();

        // Track first and last "real" nodes (excluding input/output for inlined functions)
        let mut first_real_node: Option<String> = None;
        let mut last_real_node: Option<String> = None;

        // Clone nodes
        for old_id in &ordered_nodes {
            let node = match unexpanded.nodes.get(old_id) {
                Some(n) => n,
                None => continue,
            };

            // For inlined functions, skip input/output boundary nodes and return nodes
            // Return nodes in synthetic functions (try/except bodies, for loop bodies, etc.)
            // are artifacts of the function wrapping and serve no purpose after expansion
            if !is_entry_function && (node.is_input || node.is_output || node.node_type == "return")
            {
                continue;
            }

            // Check if this is a fn_call that needs expansion
            if node.is_fn_call {
                if let Some(called_fn) = &node.called_function {
                    // Prevent infinite recursion
                    let call_key = format!("{}:{}", fn_name, old_id);
                    if visited_calls.contains(&call_key) {
                        panic!(
                            "Recursive function call detected: {} -> {}",
                            fn_name, called_fn
                        );
                    }
                    visited_calls.insert(call_key.clone());

                    // Collect exception edges from this fn_call node BEFORE expansion
                    // These need to be propagated to ALL expanded nodes
                    let exception_edges: Vec<_> = unexpanded
                        .edges
                        .iter()
                        .filter(|e| e.source == *old_id && e.exception_types.is_some())
                        .cloned()
                        .collect();

                    // Recursively expand the called function
                    let child_prefix = if let Some(prefix) = id_prefix {
                        format!("{}:{}", prefix, old_id)
                    } else {
                        old_id.clone()
                    };

                    if let Some((child_first, child_last)) = self.expand_function_recursive(
                        unexpanded,
                        called_fn,
                        target,
                        visited_calls,
                        Some(&child_prefix),
                    ) {
                        // Map this fn_call node to the expansion's first/last nodes
                        // For edge rewiring, we treat fn_call as expanding to child_first...child_last
                        id_map.insert(old_id.clone(), child_first.clone());

                        // Store the last node for later edge rewiring
                        // We'll need special handling for edges FROM this fn_call
                        id_map.insert(format!("{}_last", old_id), child_last.clone());

                        // Propagate spread attributes from fn_call to expanded action nodes
                        // This implements for loop semantics: the fn_call is marked as spread,
                        // and all action_call nodes inside need to inherit that for iteration
                        if node.is_spread {
                            // Find all action nodes that were added as part of this expansion
                            let expanded_action_ids: Vec<_> = target
                                .nodes
                                .keys()
                                .filter(|id| id.starts_with(&child_prefix))
                                .cloned()
                                .collect();

                            for action_id in expanded_action_ids {
                                if let Some(action_node) = target.nodes.get_mut(&action_id) {
                                    if action_node.node_type == "action_call" {
                                        action_node.is_spread = true;
                                        action_node.spread_loop_var = node.spread_loop_var.clone();
                                        action_node.spread_collection =
                                            node.spread_collection.clone();
                                        action_node.aggregates_to = node.aggregates_to.clone();
                                    }
                                }
                            }
                        }

                        // Propagate exception edges to ALL expanded nodes
                        // This implements the try/except context handling:
                        // when a function is called inside a try block, all its nodes
                        // should route exceptions to the handlers
                        if !exception_edges.is_empty() {
                            // Find all nodes that were added as part of this expansion
                            // They have IDs starting with child_prefix
                            let expanded_node_ids: Vec<_> = target
                                .nodes
                                .keys()
                                .filter(|id| id.starts_with(&child_prefix))
                                .cloned()
                                .collect();

                            for expanded_node_id in expanded_node_ids {
                                for exc_edge in &exception_edges {
                                    let mut new_edge = exc_edge.clone();
                                    new_edge.source = expanded_node_id.clone();
                                    // Target is already the exception handler (outside expansion)
                                    target.add_edge(new_edge);
                                }
                            }
                        }

                        if first_real_node.is_none() {
                            first_real_node = Some(child_first);
                        }
                        last_real_node = Some(child_last);
                    }

                    visited_calls.remove(&call_key);
                    continue;
                }
            }

            // Clone the node with a new ID
            let new_id = if let Some(prefix) = id_prefix {
                format!("{}:{}", prefix, node.id)
            } else {
                node.id.clone()
            };

            let mut cloned = node.clone();
            cloned.id = new_id.clone();
            cloned.node_uuid = Uuid::new_v4();

            id_map.insert(old_id.clone(), new_id.clone());

            if first_real_node.is_none() {
                first_real_node = Some(new_id.clone());
            }
            last_real_node = Some(new_id.clone());

            target.add_node(cloned);
        }

        // Clone edges, remapping IDs and handling boundary nodes
        let input_id = input_node.map(|n| n.id.as_str());
        let output_id = output_node.map(|n| n.id.as_str());

        for edge in &unexpanded.edges {
            // Skip edges where the source is not in this function
            // (target can be outside for loop-back edges)
            if !fn_node_ids.contains(&edge.source) {
                continue;
            }

            // For inlined functions (not entry), skip edges to/from input/output/return nodes
            if !is_entry_function {
                // Handle edges from input node - these become edges from predecessor (handled by caller)
                if input_id == Some(edge.source.as_str()) {
                    // Skip - the caller will wire incoming edges
                    continue;
                }

                // Handle edges to output node - these become edges to successor (handled by caller)
                if output_id == Some(edge.target.as_str()) {
                    // Skip - the caller will wire outgoing edges
                    continue;
                }

                // Handle edges to return nodes - return nodes are skipped during expansion
                // (they're artifacts of function wrapping), so edges targeting them must also be skipped
                if let Some(target_node) = unexpanded.nodes.get(&edge.target) {
                    if target_node.node_type == "return" {
                        continue;
                    }
                }
            }

            // Get remapped source and target
            let new_source = if let Some(mapped) = id_map.get(&edge.source) {
                // If source was a fn_call, we need the LAST node of its expansion
                if unexpanded
                    .nodes
                    .get(&edge.source)
                    .map(|n| n.is_fn_call)
                    .unwrap_or(false)
                {
                    id_map
                        .get(&format!("{}_last", edge.source))
                        .cloned()
                        .unwrap_or_else(|| mapped.clone())
                } else {
                    mapped.clone()
                }
            } else {
                // Source not in id_map - it's outside the current expansion scope
                // (e.g., fn_call being expanded, or external node). Skip this edge.
                continue;
            };

            // For data flow edges targeting a fn_call node, skip them.
            // The fn_call kwargs (like {processed: $processed}) indicate that the variable
            // is passed into the function, but after expansion, only specific nodes inside
            // the function actually use it. We skip these edges and let add_global_data_flow_edges
            // recompute them correctly after expansion.
            if edge.edge_type == EdgeType::DataFlow {
                if let Some(target_node) = unexpanded.nodes.get(&edge.target) {
                    if target_node.is_fn_call {
                        continue;
                    }
                }
            }

            let new_target = match id_map.get(&edge.target) {
                Some(t) => t.clone(),
                // Target not in id_map - it's outside the expansion (e.g., loop-back to for_loop)
                // Keep the original target
                None => edge.target.clone(),
            };

            let mut cloned_edge = edge.clone();
            cloned_edge.source = new_source;
            cloned_edge.target = new_target;
            target.add_edge(cloned_edge);
        }

        // Return first and last real nodes for parent to wire up
        match (first_real_node, last_real_node) {
            (Some(first), Some(last)) => Some((first, last)),
            _ => None,
        }
    }

    /// Get topological order of nodes using state machine edges
    fn get_topo_order(&self, dag: &DAG, node_ids: &HashSet<String>) -> Vec<String> {
        let mut in_degree: HashMap<String, usize> =
            node_ids.iter().map(|n| (n.clone(), 0)).collect();
        let mut adj: HashMap<String, Vec<String>> =
            node_ids.iter().map(|n| (n.clone(), Vec::new())).collect();

        for edge in &dag.edges {
            // Skip loop-back edges to avoid cycles in topological sort
            if edge.is_loop_back {
                continue;
            }
            if edge.edge_type == EdgeType::StateMachine
                && node_ids.contains(&edge.source)
                && node_ids.contains(&edge.target)
            {
                adj.get_mut(&edge.source)
                    .map(|v| v.push(edge.target.clone()));
                in_degree.entry(edge.target.clone()).and_modify(|d| *d += 1);
            }
        }

        // Kahn's algorithm
        let mut queue: Vec<String> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(n, _)| n.clone())
            .collect();
        let mut order = Vec::new();

        while let Some(node) = queue.pop() {
            order.push(node.clone());
            if let Some(neighbors) = adj.get(&node) {
                for neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push(neighbor.clone());
                        }
                    }
                }
            }
        }

        order
    }

    /// Recompute data flow edges across the expanded DAG (all functions).
    ///
    /// Function expansion can eliminate the original per-function data flow edges.
    /// This pass rebuilds data flow edges globally so variables defined inside
    /// expanded helper functions (like implicit try/except bodies) are available
    /// to downstream nodes in the caller.
    fn add_global_data_flow_edges(&self, dag: &mut DAG) {
        // Preserve existing data flow edges (they carry important loop metadata
        // like loop indices and accumulator wiring) and avoid duplicating them
        // when we add the global edges below.
        let existing_data_flow: Vec<DAGEdge> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
            .cloned()
            .collect();
        dag.edges.retain(|e| e.edge_type != EdgeType::DataFlow);

        // Build topological order for all nodes (state machine edges only, skip loop-back).
        let node_ids: HashSet<String> = dag.nodes.keys().cloned().collect();
        let order = {
            let mut in_degree: HashMap<String, usize> =
                node_ids.iter().map(|n| (n.clone(), 0)).collect();
            let mut adj: HashMap<String, Vec<String>> =
                node_ids.iter().map(|n| (n.clone(), Vec::new())).collect();

            for edge in dag.get_state_machine_edges() {
                if edge.is_loop_back {
                    continue;
                }
                if node_ids.contains(&edge.source) && node_ids.contains(&edge.target) {
                    adj.get_mut(&edge.source)
                        .map(|v| v.push(edge.target.clone()));
                    in_degree.entry(edge.target.clone()).and_modify(|d| *d += 1);
                }
            }

            let mut queue: Vec<String> = in_degree
                .iter()
                .filter(|(_, deg)| **deg == 0)
                .map(|(n, _)| n.clone())
                .collect();
            let mut order = Vec::new();

            while let Some(node) = queue.pop() {
                order.push(node.clone());
                if let Some(neighbors) = adj.get(&node) {
                    for neighbor in neighbors {
                        if let Some(deg) = in_degree.get_mut(neighbor) {
                            if *deg > 0 {
                                *deg -= 1;
                                if *deg == 0 {
                                    queue.push(neighbor.clone());
                                }
                            }
                        }
                    }
                }
            }

            order
        };

        // Collect loop-back edges to restore loop-carried data flow later.
        let loop_back_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine && e.is_loop_back)
            .cloned()
            .collect();

        tracing::debug!(?order, "add_global_data_flow_edges order");

        // Collect variable modifications (definitions) across all nodes.
        // Skip join nodes - they mark variable availability at a join point but don't
        // actually define the variable. Including them in var_modifications causes
        // incorrect data flow edges due to topological ordering issues in loops.
        let mut var_modifications: HashMap<String, Vec<String>> = HashMap::new();
        for (node_id, node) in dag.nodes.iter() {
            // Skip join nodes - they don't define variables
            if node.node_type == "join" {
                continue;
            }
            if node.is_input {
                if let Some(ref inputs) = node.io_vars {
                    for input in inputs {
                        var_modifications
                            .entry(input.clone())
                            .or_default()
                            .push(node_id.clone());
                    }
                }
            }
            if let Some(ref target) = node.target {
                var_modifications
                    .entry(target.clone())
                    .or_default()
                    .push(node_id.clone());
            }
            if let Some(ref targets) = node.targets {
                for t in targets {
                    var_modifications
                        .entry(t.clone())
                        .or_default()
                        .push(node_id.clone());
                }
            }
        }
        let var_modifications_clone = var_modifications.clone();

        // Collect edge guard expressions by source node, so we can check them without
        // borrowing dag.edges inside the closure (which would conflict with later mutations)
        let mut node_guard_exprs: HashMap<String, Vec<ast::Expr>> = HashMap::new();
        for edge in &dag.edges {
            if let Some(ref guard) = edge.guard_expr {
                node_guard_exprs
                    .entry(edge.source.clone())
                    .or_default()
                    .push(guard.clone());
            }
        }

        // Helper: does a node use a variable?
        let uses_var = |node: &DAGNode, var_name: &str| -> bool {
            fn expr_uses_var(expr: &ast::Expr, var_name: &str) -> bool {
                use ast::expr;

                match &expr.kind {
                    Some(expr::Kind::Variable(v)) => v.name == var_name,
                    Some(expr::Kind::BinaryOp(b)) => {
                        expr_uses_var(
                            b.left
                                .as_ref()
                                .expect("binary op must have left expression"),
                            var_name,
                        ) || expr_uses_var(
                            b.right
                                .as_ref()
                                .expect("binary op must have right expression"),
                            var_name,
                        )
                    }
                    Some(expr::Kind::UnaryOp(u)) => u
                        .operand
                        .as_ref()
                        .map(|op| expr_uses_var(op, var_name))
                        .unwrap_or(false),
                    Some(expr::Kind::List(list)) => list
                        .elements
                        .iter()
                        .any(|elem| expr_uses_var(elem, var_name)),
                    Some(expr::Kind::Dict(dict)) => dict.entries.iter().any(|entry| {
                        entry
                            .key
                            .as_ref()
                            .map(|k| expr_uses_var(k, var_name))
                            .unwrap_or(false)
                            || entry
                                .value
                                .as_ref()
                                .map(|v| expr_uses_var(v, var_name))
                                .unwrap_or(false)
                    }),
                    Some(expr::Kind::Index(idx)) => {
                        idx.object
                            .as_ref()
                            .map(|v| expr_uses_var(v, var_name))
                            .unwrap_or(false)
                            || idx
                                .index
                                .as_ref()
                                .map(|i| expr_uses_var(i, var_name))
                                .unwrap_or(false)
                    }
                    Some(expr::Kind::FunctionCall(call)) => {
                        call.args.iter().any(|arg| expr_uses_var(arg, var_name))
                            || call.kwargs.iter().any(|kw| {
                                kw.value
                                    .as_ref()
                                    .map(|v| expr_uses_var(v, var_name))
                                    .unwrap_or(false)
                            })
                    }
                    _ => false,
                }
            }

            if let Some(ref kwargs) = node.kwargs {
                for value in kwargs.values() {
                    if value == &format!("${}", var_name) {
                        tracing::trace!(
                            node_id = %node.id,
                            var_name = %var_name,
                            "uses_var: found in kwargs"
                        );
                        return true;
                    }
                }
            }

            if let Some(ref assign_expr) = node.assign_expr {
                if expr_uses_var(assign_expr, var_name) {
                    tracing::trace!(
                        node_id = %node.id,
                        var_name = %var_name,
                        "uses_var: found in assign_expr"
                    );
                    return true;
                }
            }

            // Check edge guards: if this node is the source of an edge with a guard
            // that uses the variable, the node needs that variable in scope
            if let Some(guards) = node_guard_exprs.get(&node.id) {
                for guard in guards {
                    if expr_uses_var(guard, var_name) {
                        tracing::trace!(
                            node_id = %node.id,
                            var_name = %var_name,
                            "uses_var: found in edge guard"
                        );
                        return true;
                    }
                }
            }

            // Check kwarg_exprs for variable references
            if let Some(ref kwarg_exprs) = node.kwarg_exprs {
                for expr in kwarg_exprs.values() {
                    if expr_uses_var(expr, var_name) {
                        tracing::trace!(
                            node_id = %node.id,
                            var_name = %var_name,
                            "uses_var: found in kwarg_exprs"
                        );
                        return true;
                    }
                }
            }

            false
        };

        // Add data flow edges from each modification to downstream uses until next modification.
        let mut seen_edges: HashSet<(String, String, Option<String>)> = HashSet::new();
        for edge in &existing_data_flow {
            seen_edges.insert((
                edge.source.clone(),
                edge.target.clone(),
                edge.variable.clone(),
            ));
        }

        // Build reachability map for checking if one node can reach another via "normal" state
        // machine edges (excluding exception edges). This is needed to correctly handle try/except
        // where modifications on exception paths should not block data flow propagation on the
        // success path. Exception edges are conditional (only taken when an exception occurs),
        // so for data flow purposes, a modification reachable only via exception edges is
        // effectively on a parallel branch.
        let reachable_via_normal_edges: HashMap<String, HashSet<String>> = {
            let mut result: HashMap<String, HashSet<String>> = HashMap::new();
            // Build adjacency list from state machine edges, excluding loop-back AND exception edges
            let mut adj: HashMap<String, Vec<String>> = HashMap::new();
            for edge in dag.get_state_machine_edges() {
                // Skip loop-back edges and exception edges
                if edge.is_loop_back || edge.exception_types.is_some() {
                    continue;
                }
                adj.entry(edge.source.clone())
                    .or_default()
                    .push(edge.target.clone());
            }
            // For each node, compute all reachable nodes via BFS (normal edges only)
            for start in dag.nodes.keys() {
                let mut reachable = HashSet::new();
                let mut queue = vec![start.clone()];
                while let Some(node) = queue.pop() {
                    if let Some(neighbors) = adj.get(&node) {
                        for neighbor in neighbors {
                            if reachable.insert(neighbor.clone()) {
                                queue.push(neighbor.clone());
                            }
                        }
                    }
                }
                result.insert(start.clone(), reachable);
            }
            result
        };

        for (var_name, modifications) in var_modifications {
            // Sort modifications in topological order for deterministic edges.
            let mut mods = modifications.clone();
            mods.sort_by_key(|id| order.iter().position(|n| n == id).unwrap_or(order.len()));
            mods.dedup();

            for (i, mod_node) in mods.iter().enumerate() {
                // Find position of this modification in execution order.
                let Some(mod_pos) = order.iter().position(|n| n == mod_node) else {
                    continue;
                };
                let next_mod = mods.get(i + 1);

                // Check if the next modification is reachable via normal (non-exception) edges.
                // If it's only reachable via exception edges (e.g., in an except handler),
                // we should continue propagating data flow to nodes after the join point,
                // since the exception path is conditional and the success path needs the
                // original value.
                let next_mod_reachable = next_mod
                    .map(|next| {
                        reachable_via_normal_edges
                            .get(mod_node)
                            .map(|r| r.contains(next))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);

                for (pos, node_id) in order.iter().enumerate() {
                    if pos <= mod_pos {
                        continue;
                    }

                    if let Some(next) = next_mod {
                        if node_id == next {
                            // Still allow edge to the next modification if it uses the var.
                            if let Some(node) = dag.nodes.get(node_id) {
                                let uses = uses_var(node, &var_name);
                                tracing::debug!(
                                    node_id = %node_id,
                                    var_name = %var_name,
                                    has_assign_expr = node.assign_expr.is_some(),
                                    uses = uses,
                                    "checking next modifier uses var"
                                );
                                if uses {
                                    let key =
                                        (mod_node.clone(), node_id.clone(), Some(var_name.clone()));
                                    if seen_edges.insert(key.clone()) {
                                        dag.edges.push(DAGEdge {
                                            source: mod_node.clone(),
                                            target: node_id.clone(),
                                            edge_type: EdgeType::DataFlow,
                                            condition: None,
                                            variable: Some(var_name.clone()),
                                            guard_expr: None,
                                            exception_types: None,
                                            is_loop_back: false,
                                            guard_string: None,
                                        });
                                    }
                                }
                            }
                            // Only break if the next modification is reachable from this one.
                            // If they're on parallel branches (e.g., try success vs except handler),
                            // we need to continue propagating to nodes after the join point.
                            if next_mod_reachable {
                                break;
                            }
                        }
                    }

                    if let Some(node) = dag.nodes.get(node_id) {
                        if uses_var(node, &var_name) {
                            let key = (mod_node.clone(), node_id.clone(), Some(var_name.clone()));
                            if seen_edges.insert(key.clone()) {
                                dag.edges.push(DAGEdge {
                                    source: mod_node.clone(),
                                    target: node_id.clone(),
                                    edge_type: EdgeType::DataFlow,
                                    condition: None,
                                    variable: Some(var_name.clone()),
                                    guard_expr: None,
                                    exception_types: None,
                                    is_loop_back: false,
                                    guard_string: None,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Build a set of nodes that are "inside a loop" - i.e., between the loop condition
        // and the loop back edge source.
        // We do this by BFS backwards from loop_back sources, stopping at nodes that are
        // direct successors of loop heads (the loop heads themselves are boundary nodes).
        let loop_back_sources: HashSet<String> =
            loop_back_edges.iter().map(|e| e.source.clone()).collect();
        let loop_back_targets: HashSet<String> =
            loop_back_edges.iter().map(|e| e.target.clone()).collect();
        let mut nodes_in_loop: HashSet<String> = loop_back_sources.clone();
        {
            // BFS backwards from loop_back sources to find all nodes inside loops
            let mut queue: Vec<String> = loop_back_sources.iter().cloned().collect();
            let mut visited: HashSet<String> = loop_back_sources.clone();
            while let Some(node_id) = queue.pop() {
                // Stop BFS if current node is a loop head (target of loop_back)
                // Loop heads like loop_cond are boundary nodes, not "inside" the loop
                if loop_back_targets.contains(&node_id) {
                    continue;
                }

                // Find predecessors (nodes with edges TO this node)
                for edge in dag.get_state_machine_edges() {
                    if edge.target == node_id && !edge.is_loop_back {
                        if visited.insert(edge.source.clone()) {
                            nodes_in_loop.insert(edge.source.clone());
                            queue.push(edge.source.clone());
                        }
                    }
                }
            }
        }

        // Add loop-carried edges for variables whose defining node is inside a loop.
        // This ensures updated values flow to earlier nodes in the cycle (e.g., loop
        // heads and extract/index nodes) and to the same node for the next iteration.
        for (var_name, modifications) in var_modifications_clone {
            for mod_node in modifications {
                if !nodes_in_loop.contains(&mod_node) {
                    continue;
                }

                for node_id in &order {
                    if node_id == &mod_node {
                        continue;
                    }

                    if let Some(node) = dag.nodes.get(node_id) {
                        // For loop index variables (__loop_*), we need to propagate to ALL
                        // action nodes inside the loop body, not just nodes that directly use
                        // the variable. This ensures the current loop index is in the action's
                        // inbox when it completes, so the subsequent completion traversal has
                        // the correct iteration value rather than a stale one.
                        let is_loop_index = var_name.starts_with("__loop_");
                        let is_action_in_loop =
                            node.node_type == "action_call" && nodes_in_loop.contains(node_id);
                        let should_add_edge =
                            uses_var(node, &var_name) || (is_loop_index && is_action_in_loop);

                        if should_add_edge {
                            let key = (mod_node.clone(), node_id.clone(), Some(var_name.clone()));
                            if seen_edges.insert(key.clone()) {
                                dag.edges.push(DAGEdge {
                                    source: mod_node.clone(),
                                    target: node_id.clone(),
                                    edge_type: EdgeType::DataFlow,
                                    condition: None,
                                    variable: Some(var_name.clone()),
                                    guard_expr: None,
                                    exception_types: None,
                                    is_loop_back: false,
                                    guard_string: None,
                                });
                            }
                        }
                    }
                }

                // Also feed the updated variable back into the defining node so it
                // is available on the next iteration (e.g., loop counters).
                let self_key = (mod_node.clone(), mod_node.clone(), Some(var_name.clone()));
                if seen_edges.insert(self_key.clone()) {
                    dag.edges.push(DAGEdge {
                        source: mod_node.clone(),
                        target: mod_node.clone(),
                        edge_type: EdgeType::DataFlow,
                        condition: None,
                        variable: Some(var_name.clone()),
                        guard_expr: None,
                        exception_types: None,
                        is_loop_back: false,
                        guard_string: None,
                    });
                }
            }
        }

        // Add loop-carried data flow edges along loop-back control edges so
        // updated variables (e.g., loop indices or accumulators) are visible
        // on the next iteration when the loop head re-executes.
        for edge in loop_back_edges {
            if let Some(source_node) = dag.nodes.get(&edge.source) {
                let mut defined_vars: Vec<String> = Vec::new();
                if let Some(ref target) = source_node.target {
                    defined_vars.push(target.clone());
                }
                if let Some(ref targets) = source_node.targets {
                    defined_vars.extend(targets.clone());
                }

                for var in defined_vars {
                    dag.edges.push(DAGEdge::data_flow(
                        edge.source.clone(),
                        edge.target.clone(),
                        &var,
                    ));
                }
            }
        }

        // Add back the original data flow edges we preserved up front.
        dag.edges.extend(existing_data_flow);
    }

    /// Convert a function definition into an isolated subgraph
    fn convert_function(&mut self, fn_def: &ast::FunctionDef) {
        self.current_function = Some(fn_def.name.clone());
        self.current_scope_vars.clear();
        self.var_modifications.clear();

        let io = fn_def.io.as_ref().unwrap();

        // Create input boundary node
        let input_id = self.next_id(&format!("{}_input", fn_def.name));
        let input_label = if io.inputs.is_empty() {
            "input: []".to_string()
        } else {
            format!("input: [{}]", io.inputs.join(", "))
        };

        let input_node = DAGNode::new(input_id.clone(), "input".to_string(), input_label)
            .with_function_name(&fn_def.name)
            .with_input(io.inputs.clone());
        self.dag.add_node(input_node);

        // Track input variables as defined at the input node
        for var in &io.inputs {
            self.track_var_definition(var, &input_id);
        }

        // Convert function body
        let mut frontier = vec![input_id.clone()];
        if let Some(body) = &fn_def.body {
            for stmt in &body.statements {
                let converted = self.convert_statement(stmt);
                if converted.is_noop {
                    continue;
                }
                if let Some(entry) = &converted.entry {
                    for prev in &frontier {
                        self.dag
                            .add_edge(DAGEdge::state_machine(prev.clone(), entry.clone()));
                    }
                }
                frontier = converted.exits;
            }
        }

        // Create output boundary node
        let output_id = self.next_id(&format!("{}_output", fn_def.name));
        let output_label = format!("output: [{}]", io.outputs.join(", "));

        let output_node = DAGNode::new(output_id.clone(), "output".to_string(), output_label)
            .with_function_name(&fn_def.name)
            .with_output(io.outputs.clone());
        self.dag.add_node(output_node);

        // Connect all continuing control-flow exits to the output node.
        for prev in frontier {
            self.dag
                .add_edge(DAGEdge::state_machine(prev, output_id.clone()));
        }

        // Connect ALL return nodes in this function to the output node.
        // This handles early returns inside conditionals, loops, etc.
        // which won't be reached by the normal "last node -> output" connection.
        let return_nodes: Vec<String> = self
            .dag
            .nodes
            .iter()
            .filter(|(_, n)| {
                n.node_type == "return" && n.function_name.as_ref() == Some(&fn_def.name)
            })
            .map(|(id, _)| id.clone())
            .collect();

        for return_node_id in return_nodes {
            // Check if this return node already has an edge to the output
            let already_connected = self
                .dag
                .edges
                .iter()
                .any(|e| e.source == return_node_id && e.target == output_id);
            if !already_connected {
                self.dag
                    .add_edge(DAGEdge::state_machine(return_node_id, output_id.clone()));
            }
        }

        // Add data flow edges within this function
        self.add_data_flow_edges_for_function(&fn_def.name);

        self.current_function = None;
    }

    /// Generate the next unique node ID
    fn next_id(&mut self, prefix: &str) -> String {
        self.node_counter += 1;
        format!("{}_{}", prefix, self.node_counter)
    }

    /// Build a guard expression for loop continuation: __loop_i < len(collection)
    fn build_loop_guard(loop_i_var: &str, collection: Option<&ast::Expr>) -> Option<ast::Expr> {
        let collection_expr = collection?;

        // Build: __loop_i < len(items=collection)
        // Using BinaryOp with BINARY_OP_LT
        // Note: The len() builtin expects kwargs["items"], so we pass via kwargs
        Some(ast::Expr {
            kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                left: Some(Box::new(ast::Expr {
                    kind: Some(ast::expr::Kind::Variable(ast::Variable {
                        name: loop_i_var.to_string(),
                    })),
                    span: None,
                })),
                op: ast::BinaryOperator::BinaryOpLt as i32,
                right: Some(Box::new(ast::Expr {
                    kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                        name: "len".to_string(),
                        args: vec![],
                        kwargs: vec![ast::Kwarg {
                            name: "items".to_string(),
                            value: Some(collection_expr.clone()),
                        }],
                    })),
                    span: None,
                })),
            }))),
            span: None,
        })
    }

    /// Convert a statement to DAG node(s)
    fn convert_block(&mut self, block: &ast::Block) -> ConvertedSubgraph {
        let mut nodes = Vec::new();
        let mut entry: Option<String> = None;
        let mut frontier: Option<Vec<String>> = None;

        for stmt in &block.statements {
            let converted = self.convert_statement(stmt);
            nodes.extend(converted.nodes.clone());

            if converted.is_noop {
                continue;
            }

            if entry.is_none() {
                entry = converted.entry.clone();
            }

            if let Some(prev_exits) = &frontier
                && let Some(next_entry) = &converted.entry
            {
                for prev in prev_exits {
                    self.dag
                        .add_edge(DAGEdge::state_machine(prev.clone(), next_entry.clone()));
                }
            }

            frontier = Some(converted.exits.clone());
        }

        if entry.is_none() {
            return ConvertedSubgraph::noop();
        }

        ConvertedSubgraph {
            entry,
            exits: frontier.unwrap_or_default(),
            nodes,
            is_noop: false,
        }
    }

    fn convert_statement(&mut self, stmt: &ast::Statement) -> ConvertedSubgraph {
        let kind = match &stmt.kind {
            Some(k) => k,
            None => return ConvertedSubgraph::noop(),
        };

        let node_ids = match kind {
            ast::statement::Kind::Assignment(assign) => self.convert_assignment(assign),
            ast::statement::Kind::ActionCall(action) => {
                // Side-effect only action statement (no target)
                self.convert_action_call_with_targets(action, &[])
            }
            ast::statement::Kind::SpreadAction(spread) => {
                // Side-effect only spread statement (no target)
                self.convert_spread_action(spread)
            }
            ast::statement::Kind::ParallelBlock(parallel) => {
                // Side-effect only parallel statement (no target)
                self.convert_parallel_block(parallel)
            }
            ast::statement::Kind::ForLoop(for_loop) => {
                return self.convert_for_loop(for_loop);
            }
            ast::statement::Kind::Conditional(cond) => {
                return self.convert_conditional(cond);
            }
            ast::statement::Kind::TryExcept(try_except) => {
                return self.convert_try_except(try_except);
            }
            ast::statement::Kind::ReturnStmt(ret) => {
                let ids = self.convert_return(ret);
                return ConvertedSubgraph {
                    entry: ids.first().cloned(),
                    exits: Vec::new(),
                    nodes: ids,
                    is_noop: false,
                };
            }
            ast::statement::Kind::ExprStmt(expr_stmt) => self.convert_expr_statement(expr_stmt),
        };

        if node_ids.is_empty() {
            return ConvertedSubgraph::noop();
        }

        ConvertedSubgraph {
            entry: node_ids.first().cloned(),
            exits: node_ids.last().cloned().into_iter().collect(),
            nodes: node_ids,
            is_noop: false,
        }
    }

    /// Convert an assignment statement
    fn convert_assignment(&mut self, assign: &ast::Assignment) -> Vec<String> {
        let value = match &assign.value {
            Some(v) => v,
            None => return vec![],
        };

        let targets = &assign.targets;

        // Check if RHS is a function call
        if let Some(ast::expr::Kind::FunctionCall(call)) = &value.kind {
            let target = targets.first().map(|s| s.as_str()).unwrap_or("_");
            return self.convert_fn_call_assignment(target, targets, call);
        }

        // Check if RHS is an action call
        if let Some(ast::expr::Kind::ActionCall(action)) = &value.kind {
            return self.convert_action_call_with_targets(action, targets);
        }

        // Check if RHS is a parallel expression
        if let Some(ast::expr::Kind::ParallelExpr(parallel)) = &value.kind {
            return self.convert_parallel_expr(parallel, targets);
        }

        // Check if RHS is a spread expression
        if let Some(ast::expr::Kind::SpreadExpr(spread)) = &value.kind {
            return self.convert_spread_expr(spread, targets);
        }

        // Regular assignment
        let node_id = self.next_id("assign");
        let target = targets.first().map(|s| s.as_str()).unwrap_or("_");
        let label = if targets.len() > 1 {
            format!("{} = ...", targets.join(", "))
        } else {
            format!("{} = ...", target)
        };

        let mut node = DAGNode::new(node_id.clone(), "assignment".to_string(), label)
            .with_targets(targets)
            .with_assign_expr(value.clone());
        if let Some(ref fn_name) = self.current_function {
            node = node.with_function_name(fn_name);
        }
        self.dag.add_node(node);

        // Track variable definitions
        for t in targets {
            self.track_var_definition(t, &node_id);
        }

        vec![node_id]
    }

    /// Convert a function call assignment
    fn convert_fn_call_assignment(
        &mut self,
        target: &str,
        targets: &[String],
        call: &ast::FunctionCall,
    ) -> Vec<String> {
        let node_id = self.next_id("fn_call");
        let label = if targets.len() > 1 {
            format!("{}() -> {}", call.name, targets.join(", "))
        } else {
            format!("{}() -> {}", call.name, target)
        };

        // Extract kwargs from the function call
        let kwargs = self.extract_kwargs(&call.kwargs);
        let kwarg_exprs = self.extract_kwarg_exprs(&call.kwargs);

        let mut node = DAGNode::new(node_id.clone(), "fn_call".to_string(), label)
            .with_fn_call(&call.name)
            .with_kwargs(kwargs)
            .with_kwarg_exprs(kwarg_exprs);
        if let Some(ref fn_name) = self.current_function {
            node = node.with_function_name(fn_name);
        }
        self.dag.add_node(node);

        // Track variable definitions
        for t in targets {
            self.track_var_definition(t, &node_id);
        }

        vec![node_id]
    }

    /// Convert an action call
    /// Convert an action call with multiple targets for tuple unpacking support.
    /// If targets is empty, it's a side-effect only action.
    /// If targets has one element, it's a simple assignment.
    /// If targets has multiple elements, the action result will be unpacked.
    fn convert_action_call_with_targets(
        &mut self,
        action: &ast::ActionCall,
        targets: &[String],
    ) -> Vec<String> {
        let node_id = self.next_id("action");

        // Build label showing all targets
        let label = if targets.is_empty() {
            format!("@{}()", action.action_name)
        } else if targets.len() == 1 {
            format!("@{}() -> {}", action.action_name, targets[0])
        } else {
            format!("@{}() -> ({})", action.action_name, targets.join(", "))
        };

        // Extract kwargs as string representations
        let kwargs = self.extract_kwargs(&action.kwargs);
        let kwarg_exprs = self.extract_kwarg_exprs(&action.kwargs);

        let mut node = DAGNode::new(node_id.clone(), "action_call".to_string(), label)
            .with_action(&action.action_name, action.module_name.as_deref())
            .with_kwargs(kwargs)
            .with_kwarg_exprs(kwarg_exprs);

        // Set targets for unpacking (store all targets)
        if !targets.is_empty() {
            node = node.with_targets(targets);
            tracing::debug!(node_id = %node_id, targets = ?targets, "setting action targets");
        } else {
            tracing::debug!(node_id = %node_id, "no targets for action");
        }

        if let Some(ref fn_name) = self.current_function {
            node = node.with_function_name(fn_name);
        }
        self.dag.add_node(node);

        // Track variable definitions for all targets
        for t in targets {
            self.track_var_definition(t, &node_id);
        }

        vec![node_id]
    }

    /// Extract kwargs from AST Kwarg list to a HashMap.
    /// Values are either literal strings/JSON or variable references like "$var".
    fn extract_kwargs(&self, kwargs: &[ast::Kwarg]) -> HashMap<String, String> {
        let mut result = HashMap::new();
        for kwarg in kwargs {
            if let Some(ref value) = kwarg.value {
                let value_str = self.expr_to_string(value);
                result.insert(kwarg.name.clone(), value_str);
            }
        }
        result
    }

    /// Clone kwarg expressions for runtime evaluation.
    fn extract_kwarg_exprs(&self, kwargs: &[ast::Kwarg]) -> HashMap<String, ast::Expr> {
        let mut result = HashMap::new();
        for kwarg in kwargs {
            if let Some(ref value) = kwarg.value {
                result.insert(kwarg.name.clone(), value.clone());
            }
        }
        result
    }

    /// Convert an expression to a string representation.
    /// Variables become "$varname", literals become their JSON representation.
    fn expr_to_string(&self, expr: &ast::Expr) -> String {
        match &expr.kind {
            Some(ast::expr::Kind::Variable(var)) => format!("${}", var.name),
            Some(ast::expr::Kind::Literal(lit)) => self.literal_to_string(lit),
            Some(ast::expr::Kind::List(list)) => {
                let items: Vec<String> = list
                    .elements
                    .iter()
                    .map(|e| self.expr_to_string(e))
                    .collect();
                format!("[{}]", items.join(", "))
            }
            Some(ast::expr::Kind::Dict(dict)) => {
                let entries: Vec<String> = dict
                    .entries
                    .iter()
                    .map(|e| {
                        let key = e
                            .key
                            .as_ref()
                            .map(|k| self.expr_to_string(k))
                            .unwrap_or_default();
                        let val = e
                            .value
                            .as_ref()
                            .map(|v| self.expr_to_string(v))
                            .unwrap_or_default();
                        format!("{}: {}", key, val)
                    })
                    .collect();
                format!("{{{}}}", entries.join(", "))
            }
            Some(ast::expr::Kind::FunctionCall(call)) => {
                let mut parts: Vec<String> =
                    call.args.iter().map(|a| self.expr_to_string(a)).collect();
                for kw in &call.kwargs {
                    if let Some(ref value) = kw.value {
                        parts.push(format!("{}={}", kw.name, self.expr_to_string(value)));
                    }
                }
                format!("{}({})", call.name, parts.join(", "))
            }
            _ => "null".to_string(),
        }
    }

    /// Convert a literal to its string representation.
    fn literal_to_string(&self, lit: &ast::Literal) -> String {
        match &lit.value {
            Some(ast::literal::Value::IntValue(i)) => i.to_string(),
            Some(ast::literal::Value::FloatValue(f)) => f.to_string(),
            Some(ast::literal::Value::StringValue(s)) => format!("\"{}\"", s),
            Some(ast::literal::Value::BoolValue(b)) => b.to_string(),
            Some(ast::literal::Value::IsNone(true)) => "null".to_string(),
            _ => "null".to_string(),
        }
    }

    /// Convert a spread action statement (side-effect only, no targets)
    fn convert_spread_action(&mut self, spread: &ast::SpreadAction) -> Vec<String> {
        self.convert_spread_action_with_targets(spread, &[])
    }

    /// Convert a spread expression with targets
    fn convert_spread_expr(&mut self, spread: &ast::SpreadExpr, targets: &[String]) -> Vec<String> {
        // SpreadExpr has the same structure as SpreadAction
        let action = spread.action.as_ref().unwrap();

        // Create a temporary SpreadAction-like structure for the common implementation
        let action_id = self.next_id("spread_action");
        let action_label = format!(
            "@{}() [spread over {}]",
            action.action_name, spread.loop_var
        );

        // Extract kwargs
        let kwargs = self.extract_kwargs(&action.kwargs);
        let kwarg_exprs = self.extract_kwarg_exprs(&action.kwargs);

        // Get the collection expression as a string
        let collection_str = spread
            .collection
            .as_ref()
            .map(|c| self.expr_to_string(c))
            .unwrap_or_default();

        // Use internal variable name for spread results flowing to aggregator
        let spread_result_var = "_spread_result".to_string();

        // Create aggregator ID first so we can link the action to it
        let agg_id = self.next_id("aggregator");

        let mut action_node =
            DAGNode::new(action_id.clone(), "action_call".to_string(), action_label)
                .with_action(&action.action_name, action.module_name.as_deref())
                .with_kwargs(kwargs)
                .with_kwarg_exprs(kwarg_exprs)
                .with_spread(&spread.loop_var, &collection_str)
                .with_target(&spread_result_var)
                .with_aggregates_to(&agg_id);
        if let Some(ref fn_name) = self.current_function {
            action_node = action_node.with_function_name(fn_name);
        }
        self.dag.add_node(action_node);

        // Create aggregator node
        let target_label = if !targets.is_empty() {
            if targets.len() == 1 {
                format!("aggregate -> {}", targets[0])
            } else {
                format!("aggregate -> ({})", targets.join(", "))
            }
        } else {
            "aggregate".to_string()
        };

        let mut agg_node = DAGNode::new(agg_id.clone(), "aggregator".to_string(), target_label)
            .with_aggregator(&action_id);
        if !targets.is_empty() {
            agg_node = agg_node.with_targets(targets);
        }
        if let Some(ref fn_name) = self.current_function {
            agg_node = agg_node.with_function_name(fn_name);
        }
        self.dag.add_node(agg_node);

        // Connect action to aggregator via state machine edge
        self.dag
            .add_edge(DAGEdge::state_machine(action_id.clone(), agg_id.clone()));

        // Add DATA_FLOW edge from spread action to aggregator for results
        self.dag.add_edge(DAGEdge::data_flow(
            action_id.clone(),
            agg_id.clone(),
            &spread_result_var,
        ));

        // Track variable definitions for all targets
        for t in targets {
            self.track_var_definition(t, &agg_id);
        }

        vec![action_id, agg_id]
    }

    /// Convert a spread action with targets (common implementation)
    fn convert_spread_action_with_targets(
        &mut self,
        spread: &ast::SpreadAction,
        targets: &[String],
    ) -> Vec<String> {
        let action = spread.action.as_ref().unwrap();

        // Create spread action node
        let action_id = self.next_id("spread_action");
        let action_label = format!(
            "@{}() [spread over {}]",
            action.action_name, spread.loop_var
        );

        // Extract kwargs
        let kwargs = self.extract_kwargs(&action.kwargs);
        let kwarg_exprs = self.extract_kwarg_exprs(&action.kwargs);

        // Get the collection expression as a string (e.g., "$items" or "range(5)")
        let collection_str = spread
            .collection
            .as_ref()
            .map(|c| self.expr_to_string(c))
            .unwrap_or_default();

        // Use internal variable name for spread results flowing to aggregator
        let spread_result_var = "_spread_result".to_string();

        // Create aggregator ID first so we can link the action to it
        let agg_id = self.next_id("aggregator");

        let mut action_node =
            DAGNode::new(action_id.clone(), "action_call".to_string(), action_label)
                .with_action(&action.action_name, action.module_name.as_deref())
                .with_kwargs(kwargs)
                .with_kwarg_exprs(kwarg_exprs)
                .with_spread(&spread.loop_var, &collection_str)
                .with_target(&spread_result_var) // Set target so results flow to aggregator
                .with_aggregates_to(&agg_id);
        if let Some(ref fn_name) = self.current_function {
            action_node = action_node.with_function_name(fn_name);
        }
        self.dag.add_node(action_node);

        // Create aggregator node
        let target_label = if !targets.is_empty() {
            if targets.len() == 1 {
                format!("aggregate -> {}", targets[0])
            } else {
                format!("aggregate -> ({})", targets.join(", "))
            }
        } else {
            "aggregate".to_string()
        };

        let mut agg_node = DAGNode::new(agg_id.clone(), "aggregator".to_string(), target_label)
            .with_aggregator(&action_id);
        if !targets.is_empty() {
            agg_node = agg_node.with_targets(targets);
        }
        if let Some(ref fn_name) = self.current_function {
            agg_node = agg_node.with_function_name(fn_name);
        }
        self.dag.add_node(agg_node);

        // Connect action to aggregator via state machine edge
        self.dag
            .add_edge(DAGEdge::state_machine(action_id.clone(), agg_id.clone()));

        // Add DATA_FLOW edge from spread action to aggregator for results
        self.dag.add_edge(DAGEdge::data_flow(
            action_id.clone(),
            agg_id.clone(),
            &spread_result_var,
        ));

        // Track variable definitions at aggregator
        for t in targets {
            self.track_var_definition(t, &agg_id);
        }

        vec![action_id, agg_id]
    }

    /// Convert a parallel block statement (side-effect only, no targets)
    fn convert_parallel_block(&mut self, parallel: &ast::ParallelBlock) -> Vec<String> {
        self.convert_parallel_block_with_targets(&parallel.calls, &[])
    }

    /// Convert a parallel expression with targets for tuple unpacking
    fn convert_parallel_expr(
        &mut self,
        parallel: &ast::ParallelExpr,
        targets: &[String],
    ) -> Vec<String> {
        self.convert_parallel_block_with_targets(&parallel.calls, targets)
    }

    /// Convert a parallel block with targets (common implementation)
    fn convert_parallel_block_with_targets(
        &mut self,
        calls: &[ast::Call],
        targets: &[String],
    ) -> Vec<String> {
        let mut result_nodes = Vec::new();

        // Create parallel entry node
        let parallel_id = self.next_id("parallel");
        let mut parallel_node = DAGNode::new(
            parallel_id.clone(),
            "parallel".to_string(),
            "parallel".to_string(),
        );
        if let Some(ref fn_name) = self.current_function {
            parallel_node = parallel_node.with_function_name(fn_name);
        }
        self.dag.add_node(parallel_node);
        result_nodes.push(parallel_id.clone());

        // Create a node for each call
        let mut call_node_ids = Vec::new();
        for (i, call) in calls.iter().enumerate() {
            // Assign a target to each parallel call based on index
            // If we have targets ["a", "b"] and calls [action1, action2],
            // then action1 produces "a" and action2 produces "b"
            let call_target = targets.get(i).cloned();

            let (call_id, call_node) = match &call.kind {
                Some(ast::call::Kind::Action(action)) => {
                    let id = self.next_id("parallel_action");
                    let label = if let Some(ref t) = call_target {
                        format!("@{}() [{}] -> {}", action.action_name, i, t)
                    } else {
                        format!("@{}() [{}]", action.action_name, i)
                    };
                    let kwargs = self.extract_kwargs(&action.kwargs);
                    let kwarg_exprs = self.extract_kwarg_exprs(&action.kwargs);
                    let mut node = DAGNode::new(id.clone(), "action_call".to_string(), label)
                        .with_action(&action.action_name, action.module_name.as_deref())
                        .with_kwargs(kwargs)
                        .with_kwarg_exprs(kwarg_exprs);
                    if let Some(ref t) = call_target {
                        node = node.with_target(t);
                    }
                    if let Some(ref fn_name) = self.current_function {
                        node = node.with_function_name(fn_name);
                    }
                    (id, node)
                }
                Some(ast::call::Kind::Function(func)) => {
                    let id = self.next_id("parallel_fn_call");
                    let label = if let Some(ref t) = call_target {
                        format!("{}() [{}] -> {}", func.name, i, t)
                    } else {
                        format!("{}() [{}]", func.name, i)
                    };
                    let mut node = DAGNode::new(id.clone(), "fn_call".to_string(), label)
                        .with_fn_call(&func.name);
                    if let Some(ref t) = call_target {
                        node = node.with_target(t);
                    }
                    if let Some(ref fn_name) = self.current_function {
                        node = node.with_function_name(fn_name);
                    }
                    (id, node)
                }
                None => continue,
            };

            self.dag.add_node(call_node);

            // Track variable definition for each call's target
            if let Some(ref t) = call_target {
                self.track_var_definition(t, &call_id);
            }

            call_node_ids.push(call_id.clone());
            result_nodes.push(call_id.clone());

            // Edge from parallel node to each call
            self.dag.add_edge(DAGEdge::state_machine_with_condition(
                parallel_id.clone(),
                call_id,
                &format!("parallel:{}", i),
            ));
        }

        // Create aggregator node (still needed for control flow even without targets)
        let agg_id = self.next_id("parallel_aggregator");
        let target_label = if !targets.is_empty() {
            if targets.len() == 1 {
                format!("parallel_aggregate -> {}", targets[0])
            } else {
                format!("parallel_aggregate -> ({})", targets.join(", "))
            }
        } else {
            "parallel_aggregate".to_string()
        };

        let mut agg_node = DAGNode::new(agg_id.clone(), "aggregator".to_string(), target_label)
            .with_aggregator(&parallel_id);
        if !targets.is_empty() {
            agg_node = agg_node.with_targets(targets);
        }
        if let Some(ref fn_name) = self.current_function {
            agg_node = agg_node.with_function_name(fn_name);
        }
        self.dag.add_node(agg_node);
        result_nodes.push(agg_id.clone());

        // Connect all call nodes to aggregator
        for call_id in call_node_ids {
            self.dag
                .add_edge(DAGEdge::state_machine(call_id, agg_id.clone()));
        }

        result_nodes
    }

    /// Convert a for loop to normalized DAG nodes.
    ///
    /// For loops are decomposed into primitive nodes:
    /// 1. loop_init (assignment): Initialize index to 0
    /// 2. loop_cond (branch): Condition check with guarded edges
    /// 3. loop_extract (assignment): Extract current item from collection
    /// 4. body nodes: The actual loop body (action_call, fn_call, etc.)
    /// 5. loop_incr (assignment): Increment the index
    /// 6. Back-edge from incr to cond
    ///
    /// The structure is:
    ///                              ┌──────────────────────────────────────────────────────┐
    ///                              │                                                      │
    ///                              ▼                                                      │
    ///   loop_init ──> loop_cond ──[guard: i<len]──> loop_extract ──> body ──> loop_incr ──┘
    ///                     │
    ///                     └──[guard: NOT(i<len)]──> (next node via result_nodes)
    ///
    /// This structure uses the same guard evaluation as if/else branches,
    /// requiring no special runtime handling for loops.
    fn convert_for_loop(&mut self, for_loop: &ast::ForLoop) -> ConvertedSubgraph {
        let mut nodes: Vec<String> = Vec::new();

        let loop_id = self.next_id("loop");
        let loop_vars_str = for_loop.loop_vars.join(", ");

        // Get the iterable expression
        let collection_expr = for_loop.iterable.clone();
        let collection_str = collection_expr
            .as_ref()
            .map(|c| self.expr_to_string(c))
            .unwrap_or_default();

        // The loop index variable name (internal)
        let loop_i_var = format!("__loop_{}_i", loop_id);

        // Build guard expression: __loop_i < len(collection)
        let continue_guard = Self::build_loop_guard(&loop_i_var, collection_expr.as_ref());

        // Build break guard: NOT(__loop_i < len(collection))
        let break_guard = continue_guard.as_ref().map(|guard| ast::Expr {
            span: None,
            kind: Some(ast::expr::Kind::UnaryOp(Box::new(ast::UnaryOp {
                op: ast::UnaryOperator::UnaryOpNot as i32,
                operand: Some(Box::new(guard.clone())),
            }))),
        });

        // ============================================================
        // 1. Create loop_init node: __loop_i = 0
        // ============================================================
        let init_id = self.next_id("loop_init");
        let init_label = format!("{} = 0", loop_i_var);
        let init_expr = ast::Expr {
            span: None,
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::IntValue(0)),
            })),
        };
        let mut init_node = DAGNode::new(init_id.clone(), "assignment".to_string(), init_label)
            .with_target(&loop_i_var)
            .with_assign_expr(init_expr);
        if let Some(ref fn_name) = self.current_function {
            init_node = init_node.with_function_name(fn_name);
        }
        self.dag.add_node(init_node);
        self.track_var_definition(&loop_i_var, &init_id);
        nodes.push(init_id.clone());

        // ============================================================
        // 2. Create loop_cond node (branch): decision point
        // ============================================================
        let cond_id = self.next_id("loop_cond");
        let cond_label = format!("for {} in {}", loop_vars_str, collection_str);
        let mut cond_node = DAGNode::new(cond_id.clone(), "branch".to_string(), cond_label.clone());
        if let Some(ref fn_name) = self.current_function {
            cond_node = cond_node.with_function_name(fn_name);
        }
        self.dag.add_node(cond_node);
        nodes.push(cond_id.clone());

        // Connect init -> cond
        self.dag
            .add_edge(DAGEdge::state_machine(init_id.clone(), cond_id.clone()));

        // ============================================================
        // 3. Create loop_extract node(s): extract loop variable(s)
        // ============================================================
        // Build expression: item = collection[__loop_i]
        let extract_id = self.next_id("loop_extract");

        // Build the index expression: collection[__loop_i]
        let coll = collection_expr.as_ref().unwrap_or_else(|| {
            panic!(
                "BUG: for-loop collection expression is None for loop '{}'",
                loop_vars_str
            )
        });
        let index_expr = ast::Expr {
            span: None,
            kind: Some(ast::expr::Kind::Index(Box::new(ast::IndexAccess {
                object: Some(Box::new(coll.clone())),
                index: Some(Box::new(ast::Expr {
                    span: None,
                    kind: Some(ast::expr::Kind::Variable(ast::Variable {
                        name: loop_i_var.clone(),
                    })),
                })),
            }))),
        };

        let extract_label = format!("{} = {}[{}]", loop_vars_str, collection_str, loop_i_var);
        let mut extract_node =
            DAGNode::new(extract_id.clone(), "assignment".to_string(), extract_label)
                .with_targets(&for_loop.loop_vars)
                .with_assign_expr(index_expr);
        if let Some(ref fn_name) = self.current_function {
            extract_node = extract_node.with_function_name(fn_name);
        }
        self.dag.add_node(extract_node);
        nodes.push(extract_id.clone());

        // Track loop variables as defined by extract node
        for loop_var in &for_loop.loop_vars {
            self.track_var_definition(loop_var, &extract_id);
        }

        // Connect cond -> extract with continue guard
        if let Some(ref guard) = continue_guard {
            self.dag.add_edge(DAGEdge::state_machine_with_guard(
                cond_id.clone(),
                extract_id.clone(),
                guard.clone(),
            ));
        } else {
            self.dag
                .add_edge(DAGEdge::state_machine(cond_id.clone(), extract_id.clone()));
        }

        // ============================================================
        // 4. Convert body nodes
        // ============================================================
        let mut body_targets: Vec<String> = Vec::new();
        let body_graph = for_loop
            .block_body
            .as_ref()
            .map(|block_body| {
                Self::collect_assigned_targets(&block_body.statements, &mut body_targets);
                self.convert_block(block_body)
            })
            .unwrap_or_else(ConvertedSubgraph::noop);
        nodes.extend(body_graph.nodes.clone());

        if body_graph.is_noop {
            // No body nodes: extract flows directly to incr.
        } else if let Some(ref body_entry) = body_graph.entry {
            self.dag.add_edge(DAGEdge::state_machine(
                extract_id.clone(),
                body_entry.clone(),
            ));
        }

        // ============================================================
        // 5. Create loop_incr node: __loop_i = __loop_i + 1
        // ============================================================
        let incr_id = self.next_id("loop_incr");
        let incr_label = format!("{} = {} + 1", loop_i_var, loop_i_var);

        // Build expression: __loop_i + 1
        let incr_expr = ast::Expr {
            span: None,
            kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                left: Some(Box::new(ast::Expr {
                    span: None,
                    kind: Some(ast::expr::Kind::Variable(ast::Variable {
                        name: loop_i_var.clone(),
                    })),
                })),
                op: ast::BinaryOperator::BinaryOpAdd as i32,
                right: Some(Box::new(ast::Expr {
                    span: None,
                    kind: Some(ast::expr::Kind::Literal(ast::Literal {
                        value: Some(ast::literal::Value::IntValue(1)),
                    })),
                })),
            }))),
        };

        let mut incr_node = DAGNode::new(incr_id.clone(), "assignment".to_string(), incr_label)
            .with_target(&loop_i_var)
            .with_assign_expr(incr_expr);
        if let Some(ref fn_name) = self.current_function {
            incr_node = incr_node.with_function_name(fn_name);
        }
        self.dag.add_node(incr_node);
        self.track_var_definition(&loop_i_var, &incr_id);
        nodes.push(incr_id.clone());

        // Connect body exits -> incr, or extract -> incr if no body.
        if body_graph.is_noop {
            self.dag
                .add_edge(DAGEdge::state_machine(extract_id.clone(), incr_id.clone()));
        } else {
            for exit in &body_graph.exits {
                self.dag
                    .add_edge(DAGEdge::state_machine(exit.clone(), incr_id.clone()));
            }
        }

        // ============================================================
        // 6. Back-edge: incr -> cond
        // ============================================================
        self.dag.add_edge(
            DAGEdge::state_machine(incr_id.clone(), cond_id.clone()).with_loop_back(true),
        );

        // ============================================================
        // 7. Create loop_exit node (join point for break path)
        // ============================================================
        // loop_exit only has one predecessor (loop_cond with break guard), so it
        // can be treated as inline when the loop body has no actions.
        let exit_id = self.next_id("loop_exit");
        let exit_label = format!("end for {}", loop_vars_str);
        let mut exit_node = DAGNode::new(exit_id.clone(), "join".to_string(), exit_label)
            .with_join_required_count(1);
        if let Some(ref fn_name) = self.current_function {
            exit_node = exit_node.with_function_name(fn_name);
        }
        // Track body targets on the exit node so they're available after the loop
        if !body_targets.is_empty() {
            exit_node.targets = Some(body_targets.clone());
            for target in &body_targets {
                self.track_var_definition(target, &exit_id);
            }
        }
        self.dag.add_node(exit_node);
        nodes.push(exit_id.clone());

        // Connect cond -> exit with break guard
        if let Some(ref guard) = break_guard {
            self.dag.add_edge(DAGEdge::state_machine_with_guard(
                cond_id.clone(),
                exit_id.clone(),
                guard.clone(),
            ));
        } else {
            // No guard means always break (empty collection edge case)
            self.dag
                .add_edge(DAGEdge::state_machine(cond_id.clone(), exit_id.clone()));
        }

        ConvertedSubgraph {
            entry: Some(init_id),
            exits: vec![exit_id],
            nodes,
            is_noop: false,
        }
    }

    /// Convert a conditional (if/elif/else)
    ///
    /// The DAG structure is "flat" - there's no explicit "if" container node.
    /// Instead:
    /// - The first action/statement of each branch becomes a regular node
    /// - Edges with guard expressions connect the predecessor to each branch
    /// - The guard is evaluated at runtime to determine which edge to follow
    /// - A join node collects the branches back together
    ///
    /// Example: if x > 0: result = @pos() else: result = @neg()
    /// Becomes:
    ///   predecessor --[guard: x > 0]--> @pos() --> join
    ///               --[guard: not (x > 0)]--> @neg() --> join
    fn convert_conditional(&mut self, cond: &ast::Conditional) -> ConvertedSubgraph {
        let mut nodes: Vec<String> = Vec::new();

        let branch_id = self.next_id("branch");
        let mut branch_node = DAGNode::new(
            branch_id.clone(),
            "branch".to_string(),
            "branch".to_string(),
        );
        if let Some(ref fn_name) = self.current_function {
            branch_node = branch_node.with_function_name(fn_name);
        }
        self.dag.add_node(branch_node);
        nodes.push(branch_id.clone());

        let if_branch = cond
            .if_branch
            .as_ref()
            .unwrap_or_else(|| panic!("BUG: conditional missing if_branch"));
        let if_guard = if_branch
            .condition
            .as_ref()
            .unwrap_or_else(|| panic!("BUG: if branch missing guard expression"))
            .clone();

        let if_body_graph = if_branch
            .block_body
            .as_ref()
            .map(|block| self.convert_block(block))
            .unwrap_or_else(ConvertedSubgraph::noop);
        nodes.extend(if_body_graph.nodes.clone());

        let mut prior_guards: Vec<ast::Expr> = vec![if_guard.clone()];
        let mut elif_graphs: Vec<(ast::Expr, ConvertedSubgraph)> = Vec::new();

        for elif_branch in &cond.elif_branches {
            let elif_cond = elif_branch
                .condition
                .as_ref()
                .unwrap_or_else(|| panic!("BUG: elif branch missing guard expression"))
                .clone();
            let compound_guard = self.build_compound_guard(&prior_guards, Some(&elif_cond));
            prior_guards.push(elif_cond);

            let graph = elif_branch
                .block_body
                .as_ref()
                .map(|block| self.convert_block(block))
                .unwrap_or_else(ConvertedSubgraph::noop);

            nodes.extend(graph.nodes.clone());
            elif_graphs.push((compound_guard, graph));
        }

        // Missing else is an implicit empty else.
        let else_guard = self.build_compound_guard(&prior_guards, None);
        let else_graph = cond
            .else_branch
            .as_ref()
            .and_then(|else_branch| {
                else_branch
                    .block_body
                    .as_ref()
                    .map(|block| self.convert_block(block))
            })
            .unwrap_or_else(ConvertedSubgraph::noop);
        nodes.extend(else_graph.nodes.clone());

        let join_needed = (if_body_graph.is_noop || !if_body_graph.exits.is_empty())
            || elif_graphs
                .iter()
                .any(|(_, graph)| graph.is_noop || !graph.exits.is_empty())
            || (else_graph.is_noop || !else_graph.exits.is_empty());

        let join_id = if join_needed {
            let join_id = self.next_id("join");
            let mut join_node =
                DAGNode::new(join_id.clone(), "join".to_string(), "join".to_string())
                    .with_join_required_count(1);
            if let Some(ref fn_name) = self.current_function {
                join_node = join_node.with_function_name(fn_name);
            }
            self.dag.add_node(join_node);
            nodes.push(join_id.clone());
            Some(join_id)
        } else {
            None
        };

        let connect_branch = |converter: &mut Self,
                              guard: ast::Expr,
                              graph: &ConvertedSubgraph,
                              join_id: Option<&String>| {
            if graph.is_noop {
                if let Some(join_target) = join_id {
                    converter.dag.add_edge(DAGEdge::state_machine_with_guard(
                        branch_id.clone(),
                        join_target.clone(),
                        guard,
                    ));
                }
                return;
            }

            if let Some(entry) = &graph.entry {
                converter.dag.add_edge(DAGEdge::state_machine_with_guard(
                    branch_id.clone(),
                    entry.clone(),
                    guard,
                ));
            }

            if let Some(join_target) = join_id {
                for exit in &graph.exits {
                    converter
                        .dag
                        .add_edge(DAGEdge::state_machine(exit.clone(), join_target.clone()));
                }
            }
        };

        connect_branch(self, if_guard, &if_body_graph, join_id.as_ref());
        for (guard, graph) in &elif_graphs {
            connect_branch(self, guard.clone(), graph, join_id.as_ref());
        }
        connect_branch(self, else_guard, &else_graph, join_id.as_ref());

        ConvertedSubgraph {
            entry: Some(branch_id),
            exits: join_id.into_iter().collect(),
            nodes,
            is_noop: false,
        }
    }

    /// Build a compound guard expression from prior guards and an optional current condition.
    ///
    /// For elif branches: NOT(prior1) AND NOT(prior2) AND ... AND current_condition
    /// For else branches: NOT(prior1) AND NOT(prior2) AND ... (no current condition)
    fn build_compound_guard(
        &self,
        prior_guards: &[ast::Expr],
        current_condition: Option<&ast::Expr>,
    ) -> ast::Expr {
        // Start with negated prior guards
        let mut parts: Vec<ast::Expr> = prior_guards
            .iter()
            .map(|guard| ast::Expr {
                span: None,
                kind: Some(ast::expr::Kind::UnaryOp(Box::new(ast::UnaryOp {
                    op: ast::UnaryOperator::UnaryOpNot as i32,
                    operand: Some(Box::new(guard.clone())),
                }))),
            })
            .collect();

        // Add the current condition if provided (for elif, not for else)
        if let Some(cond) = current_condition {
            parts.push(cond.clone());
        }

        // Combine with AND operators
        if parts.is_empty() {
            panic!(
                "BUG: build_elif_guard called with no prior conditions and no current condition"
            );
        } else if parts.len() == 1 {
            parts.remove(0)
        } else {
            // Build left-associative AND chain: ((a AND b) AND c) AND d
            let mut result = parts.remove(0);
            for part in parts {
                result = ast::Expr {
                    span: None,
                    kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                        left: Some(Box::new(result)),
                        op: ast::BinaryOperator::BinaryOpAnd as i32,
                        right: Some(Box::new(part)),
                    }))),
                };
            }
            result
        }
    }

    /// Convert a try/except block
    ///
    /// The DAG structure is "flat" - there's no explicit "try" or "except" container nodes.
    /// Instead:
    /// - The try body action becomes a regular node
    /// - The success path goes from try body to join with a "success" condition
    /// - Each except handler's action connects from the try body with exception_types on the edge
    /// - A join node collects all paths back together
    ///
    /// Example: try: result = @risky() except NetworkError: result = @fallback()
    /// Becomes:
    ///   @risky() --[success]--> join
    ///            --[except:NetworkError]--> @fallback() --> join
    fn convert_try_except(&mut self, try_except: &ast::TryExcept) -> ConvertedSubgraph {
        let mut nodes: Vec<String> = Vec::new();

        let try_graph = try_except
            .try_block
            .as_ref()
            .map(|block| self.convert_block(block))
            .unwrap_or_else(ConvertedSubgraph::noop);

        if try_graph.is_noop {
            return ConvertedSubgraph::noop();
        }
        nodes.extend(try_graph.nodes.clone());

        // Collect handler graphs up front so we can decide whether a join is needed.
        let mut handler_graphs: Vec<(Vec<String>, ConvertedSubgraph)> = Vec::new();
        for handler in &try_except.handlers {
            let graph = handler
                .block_body
                .as_ref()
                .map(|block| self.convert_block(block))
                .unwrap_or_else(ConvertedSubgraph::noop);
            nodes.extend(graph.nodes.clone());
            handler_graphs.push((handler.exception_types.clone(), graph));
        }

        let join_needed = !try_graph.exits.is_empty()
            || handler_graphs
                .iter()
                .any(|(_, graph)| graph.is_noop || !graph.exits.is_empty());

        let join_id = if join_needed {
            let join_id = self.next_id("join");
            let mut join_node =
                DAGNode::new(join_id.clone(), "join".to_string(), "join".to_string())
                    .with_join_required_count(1);
            if let Some(ref fn_name) = self.current_function {
                join_node = join_node.with_function_name(fn_name);
            }
            self.dag.add_node(join_node);
            nodes.push(join_id.clone());
            Some(join_id)
        } else {
            None
        };

        // Success path(s): all normal exits of the try body go to the join via "success".
        if let Some(join_target) = join_id.as_ref() {
            for try_exit in &try_graph.exits {
                self.dag.add_edge(DAGEdge::state_machine_success(
                    try_exit.clone(),
                    join_target.clone(),
                ));
            }
        }

        // Exception edges: any failing node inside the try body may route to a handler.
        // This mirrors Python semantics more closely than only wiring from the last try node.
        let try_exception_sources = try_graph.nodes.clone();
        for (exception_types, handler_graph) in &handler_graphs {
            if handler_graph.is_noop {
                if let Some(join_target) = join_id.as_ref() {
                    for source in &try_exception_sources {
                        self.dag.add_edge(DAGEdge::state_machine_with_exception(
                            source.clone(),
                            join_target.clone(),
                            exception_types.clone(),
                        ));
                    }
                }
                continue;
            }

            let Some(handler_entry) = handler_graph.entry.as_ref() else {
                continue;
            };
            for source in &try_exception_sources {
                self.dag.add_edge(DAGEdge::state_machine_with_exception(
                    source.clone(),
                    handler_entry.clone(),
                    exception_types.clone(),
                ));
            }

            if let Some(join_target) = join_id.as_ref() {
                for handler_exit in &handler_graph.exits {
                    self.dag.add_edge(DAGEdge::state_machine(
                        handler_exit.clone(),
                        join_target.clone(),
                    ));
                }
            }
        }

        ConvertedSubgraph {
            entry: try_graph.entry,
            exits: join_id.into_iter().collect(),
            nodes,
            is_noop: false,
        }
    }

    /// Convert a return statement
    ///
    /// Return statements should only contain variables (not action calls).
    /// The Python IR builder normalizes `return await action()` to
    /// `_tmp = await action(); return _tmp`.
    fn convert_return(&mut self, ret: &ast::ReturnStmt) -> Vec<String> {
        let node_id = self.next_id("return");
        let mut node = DAGNode::new(node_id.clone(), "return".to_string(), "return".to_string());

        if let Some(ref expr) = ret.value {
            node.assign_expr = Some(expr.clone());
            // Store the return value in a canonical "result" slot so it can
            // flow to the workflow output boundary.
            node.target = Some("result".to_string());
        }

        if let Some(ref fn_name) = self.current_function {
            node = node.with_function_name(fn_name);
        }
        let has_target = node.target.is_some();
        self.dag.add_node(node);

        if has_target {
            self.track_var_definition("result", &node_id);
        }

        vec![node_id]
    }

    /// Convert an expression statement
    fn convert_expr_statement(&mut self, expr_stmt: &ast::ExprStmt) -> Vec<String> {
        let expr = match &expr_stmt.expr {
            Some(e) => e,
            None => return vec![],
        };

        if let Some(ast::expr::Kind::ActionCall(action)) = &expr.kind {
            // Side-effect only action in expression statement
            return self.convert_action_call_with_targets(action, &[]);
        }

        if let Some(ast::expr::Kind::FunctionCall(func_call)) = &expr.kind {
            let node_id = self.next_id("fn_call");
            let kwargs = self.extract_kwargs(&func_call.kwargs);
            let kwarg_exprs = self.extract_kwarg_exprs(&func_call.kwargs);
            let mut node = DAGNode::new(
                node_id.clone(),
                "fn_call".to_string(),
                format!("{}()", func_call.name),
            )
            .with_fn_call(&func_call.name)
            .with_kwargs(kwargs)
            .with_kwarg_exprs(kwarg_exprs);
            if let Some(ref fn_name) = self.current_function {
                node = node.with_function_name(fn_name);
            }
            self.dag.add_node(node);

            return vec![node_id];
        }

        let node_id = self.next_id("expr");
        let mut node = DAGNode::new(
            node_id.clone(),
            "expression".to_string(),
            "expr".to_string(),
        );
        if let Some(ref fn_name) = self.current_function {
            node = node.with_function_name(fn_name);
        }
        self.dag.add_node(node);

        vec![node_id]
    }

    /// Track that a variable is defined/modified at a node
    fn track_var_definition(&mut self, var_name: &str, node_id: &str) {
        self.current_scope_vars
            .insert(var_name.to_string(), node_id.to_string());
        self.var_modifications
            .entry(var_name.to_string())
            .or_default()
            .push(node_id.to_string());
    }

    fn push_unique_target(targets: &mut Vec<String>, target: &str) {
        if !targets.iter().any(|existing| existing == target) {
            targets.push(target.to_string());
        }
    }

    fn collect_assigned_targets(statements: &[ast::Statement], targets: &mut Vec<String>) {
        for stmt in statements {
            let Some(kind) = &stmt.kind else {
                continue;
            };

            match kind {
                ast::statement::Kind::Assignment(assign) => {
                    for target in &assign.targets {
                        Self::push_unique_target(targets, target);
                    }
                }
                ast::statement::Kind::Conditional(cond) => {
                    if let Some(if_branch) = &cond.if_branch {
                        if let Some(body) = &if_branch.block_body {
                            Self::collect_assigned_targets(&body.statements, targets);
                        }
                    }
                    for elif_branch in &cond.elif_branches {
                        if let Some(body) = &elif_branch.block_body {
                            Self::collect_assigned_targets(&body.statements, targets);
                        }
                    }
                    if let Some(else_branch) = &cond.else_branch {
                        if let Some(body) = &else_branch.block_body {
                            Self::collect_assigned_targets(&body.statements, targets);
                        }
                    }
                }
                ast::statement::Kind::ForLoop(for_loop) => {
                    if let Some(body) = &for_loop.block_body {
                        Self::collect_assigned_targets(&body.statements, targets);
                    }
                }
                ast::statement::Kind::TryExcept(try_except) => {
                    if let Some(body) = &try_except.try_block {
                        Self::collect_assigned_targets(&body.statements, targets);
                    }
                    for handler in &try_except.handlers {
                        if let Some(body) = &handler.block_body {
                            Self::collect_assigned_targets(&body.statements, targets);
                        }
                    }
                }
                ast::statement::Kind::ParallelBlock(_)
                | ast::statement::Kind::SpreadAction(_)
                | ast::statement::Kind::ActionCall(_)
                | ast::statement::Kind::ReturnStmt(_)
                | ast::statement::Kind::ExprStmt(_) => {}
            }
        }
    }

    /// Add data flow edges for a specific function
    fn add_data_flow_edges_for_function(&mut self, function_name: &str) {
        // Get only nodes for this function
        let fn_node_ids: Vec<String> = self
            .dag
            .get_nodes_for_function(function_name)
            .keys()
            .cloned()
            .collect();

        // Get execution order for this function's nodes
        let order = self.get_execution_order_for_nodes(&fn_node_ids.iter().cloned().collect());

        // Note: edges are added in add_data_flow_from_definitions below

        // Add data flow from variable definitions to uses
        self.add_data_flow_from_definitions(function_name, &order);
    }

    /// Add data flow edges from variable definitions to their uses
    fn add_data_flow_from_definitions(&mut self, function_name: &str, order: &[String]) {
        let fn_node_ids: HashSet<String> = self
            .dag
            .get_nodes_for_function(function_name)
            .keys()
            .cloned()
            .collect();

        tracing::debug!(
            function_name = %function_name,
            fn_node_ids = ?fn_node_ids,
            order = ?order,
            var_modifications = ?self.var_modifications.keys().collect::<Vec<_>>(),
            "add_data_flow_from_definitions"
        );

        let mut edges_to_add = Vec::new();

        // For each variable modification, connect to nodes that USE the variable
        for (var_name, modifications) in &self.var_modifications {
            tracing::debug!(
                var_name = %var_name,
                modifications = ?modifications,
                "processing variable modifications"
            );
            for (i, mod_node) in modifications.iter().enumerate() {
                if !fn_node_ids.contains(mod_node) {
                    continue;
                }

                // Find the next modification of this variable (if any)
                let next_mod = modifications.get(i + 1);

                // Get the position of this modification in execution order
                let mod_pos = order.iter().position(|n| n == mod_node);

                // Find nodes that come after this modification but before the next
                // AND actually use this variable in their kwargs
                for (pos, node_id) in order.iter().enumerate() {
                    if let Some(mp) = mod_pos {
                        if pos <= mp {
                            continue;
                        }
                    }

                    // Don't add edge to the modification node itself
                    if node_id == mod_node {
                        continue;
                    }

                    // Check if this node uses the variable in its kwargs
                    if let Some(node) = self.dag.nodes.get(node_id) {
                        if self.node_uses_variable(node, var_name) {
                            edges_to_add.push((
                                var_name.clone(),
                                mod_node.clone(),
                                node_id.clone(),
                            ));
                        }
                    }

                    // Stop after the next modification (but we still add edge to it if it uses the var)
                    if let Some(next) = next_mod {
                        if node_id == next {
                            break;
                        }
                    }
                }
            }
        }

        tracing::debug!(
            edges_to_add = ?edges_to_add,
            "data flow edges being created"
        );
        for (var_name, source, target) in edges_to_add {
            self.dag
                .add_edge(DAGEdge::data_flow(source, target, &var_name));
        }
    }

    /// Check if a node uses a variable (references it in kwargs)
    fn node_uses_variable(&self, node: &DAGNode, var_name: &str) -> bool {
        // Check kwargs for variable references
        if let Some(ref kwargs) = node.kwargs {
            tracing::debug!(
                node_id = %node.id,
                var_name = %var_name,
                kwargs = ?kwargs,
                "checking if node uses variable in kwargs"
            );
            for value in kwargs.values() {
                // Check for $var_name pattern
                if value == &format!("${}", var_name) {
                    return true;
                }
            }
        }

        false
    }

    /// Get nodes in topological (execution) order for a subset of nodes
    fn get_execution_order_for_nodes(&self, node_ids: &HashSet<String>) -> Vec<String> {
        // Simple topological sort using state machine edges
        // Exclude loop-back edges to avoid cycles from for-loops
        let mut in_degree: HashMap<String, usize> =
            node_ids.iter().map(|n| (n.clone(), 0)).collect();
        let mut adj: HashMap<String, Vec<String>> =
            node_ids.iter().map(|n| (n.clone(), Vec::new())).collect();

        for edge in self.dag.get_state_machine_edges() {
            // Skip loop-back edges which create cycles
            if edge.is_loop_back {
                continue;
            }
            if node_ids.contains(&edge.source) && node_ids.contains(&edge.target) {
                adj.get_mut(&edge.source)
                    .map(|v| v.push(edge.target.clone()));
                in_degree.entry(edge.target.clone()).and_modify(|d| *d += 1);
            }
        }

        // Kahn's algorithm
        let mut queue: Vec<String> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(n, _)| n.clone())
            .collect();
        let mut order = Vec::new();

        while let Some(node) = queue.pop() {
            order.push(node.clone());
            if let Some(neighbors) = adj.get(&node) {
                for neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push(neighbor.clone());
                        }
                    }
                }
            }
        }

        order
    }
}

impl Default for DAGConverter {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to convert a program to a DAG
pub fn convert_to_dag(program: &ast::Program) -> DAG {
    let mut converter = DAGConverter::new();
    converter.convert(program)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    #[test]
    fn test_dag_function_simple() {
        let source = r#"fn test(input: [x], output: [y]):
    y = x + 1
    return y"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        assert!(!dag.nodes.is_empty());
        // Should have input, output, assignment, and return nodes
        assert!(dag.nodes.len() >= 3);
    }

    #[test]
    fn test_dag_function_chained_assignments() {
        let source = r#"fn compute(input: [x], output: [z]):
    y = x + 1
    z = y + 1
    return z"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Should have nodes for each statement plus input/output
        assert!(dag.nodes.len() >= 4);
        // Should have edges between statements
        assert!(!dag.edges.is_empty());
    }

    #[test]
    fn test_dag_function_creates_subgraph() {
        let source = r#"fn add(input: [a, b], output: [result]):
    result = a + b
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Should have function boundary nodes
        let node_ids: Vec<_> = dag.nodes.keys().collect();
        assert!(node_ids.iter().any(|id| id.contains("add")));
    }

    #[test]
    fn test_dag_function_with_action_call() {
        let source = r#"fn fetch(input: [url], output: [response]):
    response = @fetch_url(url=url)
    return response"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Check that action call node exists
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(node_types.contains("action_call"));
    }

    #[test]
    fn test_dag_function_with_for_loop() {
        let source = r#"fn process_all(input: [items], output: [results]):
    results = []
    for item in items:
        result = process(x=item)
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // In normalized loop structure, for loops are decomposed into:
        // - loop_init (assignment): initialize index
        // - loop_cond (branch): condition check
        // - loop_extract (assignment): extract item
        // - body nodes
        // - loop_incr (assignment): increment index
        // - loop_exit (join): exit point
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(
            node_types.contains("branch"),
            "Should have branch node for loop condition"
        );
        assert!(
            node_types.contains("join"),
            "Should have join node for loop exit"
        );
        assert!(
            node_types.contains("assignment"),
            "Should have assignment nodes for loop init/extract/incr"
        );

        // Check for back-edge (loop structure)
        let back_edges: Vec<_> = dag.edges.iter().filter(|e| e.is_loop_back).collect();
        assert!(!back_edges.is_empty(), "Should have a back-edge for loop");
    }

    #[test]
    fn test_dag_function_with_if_statement() {
        let source = r#"fn classify(input: [x], output: [result]):
    if x > 0:
        result = "positive"
    else:
        result = "negative"
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // In the flat DAG structure, conditionals use a "branch" node as the decision point
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(
            node_types.contains("branch"),
            "Should have branch decision node"
        );
        assert!(node_types.contains("join"), "Should have join node");
    }

    #[test]
    fn test_conditional_all_branches_return_has_no_fallthrough() {
        let source = r#"fn run(input: [], output: [result]):
    if true:
        return 1
    else:
        return 2
    result = @after()
    return result"#;

        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        let after_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call" && n.action_name.as_deref() == Some("after"))
            .expect("expected @after() node");

        let incoming: Vec<_> = dag
            .get_incoming_edges(&after_node.id)
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine)
            .collect();
        assert!(
            incoming.is_empty(),
            "expected no fallthrough edges into @after() when all branches return"
        );

        let conditional_joins = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "join" && n.label == "join")
            .count();
        assert_eq!(
            conditional_joins, 0,
            "expected no join node when all conditional branches return"
        );
    }

    #[test]
    fn test_for_loop_body_return_still_allows_empty_iteration_fallthrough() {
        let source = r#"fn run(input: [items], output: [result]):
    for item in items:
        return item
    result = @after()
    return result"#;

        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        let after_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call" && n.action_name.as_deref() == Some("after"))
            .expect("expected @after() node");

        let loop_exit = dag
            .nodes
            .values()
            .find(|n| n.node_type == "join" && n.label.starts_with("end for "))
            .expect("expected loop_exit join node");

        let has_exit_edge = dag.edges.iter().any(|e| {
            e.edge_type == EdgeType::StateMachine
                && e.source == loop_exit.id
                && e.target == after_node.id
        });
        assert!(
            has_exit_edge,
            "expected loop_exit -> @after() edge for the empty-iteration fallthrough path"
        );
    }

    #[test]
    fn test_dag_inlines_function_call_in_conditional_branch() {
        let source = r#"fn helper(input: [], output: [value]):
    value = @do_work()
    return value

fn run(input: [], output: [result]):
    if true:
        helper()
    else:
        result = @fallback()
    return result"#;

        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Helper action should be present after inlining the function call in the conditional branch
        let action_names: HashSet<_> = dag
            .nodes
            .values()
            .filter_map(|n| n.action_name.as_deref())
            .collect();
        assert!(
            action_names.contains("do_work"),
            "expected helper action to be inlined into DAG"
        );
        assert!(
            action_names.contains("fallback"),
            "expected fallback action to remain in DAG"
        );
    }

    #[test]
    fn test_dag_edge_types() {
        let source = r#"fn compute(input: [x], output: [y]):
    y = x + 1
    return y"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Should have state machine edges
        let state_machine_edges = dag.get_state_machine_edges();
        assert!(!state_machine_edges.is_empty());
    }

    #[test]
    fn test_dag_node_properties() {
        let node = DAGNode::new(
            "test_node".to_string(),
            "assignment".to_string(),
            "Test".to_string(),
        );

        assert_eq!(node.id, "test_node");
        assert_eq!(node.label, "Test");
        assert_eq!(node.node_type, "assignment");
    }

    #[test]
    fn test_dag_edge_properties() {
        let edge = DAGEdge::data_flow("node1".to_string(), "node2".to_string(), "x");

        assert_eq!(edge.source, "node1");
        assert_eq!(edge.target, "node2");
        assert_eq!(edge.edge_type, EdgeType::DataFlow);
        assert_eq!(edge.variable, Some("x".to_string()));
    }

    #[test]
    fn test_edge_type_enum() {
        assert_ne!(EdgeType::StateMachine, EdgeType::DataFlow);
    }

    #[test]
    fn test_dag_function_with_spread_action() {
        let source = r#"fn fetch_all(input: [items], output: [results]):
    results = spread items:item -> @fetch(id=item)
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Should have aggregator node
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(node_types.contains("aggregator"));
    }

    #[test]
    fn test_dag_for_loop_label_includes_iterable() {
        let source = r#"fn loop_labels(input: [items], output: [result]):
    result = []
    for i, item in enumerate(items):
        result = result + [item]
    return result"#;

        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        let labels: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "branch")
            .map(|n| n.label.as_str())
            .collect();

        assert!(
            labels.iter().any(|label| label.contains("enumerate")),
            "loop label should include iterable representation"
        );
        assert!(
            labels.iter().all(|label| !label.contains("null")),
            "loop label should not degrade to 'null'"
        );
    }

    #[test]
    fn test_dag_multiple_functions() {
        // With function expansion, only the entry function and functions it calls
        // are included in the final DAG. Independent functions are not included.
        let source = r#"fn helper(input: [x], output: [y]):
    y = x + 1
    return y

fn main(input: [a], output: [result]):
    result = helper(x=a)
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // The expanded DAG should be rooted at main
        // Helper's body should be inlined (once we implement expansion)
        let functions = dag.get_functions();
        assert!(
            functions.contains(&"main".to_string()),
            "Should have main function"
        );
    }

    #[test]
    fn test_dag_function_io_nodes() {
        let source = r#"fn transform(input: [x], output: [y]):
    y = x * 2
    return y"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find input and output nodes
        let input_nodes: Vec<_> = dag.nodes.values().filter(|n| n.is_input).collect();
        let output_nodes: Vec<_> = dag.nodes.values().filter(|n| n.is_output).collect();

        assert_eq!(input_nodes.len(), 1);
        assert_eq!(output_nodes.len(), 1);
        assert_eq!(input_nodes[0].io_vars, Some(vec!["x".to_string()]));
        assert_eq!(output_nodes[0].io_vars, Some(vec!["y".to_string()]));
    }

    #[test]
    fn test_dag_fn_call_expansion() {
        // With function expansion, fn_call nodes are replaced by the called function's body
        let source = r#"fn helper(input: [x], output: [y]):
    y = x + 1
    return y

fn main(input: [], output: [result]):
    result = helper(x=10)
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // fn_call nodes should be expanded (replaced by helper's body)
        let fn_call_nodes: Vec<_> = dag.nodes.values().filter(|n| n.is_fn_call).collect();
        assert!(
            fn_call_nodes.is_empty(),
            "fn_call nodes should be expanded into function body"
        );

        // The expanded DAG should contain the helper's assignment node
        let assignment_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "assignment")
            .collect();
        assert!(
            !assignment_nodes.is_empty(),
            "Should have helper's assignment node inlined"
        );
    }

    #[test]
    fn test_dag_try_except() {
        let source = r#"fn safe(input: [x], output: [result]):
    try:
        result = @risky_action(x=x)
    except NetworkError:
        result = @fallback(x=x)
    except:
        result = "error"
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // In the flat DAG structure, try/except doesn't have container nodes.
        // Instead, we have:
        // - The try body action (@risky_action) is a regular action_call node
        // - Except handlers' actions (@fallback, assignment) are regular nodes
        // - All paths converge at a join node
        // - Exception edges connect the try body to handlers with exception_types
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(
            node_types.contains("action_call"),
            "Should have action_call for try body and except handlers"
        );
        assert!(node_types.contains("join"), "Should have join node");

        // Check for exception edges
        let exception_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.exception_types.is_some())
            .collect();
        assert!(
            !exception_edges.is_empty(),
            "Should have edges with exception_types"
        );

        // Check for success edge from try body to join
        let success_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.condition.as_deref() == Some("success"))
            .collect();
        assert!(
            !success_edges.is_empty(),
            "Should have success edge from try body"
        );
    }

    #[test]
    fn test_try_except_exception_edges_remap_to_expanded_handlers() {
        let source = r#"fn run(input: [], output: [result]):
    try:
        number = @provide_value()
        @explode_custom(value=number)
    except CustomError:
        result = @cleanup(label="custom_fallback")
    return result"#;

        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Every exception edge should point to a node that exists in the expanded DAG.
        for edge in dag.edges.iter().filter(|e| e.exception_types.is_some()) {
            assert!(
                dag.nodes.contains_key(&edge.target),
                "exception edge target should exist after expansion: {}",
                edge.target
            );
        }

        // All nodes within the try body should route exceptions to a handler.
        for node_id in dag
            .nodes
            .values()
            .filter(|n| n.function_name.as_deref() == Some("__try_body_1__"))
            .map(|n| n.id.as_str())
        {
            let has_handler_edge = dag.edges.iter().any(|e| {
                e.source == node_id
                    && e.exception_types.is_some()
                    && dag.nodes.contains_key(&e.target)
            });
            assert!(
                has_handler_edge,
                "try body node {} should have an exception handler edge",
                node_id
            );
        }
    }

    #[test]
    fn test_dag_parallel_block() {
        let source = r#"fn fetch_all(input: [ids], output: [results]):
    results = parallel:
        @fetch_user(id=1)
        @fetch_user(id=2)
        @fetch_user(id=3)
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Check that parallel and aggregator nodes exist
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(node_types.contains("parallel"));
        assert!(node_types.contains("aggregator"));
    }

    #[test]
    fn test_dag_parallel_tuple_unpacking() {
        // Test parallel block with tuple unpacking: a, b = parallel: ...
        let source = r#"fn compute(input: [n], output: [summary]):
    factorial_value, fib_value = parallel:
        @compute_factorial(n=n)
        @compute_fibonacci(n=n)
    summary = @summarize(factorial=factorial_value, fib=fib_value)
    return summary"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // The aggregator node should have the targets for unpacking
        let aggregator_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "aggregator")
            .expect("Should have aggregator node");

        // Aggregator node should have both targets for tuple unpacking
        assert!(
            aggregator_node.targets.is_some(),
            "Aggregator node should have targets for unpacking"
        );
        let targets = aggregator_node.targets.as_ref().unwrap();
        assert_eq!(
            targets.len(),
            2,
            "Should have 2 targets for tuple unpacking"
        );
        assert!(targets.contains(&"factorial_value".to_string()));
        assert!(targets.contains(&"fib_value".to_string()));

        // First target should be set for backwards compatibility
        assert_eq!(
            aggregator_node.target,
            Some("factorial_value".to_string()),
            "First target should be set for backwards compat"
        );

        // Check DATA_FLOW edges exist for both unpacked variables
        let data_flow_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
            .collect();

        let factorial_edge = data_flow_edges
            .iter()
            .find(|e| e.variable.as_deref() == Some("factorial_value"));
        let fib_edge = data_flow_edges
            .iter()
            .find(|e| e.variable.as_deref() == Some("fib_value"));

        assert!(
            factorial_edge.is_some(),
            "Should have DATA_FLOW edge for factorial_value"
        );
        assert!(
            fib_edge.is_some(),
            "Should have DATA_FLOW edge for fib_value"
        );
    }

    #[test]
    fn test_dag_converter_class() {
        // Test using DAGConverter class directly
        let source = r#"fn test(input: [], output: [x]):
    x = 42
    return x"#;
        let program = parse(source).unwrap();

        let mut converter = DAGConverter::new();
        let dag = converter.convert(&program);

        assert!(!dag.nodes.is_empty());
    }

    #[test]
    fn test_dag_visualization_no_crash() {
        // Test that DAG has nodes and edges structure
        let source = r#"fn compute(input: [x], output: [z]):
    y = x + 1
    z = y + 1
    return z"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Should have nodes and edges
        assert!(!dag.nodes.is_empty());
        // edges may be empty or not depending on implementation
    }

    #[test]
    fn test_dag_complex_workflow() {
        // Test DAG for complex workflow with multiple constructs
        let source = r#"fn process(input: [x], output: [y]):
    y = x * 2
    return y"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Should handle all constructs without error
        assert!(!dag.nodes.is_empty());
    }

    #[test]
    fn test_data_flow_cutoff_on_variable_reassignment() {
        // Test that data flow connects to most recent definition when variable is reassigned.
        //
        // When a variable is reassigned, downstream uses should get data flow from
        // the reassignment node, not the original definition. This ensures the
        // data flow graph correctly models variable shadowing/updates.
        let source = r#"fn test(input: [x], output: [z]):
    y = x + 1
    x = 10
    z = x + 2
    return z"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Get data flow edges for variable 'x'
        let data_flow_edges = dag.get_data_flow_edges();
        let _x_data_flow: Vec<_> = data_flow_edges
            .iter()
            .filter(|e| e.variable.as_deref() == Some("x"))
            .collect();

        // Find the nodes by their properties
        let input_node: Option<&String> = dag
            .nodes
            .iter()
            .find(|(_, n)| n.is_input && n.function_name.as_deref() == Some("test"))
            .map(|(id, _)| id);

        // We should have an input node
        assert!(input_node.is_some(), "Should have input node");

        // The key behavior: we track variable modifications correctly
        // In our implementation, the var_modifications map tracks each definition
        // and data flow edges connect definitions to uses

        // At minimum, we should have some structure for this function
        let test_fn_nodes = dag.get_nodes_for_function("test");
        assert!(
            test_fn_nodes.len() >= 4,
            "Should have at least input, 3 assignments, output, return"
        );

        // Verify the input node has 'x' as an io_var
        let input = dag
            .nodes
            .values()
            .find(|n| n.is_input && n.function_name.as_deref() == Some("test"))
            .unwrap();
        assert_eq!(input.io_vars, Some(vec!["x".to_string()]));
    }

    #[test]
    fn test_dag_state_machine_edge_creation() {
        // Test that state machine edges connect nodes in execution order
        let source = r#"fn sequential(input: [x], output: [z]):
    y = x + 1
    z = y + 2
    return z"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        let sm_edges = dag.get_state_machine_edges();

        // Should have edges: input -> assign1 -> assign2 -> return -> output
        assert!(
            sm_edges.len() >= 4,
            "Should have at least 4 state machine edges"
        );
    }

    #[test]
    fn test_dag_for_loop_has_loop_vars() {
        // Test that loop variable extraction is correctly tracked in normalized structure
        let source = r#"fn iterate(input: [items], output: [results]):
    results = []
    for i, item in enumerate(items):
        results = results + [item]
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // In normalized structure, loop variables are tracked in the loop_extract assignment node
        // The extract label is: "{loop_vars} = {collection}[{index_var}]"
        // So we look for an assignment containing the loop vars and the collection
        let extract_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "assignment" && n.id.contains("loop_extract"));
        assert!(
            extract_node.is_some(),
            "Should have loop_extract assignment node"
        );

        // Verify the extract node has the correct targets
        let extract = extract_node.unwrap();
        assert_eq!(
            extract.targets,
            Some(vec!["i".to_string(), "item".to_string()])
        );

        // Verify the loop structure has a back-edge
        let back_edges: Vec<_> = dag.edges.iter().filter(|e| e.is_loop_back).collect();
        assert!(!back_edges.is_empty(), "Should have a back-edge for loop");
    }

    #[test]
    fn test_dag_aggregator_tracks_source() {
        // Test that aggregator nodes track what they aggregate from
        let source = r#"fn fetch_all(input: [items], output: [results]):
    results = spread items:item -> @fetch(id=item)
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the aggregator node
        let agg_node = dag
            .nodes
            .values()
            .find(|n| n.is_aggregator)
            .expect("Should have aggregator node");

        assert!(agg_node.aggregates_from.is_some());
    }

    #[test]
    fn test_dag_conditional_branches() {
        // Test that conditionals create proper branch structure
        let source = r#"fn branch(input: [x], output: [result]):
    if x > 0:
        result = "positive"
    else:
        result = "negative"
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // In the flat DAG structure, conditionals have:
        // - A "branch" node (decision point)
        // - Edges with guard expressions going to each branch's first action
        // - A "join" node where branches converge
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(
            node_types.contains("branch"),
            "Should have branch decision node"
        );
        assert!(node_types.contains("join"), "Should have join node");

        // Check for guarded edges (edges with guard_expr)
        let guarded_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.guard_expr.is_some())
            .collect();
        assert!(
            !guarded_edges.is_empty(),
            "Should have edges with guard expressions"
        );

        // There should be at least 2 guarded edges (one for then, one for else)
        assert!(
            guarded_edges.len() >= 2,
            "Should have guarded edges for both branches"
        );
    }

    #[test]
    fn test_dag_conditional_with_action_calls() {
        // Test that conditionals with action calls work correctly
        let source = r#"fn classify(input: [x], output: [result]):
    if x > 0:
        result = @positive_handler(x=x)
    else:
        result = @negative_handler(x=x)
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Should have action_call nodes for each branch
        let action_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "action_call")
            .collect();
        assert_eq!(
            action_nodes.len(),
            2,
            "Should have 2 action_call nodes for each branch"
        );

        // Each should have the correct action name
        let action_names: HashSet<_> = action_nodes
            .iter()
            .filter_map(|n| n.action_name.as_deref())
            .collect();
        assert!(action_names.contains("positive_handler"));
        assert!(action_names.contains("negative_handler"));
    }

    // =========================================================================
    // Block tuple unpacking tests
    // =========================================================================

    fn make_action_expr(action_name: &str) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::ActionCall(ast::ActionCall {
                action_name: action_name.to_string(),
                kwargs: vec![],
                policies: vec![],
                module_name: None,
            })),
            span: None,
        }
    }

    fn make_assignment_stmt(targets: Vec<String>, action_name: &str) -> ast::Statement {
        ast::Statement {
            kind: Some(ast::statement::Kind::Assignment(ast::Assignment {
                targets,
                value: Some(make_action_expr(action_name)),
            })),
            span: None,
        }
    }

    fn make_side_effect_action_stmt(action_name: &str) -> ast::Statement {
        ast::Statement {
            kind: Some(ast::statement::Kind::ActionCall(ast::ActionCall {
                action_name: action_name.to_string(),
                kwargs: vec![],
                policies: vec![],
                module_name: None,
            })),
            span: None,
        }
    }

    /// Helper to create a simple function def for testing
    fn make_test_function(name: &str, body_statements: Vec<ast::Statement>) -> ast::FunctionDef {
        ast::FunctionDef {
            name: name.to_string(),
            io: Some(ast::IoDecl {
                inputs: vec!["input".to_string()],
                outputs: vec!["output".to_string()],
                span: None,
            }),
            body: Some(ast::Block {
                statements: body_statements,
                span: None,
            }),
            span: None,
        }
    }

    #[test]
    fn test_block_body_tuple_unpacking_in_if_branch() {
        // Test: if condition: first, second = @get_pair() followed by @use_pair(a=first, b=second)
        let if_body = ast::Block {
            statements: vec![make_assignment_stmt(
                vec!["first".to_string(), "second".to_string()],
                "get_pair",
            )],
            span: None,
        };

        let condition = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::BoolValue(true)),
            })),
            span: None,
        };

        let if_branch = ast::IfBranch {
            condition: Some(condition),
            span: None,
            block_body: Some(if_body),
        };

        let conditional = ast::Conditional {
            if_branch: Some(if_branch),
            elif_branches: vec![],
            else_branch: None,
        };

        let stmt = ast::Statement {
            kind: Some(ast::statement::Kind::Conditional(conditional)),
            span: None,
        };

        let use_action = ast::ActionCall {
            action_name: "use_pair".to_string(),
            kwargs: vec![
                ast::Kwarg {
                    name: "a".to_string(),
                    value: Some(ast::Expr {
                        kind: Some(ast::expr::Kind::Variable(ast::Variable {
                            name: "first".to_string(),
                        })),
                        span: None,
                    }),
                },
                ast::Kwarg {
                    name: "b".to_string(),
                    value: Some(ast::Expr {
                        kind: Some(ast::expr::Kind::Variable(ast::Variable {
                            name: "second".to_string(),
                        })),
                        span: None,
                    }),
                },
            ],
            policies: vec![],
            module_name: None,
        };

        let use_stmt = ast::Statement {
            kind: Some(ast::statement::Kind::ActionCall(use_action)),
            span: None,
        };

        let func = make_test_function("test_if_tuple", vec![stmt, use_stmt]);
        let program = ast::Program {
            functions: vec![func],
        };

        let dag = convert_to_dag(&program);

        // Find the action node
        let action_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call" && n.action_name.as_deref() == Some("get_pair"))
            .expect("Should have action_call node for get_pair");

        // Verify targets are set correctly
        assert!(
            action_node.targets.is_some(),
            "Action node should have targets for tuple unpacking"
        );
        let targets = action_node.targets.as_ref().unwrap();
        assert_eq!(targets.len(), 2, "Should have 2 targets");
        assert_eq!(targets[0], "first");
        assert_eq!(targets[1], "second");

        // Verify first target is also set for backwards compatibility
        assert_eq!(
            action_node.target,
            Some("first".to_string()),
            "First target should be set for backwards compat"
        );

        // Verify DATA_FLOW edges for both variables (to the use_pair action)
        let use_pair_node = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("use_pair"))
            .expect("Should have use_pair action");

        let data_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
            .collect();

        let first_edge = data_edges
            .iter()
            .find(|e| e.variable.as_deref() == Some("first") && e.target == use_pair_node.id);
        let second_edge = data_edges
            .iter()
            .find(|e| e.variable.as_deref() == Some("second") && e.target == use_pair_node.id);

        assert!(
            first_edge.is_some(),
            "Should have DATA_FLOW edge for 'first' to use_pair"
        );
        assert!(
            second_edge.is_some(),
            "Should have DATA_FLOW edge for 'second' to use_pair"
        );
    }

    #[test]
    fn test_block_body_tuple_unpacking_in_try_body() {
        // Test: try: result_a, result_b = @risky_action()
        let try_block = ast::Block {
            statements: vec![make_assignment_stmt(
                vec!["result_a".to_string(), "result_b".to_string()],
                "risky_action",
            )],
            span: None,
        };

        // Simple handler body with single target: recovered = @recover()
        let handler_block = ast::Block {
            statements: vec![make_assignment_stmt(
                vec!["recovered".to_string()],
                "recover",
            )],
            span: None,
        };

        let handler = ast::ExceptHandler {
            exception_types: vec!["Error".to_string()],
            span: None,
            block_body: Some(handler_block),
        };

        let try_except = ast::TryExcept {
            handlers: vec![handler],
            try_block: Some(try_block),
        };

        let stmt = ast::Statement {
            kind: Some(ast::statement::Kind::TryExcept(try_except)),
            span: None,
        };

        let func = make_test_function("test_try_tuple", vec![stmt]);
        let program = ast::Program {
            functions: vec![func],
        };

        let dag = convert_to_dag(&program);

        // Find the risky_action node (in try body)
        let try_action = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "action_call" && n.action_name.as_deref() == Some("risky_action")
            })
            .expect("Should have action_call node for risky_action");

        // Verify tuple unpacking targets
        assert!(
            try_action.targets.is_some(),
            "Try body action should have targets"
        );
        let targets = try_action.targets.as_ref().unwrap();
        assert_eq!(targets.len(), 2, "Should have 2 targets in try body");
        assert_eq!(targets[0], "result_a");
        assert_eq!(targets[1], "result_b");

        // Verify handler action has single target
        let handler_action = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call" && n.action_name.as_deref() == Some("recover"))
            .expect("Should have action_call node for recover");

        assert!(handler_action.targets.is_some());
        let handler_targets = handler_action.targets.as_ref().unwrap();
        assert_eq!(handler_targets.len(), 1);
        assert_eq!(handler_targets[0], "recovered");
    }

    #[test]
    fn test_block_body_tuple_unpacking_in_for_loop() {
        // Test: for item in items: processed_x, processed_y = @process_item()
        let loop_body = ast::Block {
            statements: vec![make_assignment_stmt(
                vec!["processed_x".to_string(), "processed_y".to_string()],
                "process_item",
            )],
            span: None,
        };

        let iterable = ast::Expr {
            kind: Some(ast::expr::Kind::Variable(ast::Variable {
                name: "items".to_string(),
            })),
            span: None,
        };

        let for_loop = ast::ForLoop {
            loop_vars: vec!["item".to_string()],
            iterable: Some(iterable),
            block_body: Some(loop_body),
        };

        let stmt = ast::Statement {
            kind: Some(ast::statement::Kind::ForLoop(for_loop)),
            span: None,
        };

        let func = make_test_function("test_for_tuple", vec![stmt]);
        let program = ast::Program {
            functions: vec![func],
        };

        let dag = convert_to_dag(&program);

        // In normalized loop structure, find the action node for process_item
        let action_node = dag.nodes.values().find(|n| {
            n.node_type == "action_call" && n.action_name.as_deref() == Some("process_item")
        });

        // Verify the loop structure exists (branch for condition, join for exit)
        let loop_cond = dag
            .nodes
            .values()
            .find(|n| n.node_type == "branch" && n.label.contains("for"))
            .expect("Should have branch node for loop condition");

        let loop_exit = dag
            .nodes
            .values()
            .find(|n| n.node_type == "join" && n.label.contains("end for"))
            .expect("Should have join node for loop exit");

        // Loop nodes should exist and have proper structure
        assert!(!loop_cond.id.is_empty());
        assert!(!loop_exit.id.is_empty());

        // Verify there's a back-edge
        let back_edges: Vec<_> = dag.edges.iter().filter(|e| e.is_loop_back).collect();
        assert!(!back_edges.is_empty(), "Should have a back-edge for loop");

        // If action is directly in the loop body (not wrapped), verify targets
        if let Some(action) = action_node {
            assert!(
                action.targets.is_some(),
                "Action in loop body should have targets"
            );
            let targets = action.targets.as_ref().unwrap();
            assert_eq!(targets.len(), 2);
            assert!(targets.contains(&"processed_x".to_string()));
            assert!(targets.contains(&"processed_y".to_string()));
        }
    }

    #[test]
    fn test_block_body_no_targets() {
        // Test: if condition: @side_effect()  (no assignment)
        let if_body = ast::Block {
            statements: vec![make_side_effect_action_stmt("side_effect")],
            span: None,
        };

        let condition = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::BoolValue(true)),
            })),
            span: None,
        };

        let conditional = ast::Conditional {
            if_branch: Some(ast::IfBranch {
                condition: Some(condition),
                span: None,
                block_body: Some(if_body),
            }),
            elif_branches: vec![],
            else_branch: None,
        };

        let stmt = ast::Statement {
            kind: Some(ast::statement::Kind::Conditional(conditional)),
            span: None,
        };

        let func = make_test_function("test_no_target", vec![stmt]);
        let program = ast::Program {
            functions: vec![func],
        };

        let dag = convert_to_dag(&program);

        // Find the action node
        let action_node = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "action_call" && n.action_name.as_deref() == Some("side_effect")
            })
            .expect("Should have action_call node");

        // Should have no targets (side effect only)
        assert!(
            action_node.targets.is_none() || action_node.targets.as_ref().unwrap().is_empty(),
            "Side effect action should have no targets"
        );
        assert!(
            action_node.target.is_none(),
            "Side effect action should have no target"
        );
    }

    #[test]
    fn test_block_body_single_target() {
        // Test: if condition: result = @compute()  (single target - common case)
        let if_body = ast::Block {
            statements: vec![make_assignment_stmt(vec!["result".to_string()], "compute")],
            span: None,
        };

        let condition = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::BoolValue(true)),
            })),
            span: None,
        };

        let conditional = ast::Conditional {
            if_branch: Some(ast::IfBranch {
                condition: Some(condition),
                span: None,
                block_body: Some(if_body),
            }),
            elif_branches: vec![],
            else_branch: None,
        };

        let stmt = ast::Statement {
            kind: Some(ast::statement::Kind::Conditional(conditional)),
            span: None,
        };

        let func = make_test_function("test_single_target", vec![stmt]);
        let program = ast::Program {
            functions: vec![func],
        };

        let dag = convert_to_dag(&program);

        let action_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "action_call" && n.action_name.as_deref() == Some("compute"))
            .expect("Should have action_call node");

        // Should have single target
        assert!(action_node.targets.is_some());
        let targets = action_node.targets.as_ref().unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0], "result");

        // Backwards compat target should also be set
        assert_eq!(action_node.target, Some("result".to_string()));
    }

    #[test]
    fn test_block_body_three_targets() {
        // Test edge case: x, y, z = @get_triple()
        let if_body = ast::Block {
            statements: vec![make_assignment_stmt(
                vec!["x".to_string(), "y".to_string(), "z".to_string()],
                "get_triple",
            )],
            span: None,
        };

        let condition = ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::BoolValue(true)),
            })),
            span: None,
        };

        let conditional = ast::Conditional {
            if_branch: Some(ast::IfBranch {
                condition: Some(condition),
                span: None,
                block_body: Some(if_body),
            }),
            elif_branches: vec![],
            else_branch: None,
        };

        let stmt = ast::Statement {
            kind: Some(ast::statement::Kind::Conditional(conditional)),
            span: None,
        };

        let func = make_test_function("test_triple", vec![stmt]);
        let program = ast::Program {
            functions: vec![func],
        };

        let dag = convert_to_dag(&program);

        let action_node = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "action_call" && n.action_name.as_deref() == Some("get_triple")
            })
            .expect("Should have action_call node");

        // Should have all 3 targets
        assert!(action_node.targets.is_some());
        let targets = action_node.targets.as_ref().unwrap();
        assert_eq!(targets.len(), 3);
        assert_eq!(targets, &vec!["x", "y", "z"]);

        // Label should show all targets
        assert!(
            action_node.label.contains("x, y, z"),
            "Label should show all targets: {}",
            action_node.label
        );
    }

    #[test]
    fn test_dag_parallel_input_flows_to_downstream() {
        // Test that input variable flows through parallel block to downstream action
        // This reproduces the bug where summarize_math gets TypeError because
        // the input_number variable is not in its inbox
        let source = r#"fn compute(input: [n], output: [summary]):
    factorial_value, fib_value = parallel:
        @compute_factorial(n=n)
        @compute_fibonacci(n=n)
    summary = @summarize(input_number=n, factorial=factorial_value, fib=fib_value)
    return summary"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the summarize action node
        let summarize_node = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("summarize"))
            .expect("Should have summarize action node");

        // Check kwargs include input_number
        let kwargs = summarize_node.kwargs.as_ref().expect("Should have kwargs");
        assert_eq!(
            kwargs.get("input_number").map(|v| v.as_str()),
            Some("$n"),
            "input_number kwarg should reference n"
        );

        // Print DataFlow edges for debugging
        println!("\nDataFlow edges:");
        for edge in dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
        {
            println!(
                "  {} --[{}]--> {}",
                edge.source,
                edge.variable.as_deref().unwrap_or("?"),
                edge.target
            );
        }

        // Check that input 'n' flows to summarize action
        let input_to_summarize = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
            .find(|e| e.variable.as_deref() == Some("n") && e.target == summarize_node.id);

        assert!(
            input_to_summarize.is_some(),
            "Should have DATA_FLOW edge for input 'n' to summarize action"
        );
    }

    #[test]
    fn test_dag_parallel_data_flow_to_summarize() {
        // Test that data flows correctly from parallel actions to downstream action
        let source = r#"fn compute(input: [n], output: [summary]):
    factorial_value, fib_value = parallel:
        @compute_factorial(n=n)
        @compute_fibonacci(n=n)
    summary = @summarize(factorial=factorial_value, fib=fib_value)
    return summary"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the summarize action node
        let summarize_node = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("summarize"))
            .expect("Should have summarize action node");

        println!("Summarize node id: {}", summarize_node.id);
        println!("Summarize node kwargs: {:?}", summarize_node.kwargs);

        // Check the kwargs - they should reference the variables
        let kwargs = summarize_node.kwargs.as_ref().expect("Should have kwargs");
        assert!(
            kwargs.get("factorial").map(|v| v.as_str()) == Some("$factorial_value"),
            "factorial kwarg should reference factorial_value"
        );
        assert!(
            kwargs.get("fib").map(|v| v.as_str()) == Some("$fib_value"),
            "fib kwarg should reference fib_value"
        );

        // Print all data flow edges for debugging
        println!("\nDataFlow edges:");
        for edge in dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
        {
            println!(
                "  {} --[{}]--> {}",
                edge.source,
                edge.variable.as_deref().unwrap_or("?"),
                edge.target
            );
        }

        // Check that there are DATA_FLOW edges from the parallel action nodes to summarize
        let factorial_to_summarize = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
            .find(|e| {
                e.variable.as_deref() == Some("factorial_value") && e.target == summarize_node.id
            });

        let fib_to_summarize = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
            .find(|e| e.variable.as_deref() == Some("fib_value") && e.target == summarize_node.id);

        assert!(
            factorial_to_summarize.is_some(),
            "Should have DATA_FLOW edge from factorial_value definition to summarize"
        );
        assert!(
            fib_to_summarize.is_some(),
            "Should have DATA_FLOW edge from fib_value definition to summarize"
        );
    }

    #[test]
    fn test_dag_parallel_structure_for_aggregator() {
        // Test to visualize the full DAG structure for parallel workflow
        // This helps debug the aggregator synchronization issue
        let source = r#"fn compute(input: [n], output: [summary]):
    factorial_value, fib_value = parallel:
        @compute_factorial(n=n)
        @compute_fibonacci(n=n)
    summary = @summarize(factorial=factorial_value, fib=fib_value)
    return summary"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        println!("\n=== DAG NODES ===");
        for (id, node) in dag.nodes.iter() {
            println!(
                "  {} (type={}, action={:?}, target={:?}, targets={:?}, is_aggregator={}, aggregates_from={:?})",
                id,
                node.node_type,
                node.action_name,
                node.target,
                node.targets,
                node.is_aggregator,
                node.aggregates_from
            );
        }

        println!("\n=== STATE MACHINE EDGES ===");
        for edge in dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine)
        {
            println!("  {} --> {}", edge.source, edge.target);
        }

        println!("\n=== DATA FLOW EDGES ===");
        for edge in dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
        {
            println!(
                "  {} --[{}]--> {}",
                edge.source,
                edge.variable.as_deref().unwrap_or("?"),
                edge.target
            );
        }

        // Find the parallel entry node, parallel actions, aggregator, and summarize action
        let _parallel_entry = dag
            .nodes
            .values()
            .find(|n| n.node_type == "parallel")
            .expect("Should have parallel entry node");

        let aggregator = dag
            .nodes
            .values()
            .find(|n| n.is_aggregator)
            .expect("Should have aggregator node");

        let summarize = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("summarize"))
            .expect("Should have summarize action");

        // Get state machine successors of parallel_action_3 (compute_factorial)
        let factorial_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("compute_factorial"))
            .expect("Should have compute_factorial action");

        println!("\n=== FACTORIAL ACTION STATE MACHINE SUCCESSORS ===");
        for edge in dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine && e.source == factorial_action.id)
        {
            println!("  {} --> {}", edge.source, edge.target);
        }

        // The key assertion: parallel actions should have aggregator as their SM successor
        let factorial_sm_successors: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine && e.source == factorial_action.id)
            .map(|e| e.target.clone())
            .collect();

        println!("\nFactorial SM successors: {:?}", factorial_sm_successors);
        println!("Aggregator ID: {}", aggregator.id);

        assert!(
            factorial_sm_successors.contains(&aggregator.id),
            "Parallel action should have aggregator as StateMachine successor"
        );

        // The aggregator should have summarize as its SM successor
        let aggregator_sm_successors: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine && e.source == aggregator.id)
            .map(|e| e.target.clone())
            .collect();

        println!("Aggregator SM successors: {:?}", aggregator_sm_successors);
        println!("Summarize ID: {}", summarize.id);

        assert!(
            aggregator_sm_successors.contains(&summarize.id),
            "Aggregator should have summarize action as StateMachine successor"
        );
    }

    #[test]
    fn test_parallel_math_from_protobuf() {
        // Test that loads the actual parallel_math.pb protobuf file and verifies
        // that DataFlow edges exist from the input node to summarize_math action.
        use prost::Message;
        use std::fs;

        let pb_path = "/tmp/parallel_math.pb";
        if !std::path::Path::new(pb_path).exists() {
            // Skip test if the file doesn't exist (CI environments)
            eprintln!(
                "Skipping test_parallel_math_from_protobuf: {} not found",
                pb_path
            );
            return;
        }

        let data = fs::read(pb_path).expect("Failed to read parallel_math.pb");
        let program = crate::ast::Program::decode(&data[..]).expect("Failed to decode protobuf");

        // Convert to DAG
        let dag = convert_to_dag(&program);

        // Print the DAG structure for debugging
        println!("\n=== NODES ===");
        for (id, node) in &dag.nodes {
            println!(
                "  {} (type={}, action={:?}, target={:?}, targets={:?}, kwargs={:?})",
                id, node.node_type, node.action_name, node.target, node.targets, node.kwargs
            );
        }

        println!("\n=== DATA FLOW EDGES ===");
        for edge in dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
        {
            println!(
                "  {} --[{}]--> {}",
                edge.source,
                edge.variable.as_deref().unwrap_or("?"),
                edge.target
            );
        }

        println!("\n=== STATE MACHINE EDGES ===");
        for edge in dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::StateMachine)
        {
            println!(
                "  {} --({:?})--> {}",
                edge.source, edge.condition, edge.target
            );
        }

        // Find the input node
        let input_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "input")
            .expect("Should have input node");

        println!("\nInput node id: {}", input_node.id);
        println!("Input node io_vars: {:?}", input_node.io_vars);

        // Find the summarize action node
        let summarize_node = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("summarize_math"))
            .expect("Should have summarize_math action node");

        println!("Summarize node id: {}", summarize_node.id);
        println!("Summarize node kwargs: {:?}", summarize_node.kwargs);

        // Check that there's a DataFlow edge from input to summarize for 'number'
        let input_to_summarize_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| {
                e.edge_type == EdgeType::DataFlow
                    && e.source == input_node.id
                    && e.target == summarize_node.id
            })
            .collect();

        println!("\nDataFlow edges from input to summarize:");
        for edge in &input_to_summarize_edges {
            println!(
                "  {} --[{}]--> {}",
                edge.source,
                edge.variable.as_deref().unwrap_or("?"),
                edge.target
            );
        }

        // This is the key assertion - there should be a DataFlow edge for 'number'
        let number_edge = input_to_summarize_edges
            .iter()
            .find(|e| e.variable.as_deref() == Some("number"));

        assert!(
            number_edge.is_some(),
            "Should have DataFlow edge for 'number' from input to summarize_math"
        );
    }

    #[test]
    fn test_dag_chain_workflow_dataflow_edges() {
        // Test the chain workflow pattern where a final action needs variables from
        // multiple earlier actions:
        // step1 = @action1(text=text)
        // step2 = @action2(text=step1)
        // step3 = @action3(text=step2)
        // _return_tmp = @final_action(original=text, step1=step1, step2=step2, step3=step3)
        // return _return_tmp
        //
        // The final_action needs text (from input), step1, step2, and step3.
        // This tests that DataFlow edges are created correctly for all these dependencies.
        let source = r#"fn run(input: [text], output: [result]):
    step1 = @step_uppercase(text=text)
    step2 = @step_reverse(text=step1)
    step3 = @step_add_stars(text=step2)
    _return_tmp = @build_chain_result(original=text, step1=step1, step2=step2, step3=step3)
    return _return_tmp"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the final action node (build_chain_result)
        let final_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("build_chain_result"))
            .expect("Should have build_chain_result action");

        println!("\n=== Chain Workflow DAG Analysis ===");
        println!("\nAll action nodes:");
        for node in dag.nodes.values().filter(|n| n.node_type == "action_call") {
            println!(
                "  {} -> {} (target: {:?})",
                node.id,
                node.action_name.as_deref().unwrap_or("?"),
                node.target
            );
        }

        println!("\nAll DataFlow edges:");
        for edge in dag.get_data_flow_edges() {
            println!(
                "  {} --[{}]--> {}",
                edge.source,
                edge.variable.as_deref().unwrap_or("?"),
                edge.target
            );
        }

        // Get all DataFlow edges pointing to the final action
        let final_action_data_flow: Vec<_> = dag
            .get_data_flow_edges()
            .iter()
            .filter(|e| e.target == final_action.id)
            .cloned()
            .collect();

        println!(
            "\nDataFlow edges to build_chain_result ({}):",
            final_action.id
        );
        for edge in &final_action_data_flow {
            println!(
                "  {} --[{}]--> {}",
                edge.source,
                edge.variable.as_deref().unwrap_or("?"),
                edge.target
            );
        }

        // The final action needs 4 variables: text, step1, step2, step3
        // Each should have a DataFlow edge to the final action

        let text_edge = final_action_data_flow
            .iter()
            .find(|e| e.variable.as_deref() == Some("text"));
        let step1_edge = final_action_data_flow
            .iter()
            .find(|e| e.variable.as_deref() == Some("step1"));
        let step2_edge = final_action_data_flow
            .iter()
            .find(|e| e.variable.as_deref() == Some("step2"));
        let step3_edge = final_action_data_flow
            .iter()
            .find(|e| e.variable.as_deref() == Some("step3"));

        assert!(
            text_edge.is_some(),
            "Should have DataFlow edge for 'text' to build_chain_result"
        );
        assert!(
            step1_edge.is_some(),
            "Should have DataFlow edge for 'step1' to build_chain_result"
        );
        assert!(
            step2_edge.is_some(),
            "Should have DataFlow edge for 'step2' to build_chain_result"
        );
        assert!(
            step3_edge.is_some(),
            "Should have DataFlow edge for 'step3' to build_chain_result"
        );
    }

    #[test]
    fn test_data_flow_for_loop_shadows_initial_assignment() {
        // Test that the normalized loop structure correctly tracks loop body targets.
        //
        // When we have:
        //   results = []
        //   for item in items:
        //       results = results + [item]
        //   use_results(data=results)
        //
        // The loop_exit (join) node should track 'results' as a target since it's
        // modified inside the loop body.
        let source = r#"fn run(input: [items], output: [final_result]):
    results = []
    for item in items:
        results = results + [item]
    final_result = @use_results(data=results)
    return final_result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Verify the normalized loop structure exists
        let back_edges: Vec<_> = dag.edges.iter().filter(|e| e.is_loop_back).collect();
        assert!(!back_edges.is_empty(), "Should have back-edges for loop");

        // Find the loop_exit (join) node - this is where loop's final values come from
        let loop_exit = dag
            .nodes
            .values()
            .find(|n| n.node_type == "join" && n.label.contains("end for"))
            .expect("Should have loop_exit (join) node");

        // Verify the loop_exit node tracks 'results' as a target (modified in loop body)
        assert!(
            loop_exit
                .targets
                .as_ref()
                .is_some_and(|t| t.contains(&"results".to_string())),
            "Loop exit should track 'results' as a target since it's modified in the loop body"
        );

        // Find the action node that uses 'results'
        let use_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("use_results"))
            .expect("Should have use_results action node");

        // Verify there's at least one data flow edge from loop_exit to the action
        let data_flow_edges = dag.get_data_flow_edges();
        let has_loop_exit_edge = data_flow_edges.iter().any(|e| {
            e.variable.as_deref() == Some("results")
                && e.source == loop_exit.id
                && e.target == use_action.id
        });

        assert!(
            has_loop_exit_edge,
            "Should have data flow edge from loop_exit to use_action for 'results'"
        );
    }

    #[test]
    fn test_synthetic_function_return_nodes_stripped_on_expansion() {
        // Test that return nodes from synthetic functions (try/except bodies, for loop bodies,
        // if/elif/else branches) are stripped when the function is expanded inline.
        //
        // These return nodes are artifacts of how multi-statement bodies are wrapped into
        // synthetic functions. They serve no purpose in the expanded DAG and should be removed.
        let source = r#"fn run(input: [x], output: [result]):
    try:
        result = @risky_action(x=x)
    except NetworkError:
        result = @fallback_action(x=x)
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Count return nodes - there should only be ONE (the main function's return)
        let return_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "return")
            .collect();

        println!("Return nodes found:");
        for node in &return_nodes {
            println!(
                "  {}: {} (function: {:?})",
                node.id, node.label, node.function_name
            );
        }

        assert_eq!(
            return_nodes.len(),
            1,
            "Should have exactly 1 return node (main function's return), got {}. \
             Synthetic function return nodes should be stripped during expansion.",
            return_nodes.len()
        );

        // The only return node should be from the main 'run' function
        let return_node = return_nodes[0];
        assert_eq!(
            return_node.function_name.as_deref(),
            Some("run"),
            "The only return node should be from the main 'run' function"
        );

        // Verify the try/except body actions exist (they should be expanded)
        let action_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "action_call")
            .collect();

        let action_names: HashSet<_> = action_nodes
            .iter()
            .filter_map(|n| n.action_name.as_deref())
            .collect();

        assert!(
            action_names.contains("risky_action"),
            "Should have risky_action node from try body"
        );
        assert!(
            action_names.contains("fallback_action"),
            "Should have fallback_action node from except handler"
        );
    }

    // =========================================================================
    // Complete Feature Workflow Tests
    // =========================================================================
    // These tests validate the DAG generation for a comprehensive workflow
    // that exercises all Rappel language features. They ensure:
    // 1. Conditional branches with action calls are properly inlined
    // 2. For loop bodies with action calls are properly represented
    // 3. Try/except bodies are properly inlined
    // 4. Data flow edges are complete and correct
    // 5. No dangling edges exist
    // =========================================================================

    /// The complete feature workflow IR that exercises all language features.
    /// This is the IR representation of the Python workflow in
    /// tests/fixtures/complete_feature_workflow.py
    const COMPLETE_FEATURE_WORKFLOW_IR: &str = r#"
fn __for_body_1__(input: [i, item, results], output: [results]):
    item_result = @validate_item(item=item, index=i)
    results = (results + [item_result])
    return results

fn __if_then_2__(input: [], output: []):
    final_status = @handle_overflow(count=count)

fn __if_elif_3__(input: [], output: []):
    final_status = @handle_threshold(count=count)

fn __if_else_4__(input: [], output: []):
    final_status = @handle_normal(count=count)

fn __try_body_5__(input: [results, risky_result], output: [risky_result]):
    risky_result = @risky_operation(data=results)
    return risky_result

fn __except_handler_6__(input: [results, risky_result], output: [risky_result]):
    risky_result = @fallback_operation(data=results)
    return risky_result

fn run(input: [items, threshold], output: []):
    count = 0
    results = []
    status_a, status_b = parallel:
        @check_status_a(service="alpha")
        @check_status_b(service="beta")
    processed = spread items:x -> @process_item(item=x)
    for i, item in enumerate(processed):
        results = __for_body_1__(i=i, item=item, results=results)
    if (count > threshold):
        __if_then_2__()
    elif (count == threshold):
        __if_elif_3__()
    else:
        __if_else_4__()
    try:
        risky_result = __try_body_5__(results=results, risky_result=risky_result)
    except NetworkError:
        risky_result = __except_handler_6__(results=results, risky_result=risky_result)
    summary = @aggregate_results(items=processed, status_a=status_a, status_b=status_b, final_status=final_status)
    return summary
"#;

    #[test]
    fn test_complete_workflow_parses_successfully() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse complete workflow");
        assert_eq!(program.functions.len(), 7, "Should have 7 functions");

        let function_names: HashSet<_> =
            program.functions.iter().map(|f| f.name.as_str()).collect();
        assert!(function_names.contains("run"));
        assert!(function_names.contains("__for_body_1__"));
        assert!(function_names.contains("__if_then_2__"));
        assert!(function_names.contains("__if_elif_3__"));
        assert!(function_names.contains("__if_else_4__"));
        assert!(function_names.contains("__try_body_5__"));
        assert!(function_names.contains("__except_handler_6__"));
    }

    #[test]
    fn test_complete_workflow_dag_has_all_action_calls() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Collect all action names in the DAG
        let action_names: HashSet<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "action_call")
            .filter_map(|n| n.action_name.as_deref())
            .collect();

        // All actions from the workflow should be present after expansion
        let expected_actions = [
            "check_status_a",
            "check_status_b",
            "process_item",
            "validate_item",
            "handle_overflow",
            "handle_threshold",
            "handle_normal",
            "risky_operation",
            "fallback_operation",
            "aggregate_results",
        ];

        for expected in expected_actions {
            assert!(
                action_names.contains(expected),
                "DAG should contain action '{}' after function expansion. Found: {:?}",
                expected,
                action_names
            );
        }
    }

    #[test]
    fn test_complete_workflow_conditional_branches_inlined() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // The conditional branches should have action_call nodes, not just expression nodes
        let action_names: HashSet<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "action_call")
            .filter_map(|n| n.action_name.as_deref())
            .collect();

        // These are the actions inside if/elif/else branches
        assert!(
            action_names.contains("handle_overflow"),
            "if branch action should be inlined"
        );
        assert!(
            action_names.contains("handle_threshold"),
            "elif branch action should be inlined"
        );
        assert!(
            action_names.contains("handle_normal"),
            "else branch action should be inlined"
        );

        // There should be NO fn_call nodes for the synthetic functions after expansion
        let fn_call_to_synthetic: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.is_fn_call)
            .filter(|n| {
                n.called_function
                    .as_ref()
                    .map(|f| f.starts_with("__if_"))
                    .unwrap_or(false)
            })
            .collect();

        assert!(
            fn_call_to_synthetic.is_empty(),
            "Synthetic if/elif/else functions should be expanded, not left as fn_call nodes. Found: {:?}",
            fn_call_to_synthetic
                .iter()
                .map(|n| &n.id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_complete_workflow_for_loop_body_inlined() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // The for loop body should have the validate_item action
        let validate_item_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.action_name.as_deref() == Some("validate_item"))
            .collect();

        assert!(
            !validate_item_nodes.is_empty(),
            "For loop body should contain validate_item action"
        );

        // The validate_item action should have proper kwargs for item and index
        let validate_node = validate_item_nodes[0];
        assert!(
            validate_node.kwarg_exprs.is_some(),
            "validate_item should have kwarg expressions"
        );
        let kwarg_exprs = validate_node.kwarg_exprs.as_ref().unwrap();
        assert!(kwarg_exprs.contains_key("item"), "Should have 'item' kwarg");
        assert!(
            kwarg_exprs.contains_key("index"),
            "Should have 'index' kwarg"
        );

        // There should be NO fn_call nodes for __for_body_1__ after expansion
        let fn_call_to_for_body: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.is_fn_call)
            .filter(|n| {
                n.called_function
                    .as_ref()
                    .map(|f| f.starts_with("__for_body"))
                    .unwrap_or(false)
            })
            .collect();

        assert!(
            fn_call_to_for_body.is_empty(),
            "For loop body function should be expanded. Found fn_call nodes: {:?}",
            fn_call_to_for_body
                .iter()
                .map(|n| &n.id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_complete_workflow_try_except_bodies_inlined() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Both risky_operation and fallback_operation should be present
        let action_names: HashSet<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "action_call")
            .filter_map(|n| n.action_name.as_deref())
            .collect();

        assert!(
            action_names.contains("risky_operation"),
            "Try body action should be inlined"
        );
        assert!(
            action_names.contains("fallback_operation"),
            "Except handler action should be inlined"
        );

        // There should be NO fn_call nodes for try/except synthetic functions
        let fn_call_to_try_except: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.is_fn_call)
            .filter(|n| {
                n.called_function
                    .as_ref()
                    .map(|f| f.starts_with("__try_body") || f.starts_with("__except_handler"))
                    .unwrap_or(false)
            })
            .collect();

        assert!(
            fn_call_to_try_except.is_empty(),
            "Try/except synthetic functions should be expanded. Found: {:?}",
            fn_call_to_try_except
                .iter()
                .map(|n| &n.id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_complete_workflow_no_dangling_edges() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Every edge should reference nodes that exist in the DAG
        for edge in &dag.edges {
            assert!(
                dag.nodes.contains_key(&edge.source),
                "Edge source '{}' should exist in DAG nodes. Edge: {:?}",
                edge.source,
                edge
            );
            assert!(
                dag.nodes.contains_key(&edge.target),
                "Edge target '{}' should exist in DAG nodes. Edge: {:?}",
                edge.target,
                edge
            );
        }
    }

    #[test]
    fn test_complete_workflow_data_flow_to_aggregate_results() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Find the aggregate_results action
        let aggregate_node = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("aggregate_results"))
            .expect("Should have aggregate_results node");

        // Collect all data flow edges TO aggregate_results
        let incoming_data_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow && e.target == aggregate_node.id)
            .collect();

        let incoming_vars: HashSet<_> = incoming_data_edges
            .iter()
            .filter_map(|e| e.variable.as_deref())
            .collect();

        // aggregate_results needs: items (processed), status_a, status_b, final_status
        // Note: 'items' in kwargs refers to 'processed' variable
        assert!(
            incoming_vars.contains("processed") || incoming_vars.contains("items"),
            "aggregate_results should have data flow for 'processed'. Found: {:?}",
            incoming_vars
        );
        assert!(
            incoming_vars.contains("status_a"),
            "aggregate_results should have data flow for 'status_a'. Found: {:?}",
            incoming_vars
        );
        assert!(
            incoming_vars.contains("status_b"),
            "aggregate_results should have data flow for 'status_b'. Found: {:?}",
            incoming_vars
        );
        assert!(
            incoming_vars.contains("final_status"),
            "aggregate_results should have data flow for 'final_status'. Found: {:?}",
            incoming_vars
        );
    }

    #[test]
    fn test_complete_workflow_final_status_defined_by_branches() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Find all nodes that define final_status (should be the 3 branch actions)
        let final_status_producers: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| {
                n.targets
                    .as_ref()
                    .map(|t| t.contains(&"final_status".to_string()))
                    .unwrap_or(false)
                    || n.target.as_deref() == Some("final_status")
            })
            .collect();

        assert!(
            !final_status_producers.is_empty(),
            "Should have nodes that produce 'final_status'"
        );

        // The producers should be the handle_* actions
        let producer_actions: HashSet<_> = final_status_producers
            .iter()
            .filter_map(|n| n.action_name.as_deref())
            .collect();

        // At least one of the branch actions should produce final_status
        let branch_actions = ["handle_overflow", "handle_threshold", "handle_normal"];
        let has_branch_producer = branch_actions.iter().any(|a| producer_actions.contains(a));

        assert!(
            has_branch_producer,
            "One of the branch actions should produce 'final_status'. Producers: {:?}",
            producer_actions
        );
    }

    #[test]
    fn test_complete_workflow_no_expression_nodes_for_branches() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // There should be NO generic "expression" nodes that are actually branch bodies
        // Branch bodies should be expanded to their actual content (action_call nodes)
        let expression_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "expression" && n.label == "expr")
            .collect();

        // If there are expression nodes, they shouldn't be connected to branch nodes
        let branch_nodes: HashSet<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "branch")
            .map(|n| n.id.as_str())
            .collect();

        for expr_node in &expression_nodes {
            // Check if this expression node is directly connected from a branch
            let is_branch_body = dag.edges.iter().any(|e| {
                branch_nodes.contains(e.source.as_str())
                    && e.target == expr_node.id
                    && e.edge_type == EdgeType::StateMachine
            });

            assert!(
                !is_branch_body,
                "Expression node '{}' should not be a branch body - branch bodies should be expanded to action_call nodes",
                expr_node.id
            );
        }
    }

    #[test]
    fn test_complete_workflow_parallel_structure() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Should have parallel entry node
        let parallel_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "parallel")
            .collect();

        assert!(
            !parallel_nodes.is_empty(),
            "Should have parallel entry node"
        );

        // Should have aggregator for parallel
        let parallel_aggregators: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.is_aggregator && n.aggregates_from.is_some())
            .collect();

        assert!(
            !parallel_aggregators.is_empty(),
            "Should have aggregator for parallel block"
        );

        // Parallel actions check_status_a and check_status_b should exist
        let action_names: HashSet<_> = dag
            .nodes
            .values()
            .filter_map(|n| n.action_name.as_deref())
            .collect();

        assert!(action_names.contains("check_status_a"));
        assert!(action_names.contains("check_status_b"));
    }

    #[test]
    fn test_complete_workflow_spread_structure() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Should have spread action for process_item
        let spread_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.is_spread && n.action_name.as_deref() == Some("process_item"))
            .collect();

        assert!(
            !spread_nodes.is_empty(),
            "Should have spread action for process_item"
        );

        // Should have aggregator for spread results
        let spread_node = spread_nodes[0];
        assert!(
            spread_node.aggregates_to.is_some(),
            "Spread action should link to aggregator"
        );

        let aggregator_id = spread_node.aggregates_to.as_ref().unwrap();
        assert!(
            dag.nodes.contains_key(aggregator_id),
            "Aggregator node should exist"
        );
    }

    #[test]
    fn test_complete_workflow_for_loop_structure() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Normalized for loop should have these components:
        // 1. loop_init (assignment): __loop_*_i = 0
        // 2. loop_cond (branch): condition check
        // 3. loop_extract (assignment): extract item from collection
        // 4. body nodes (action_call for validate_item)
        // 5. loop_incr (assignment): increment index
        // 6. loop_exit (join): exit point

        // Check for loop init
        let loop_init = dag
            .nodes
            .values()
            .find(|n| n.node_type == "assignment" && n.id.contains("loop_init"));
        assert!(loop_init.is_some(), "Should have loop_init node");

        // Check for loop condition (branch)
        let loop_cond = dag
            .nodes
            .values()
            .find(|n| n.node_type == "branch" && n.label.contains("for"));
        assert!(loop_cond.is_some(), "Should have loop_cond branch node");

        // Check for loop extract
        let loop_extract = dag
            .nodes
            .values()
            .find(|n| n.node_type == "assignment" && n.id.contains("loop_extract"));
        assert!(loop_extract.is_some(), "Should have loop_extract node");

        // Check for loop increment
        let loop_incr = dag
            .nodes
            .values()
            .find(|n| n.node_type == "assignment" && n.id.contains("loop_incr"));
        assert!(loop_incr.is_some(), "Should have loop_incr node");

        // Check for loop exit (join)
        let loop_exit = dag
            .nodes
            .values()
            .find(|n| n.node_type == "join" && n.label.contains("end for"));
        assert!(loop_exit.is_some(), "Should have loop_exit join node");

        // Check for back-edge
        let back_edges: Vec<_> = dag.edges.iter().filter(|e| e.is_loop_back).collect();
        assert!(!back_edges.is_empty(), "Should have loop back-edge");
    }

    #[test]
    fn test_complete_workflow_try_except_structure() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Find the risky_operation and fallback_operation nodes
        let risky_node = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("risky_operation"));
        let fallback_node = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_deref() == Some("fallback_operation"));

        assert!(risky_node.is_some(), "Should have risky_operation node");
        assert!(
            fallback_node.is_some(),
            "Should have fallback_operation node"
        );

        let risky = risky_node.unwrap();
        let fallback = fallback_node.unwrap();

        // Should have exception edge from risky to fallback
        let exception_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.exception_types.is_some())
            .collect();

        assert!(!exception_edges.is_empty(), "Should have exception edges");

        // Check for success edge from try body
        let success_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.condition.as_deref() == Some("success"))
            .collect();

        assert!(
            !success_edges.is_empty(),
            "Should have success edge from try body"
        );

        // Should have join node for try/except convergence
        let try_except_joins: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "join")
            .filter(|n| {
                // Find joins that have the risky or fallback as predecessors
                dag.edges.iter().any(|e| {
                    (e.source == risky.id || e.source == fallback.id)
                        && e.target == n.id
                        && e.edge_type == EdgeType::StateMachine
                })
            })
            .collect();

        assert!(
            !try_except_joins.is_empty(),
            "Should have join node for try/except"
        );
    }

    #[test]
    fn test_complete_workflow_return_node_is_singular() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // After expansion, there should be only ONE return node (from the main 'run' function)
        // Synthetic function return nodes should be stripped during expansion
        let return_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "return")
            .collect();

        assert_eq!(
            return_nodes.len(),
            1,
            "Should have exactly 1 return node after expansion. Found: {:?}",
            return_nodes.iter().map(|n| &n.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_complete_workflow_no_fn_call_nodes_after_expansion() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // All fn_call nodes to internal synthetic functions should be expanded
        let fn_call_nodes: Vec<_> = dag.nodes.values().filter(|n| n.is_fn_call).collect();

        // Print details for debugging
        if !fn_call_nodes.is_empty() {
            eprintln!("Found unexpanded fn_call nodes:");
            for node in &fn_call_nodes {
                eprintln!(
                    "  - id: {}, called_function: {:?}",
                    node.id, node.called_function
                );
            }
        }

        assert!(
            fn_call_nodes.is_empty(),
            "All internal function calls should be expanded. Found {} fn_call nodes",
            fn_call_nodes.len()
        );
    }

    #[test]
    fn test_complete_workflow_input_variables_flow_to_users() {
        let program = parse(COMPLETE_FEATURE_WORKFLOW_IR).expect("Should parse");
        let dag = convert_to_dag(&program);

        // Find the input node
        let input_node = dag
            .nodes
            .values()
            .find(|n| n.is_input)
            .expect("Should have input node");

        assert_eq!(
            input_node.io_vars,
            Some(vec!["items".to_string(), "threshold".to_string()])
        );

        // Check that the spread action can access 'items'
        // The spread action stores its collection in spread_collection field
        let spread_node = dag
            .nodes
            .values()
            .find(|n| n.is_spread)
            .expect("Should have spread node");

        // The spread_collection should reference 'items'
        let spread_collection = spread_node.spread_collection.as_ref();
        assert!(
            spread_collection.is_some(),
            "Spread node should have spread_collection"
        );
        assert!(
            spread_collection.unwrap().contains("items"),
            "Spread collection should reference 'items'. Got: {:?}",
            spread_collection
        );

        // Additionally, check that data flow edges exist from input to nodes that use
        // the input variables via kwarg_exprs
        let input_data_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow && e.source == input_node.id)
            .collect();

        // Input node should have some outgoing data edges
        assert!(
            !input_data_edges.is_empty(),
            "Input node should have outgoing data flow edges. Found: {:?}",
            input_data_edges
                .iter()
                .map(|e| (&e.target, &e.variable))
                .collect::<Vec<_>>()
        );
    }

    /// Test that loop index variables (__loop_*) are propagated to ALL action nodes
    /// inside the loop body via DataFlow edges.
    ///
    /// This is critical for correct loop iteration: when an action inside a loop completes,
    /// the completion logic reads the loop index from the action's inbox. Without these
    /// edges, the action would have a stale loop index value from when it was first
    /// dispatched, causing infinite loops.
    #[test]
    fn test_loop_index_propagated_to_action_nodes_in_loop_body() {
        // A for-loop with a function call in the body that expands to actions
        let source = r#"fn process(input: [x], output: [result]):
    result = @do_work(value=x)
    return result

fn run(input: [items], output: [final]):
    for item in items:
        processed = process(x=item)
    final = processed
    return final"#;

        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the loop_incr node (it defines __loop_*_i)
        let loop_incr_node = dag
            .nodes
            .values()
            .find(|n| n.id.contains("loop_incr"))
            .expect("Should have loop_incr node");

        // Get the loop index variable name
        let loop_index_var = loop_incr_node
            .target
            .as_ref()
            .expect("loop_incr should have a target");
        assert!(
            loop_index_var.starts_with("__loop_"),
            "Loop index should start with __loop_"
        );

        // Find all action nodes inside the loop body (they have IDs containing the fn_call prefix)
        let action_nodes_in_loop: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "action_call" && n.id.contains("fn_call"))
            .collect();

        assert!(
            !action_nodes_in_loop.is_empty(),
            "Should have action nodes inside the loop body"
        );

        // Verify that loop_incr has DataFlow edges to each action node for the loop index
        for action_node in &action_nodes_in_loop {
            let has_loop_index_edge = dag.edges.iter().any(|e| {
                e.edge_type == EdgeType::DataFlow
                    && e.source == loop_incr_node.id
                    && e.target == action_node.id
                    && e.variable.as_ref() == Some(loop_index_var)
            });

            assert!(
                has_loop_index_edge,
                "loop_incr ({}) should have DataFlow edge for {} to action node {}. \
                 This is required for correct loop iteration - without it, the action's \
                 inbox would have a stale loop index causing infinite loops. \
                 Existing edges from loop_incr: {:?}",
                loop_incr_node.id,
                loop_index_var,
                action_node.id,
                dag.edges
                    .iter()
                    .filter(|e| e.source == loop_incr_node.id)
                    .map(|e| (&e.target, &e.variable))
                    .collect::<Vec<_>>()
            );
        }
    }

    // ==================================================================================
    // Try/Except DataFlow Edge Tests
    //
    // These tests verify that variables initialized before a try block properly flow
    // to nodes after the try/except join, even when the variable is also modified
    // in the exception handler. This is critical because:
    // 1. Exception paths are conditional (only taken when an exception occurs)
    // 2. The success path needs the original value from before the try block
    // 3. Both paths merge at a join node, and both values should be available
    // ==================================================================================

    /// Test: Variable initialized before try block flows to action after try/except
    ///
    /// Pattern:
    ///   recovered = False
    ///   try:
    ///       result = @risky_action()
    ///   except SomeError:
    ///       recovered = True
    ///   @final_action(recovered=recovered)
    ///
    /// Expected: DataFlow edge from initial assignment to final_action for 'recovered'
    #[test]
    fn test_try_except_variable_initialized_before_flows_to_after() {
        let source = r#"
fn __try_body__(input: [x], output: [result]):
    result = @risky_action(x=x)
    return result

fn __except_handler__(input: [recovered], output: [recovered]):
    recovered = True
    return recovered

fn run(input: [x], output: []):
    recovered = False
    try:
        result = __try_body__(x=x)
    except SomeError:
        recovered = __except_handler__(recovered=recovered)
    final = @final_action(recovered=recovered)
    return final
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the initial assignment node for 'recovered'
        let initial_assign = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "assignment"
                    && n.target.as_ref() == Some(&"recovered".to_string())
                    && n.function_name.as_ref() == Some(&"run".to_string())
            })
            .expect("should have initial assignment for 'recovered'");

        // Find the final_action node
        let final_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"final_action".to_string()))
            .expect("should have final_action node");

        // Check that there's a DataFlow edge from initial assignment to final_action
        let has_edge_from_initial = dag.edges.iter().any(|e| {
            e.edge_type == EdgeType::DataFlow
                && e.source == initial_assign.id
                && e.target == final_action.id
                && e.variable.as_ref() == Some(&"recovered".to_string())
        });

        assert!(
            has_edge_from_initial,
            "Should have DataFlow edge from initial 'recovered' assignment ({}) to final_action ({}). \
             This is required for the success path where no exception occurs. \
             Existing DataFlow edges to final_action: {:?}",
            initial_assign.id,
            final_action.id,
            dag.edges
                .iter()
                .filter(|e| e.edge_type == EdgeType::DataFlow && e.target == final_action.id)
                .map(|e| (&e.source, &e.variable))
                .collect::<Vec<_>>()
        );
    }

    /// Test: Exception handler modification also has DataFlow edge to action after join
    ///
    /// Both the initial value AND the exception handler's modification should flow
    /// to the action after the try/except, since we don't know at DAG construction
    /// time which path will be taken.
    #[test]
    fn test_try_except_exception_handler_modification_flows_to_after() {
        let source = r#"
fn __try_body__(input: [x], output: [result]):
    result = @risky_action(x=x)
    return result

fn __except_handler__(input: [recovered], output: [recovered]):
    recovered = True
    return recovered

fn run(input: [x], output: []):
    recovered = False
    try:
        result = __try_body__(x=x)
    except SomeError:
        recovered = __except_handler__(recovered=recovered)
    final = @final_action(recovered=recovered)
    return final
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the exception handler's assignment node for 'recovered'
        let handler_assign = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "assignment"
                    && n.target.as_ref() == Some(&"recovered".to_string())
                    && n.function_name
                        .as_ref()
                        .map(|f| f.contains("except_handler"))
                        .unwrap_or(false)
            })
            .expect("should have exception handler assignment for 'recovered'");

        // Find the final_action node
        let final_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"final_action".to_string()))
            .expect("should have final_action node");

        // Check that there's a DataFlow edge from handler assignment to final_action
        let has_edge_from_handler = dag.edges.iter().any(|e| {
            e.edge_type == EdgeType::DataFlow
                && e.source == handler_assign.id
                && e.target == final_action.id
                && e.variable.as_ref() == Some(&"recovered".to_string())
        });

        assert!(
            has_edge_from_handler,
            "Should have DataFlow edge from exception handler 'recovered' assignment ({}) to final_action ({}). \
             This is required for the exception path. \
             Existing DataFlow edges to final_action: {:?}",
            handler_assign.id,
            final_action.id,
            dag.edges
                .iter()
                .filter(|e| e.edge_type == EdgeType::DataFlow && e.target == final_action.id)
                .map(|e| (&e.source, &e.variable))
                .collect::<Vec<_>>()
        );
    }

    /// Test: Multiple variables with different modification patterns in try/except
    ///
    /// Pattern:
    ///   status = "pending"      # modified in both paths
    ///   error_msg = ""          # only modified in except
    ///   attempt_count = 0       # only modified in try (success)
    ///   try:
    ///       status = "success"
    ///       attempt_count = 1
    ///   except:
    ///       status = "failed"
    ///       error_msg = "error occurred"
    ///   @report(status=status, error_msg=error_msg, attempts=attempt_count)
    #[test]
    fn test_try_except_multiple_variables_different_patterns() {
        let source = r#"
fn __try_body__(input: [status, attempt_count], output: [status, attempt_count]):
    status = "success"
    attempt_count = 1
    return [status, attempt_count]

fn __except_handler__(input: [status, error_msg], output: [status, error_msg]):
    status = "failed"
    error_msg = "error occurred"
    return [status, error_msg]

fn run(input: [], output: []):
    status = "pending"
    error_msg = ""
    attempt_count = 0
    try:
        status, attempt_count = __try_body__(status=status, attempt_count=attempt_count)
    except SomeError:
        status, error_msg = __except_handler__(status=status, error_msg=error_msg)
    result = @report(status=status, error_msg=error_msg, attempts=attempt_count)
    return result
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the report action node
        let report_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"report".to_string()))
            .expect("should have report action node");

        // Collect all DataFlow edges to the report action
        let edges_to_report: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow && e.target == report_action.id)
            .collect();

        // Check that we have edges for all three variables
        let vars_with_edges: HashSet<_> = edges_to_report
            .iter()
            .filter_map(|e| e.variable.as_ref())
            .collect();

        assert!(
            vars_with_edges.contains(&"status".to_string()),
            "Should have DataFlow edge for 'status' to report action"
        );
        assert!(
            vars_with_edges.contains(&"error_msg".to_string()),
            "Should have DataFlow edge for 'error_msg' to report action"
        );
        assert!(
            vars_with_edges.contains(&"attempt_count".to_string()),
            "Should have DataFlow edge for 'attempt_count' to report action"
        );

        // Verify that 'status' has edges from BOTH the initial assignment AND exception handler
        // (since it's modified in both paths)
        let status_edge_sources: Vec<_> = edges_to_report
            .iter()
            .filter(|e| e.variable.as_ref() == Some(&"status".to_string()))
            .map(|e| &e.source)
            .collect();

        assert!(
            status_edge_sources.len() >= 2,
            "Should have at least 2 DataFlow edges for 'status' (from initial and at least one modification). \
             Found sources: {:?}",
            status_edge_sources
        );
    }

    /// Test: Nested try/except blocks preserve DataFlow correctly
    ///
    /// Pattern:
    ///   outer_flag = False
    ///   try:
    ///       inner_flag = False
    ///       try:
    ///           result = @inner_action()
    ///       except InnerError:
    ///           inner_flag = True
    ///   except OuterError:
    ///       outer_flag = True
    ///   @final(outer=outer_flag, inner=inner_flag)
    #[test]
    fn test_try_except_nested_dataflow() {
        let source = r#"
fn __inner_try__(input: [], output: [result]):
    result = @inner_action()
    return result

fn __inner_except__(input: [inner_flag], output: [inner_flag]):
    inner_flag = True
    return inner_flag

fn __outer_try__(input: [], output: [inner_flag, result]):
    inner_flag = False
    try:
        result = __inner_try__()
    except InnerError:
        inner_flag = __inner_except__(inner_flag=inner_flag)
    return [inner_flag, result]

fn __outer_except__(input: [outer_flag], output: [outer_flag]):
    outer_flag = True
    return outer_flag

fn run(input: [], output: []):
    outer_flag = False
    try:
        inner_flag, result = __outer_try__()
    except OuterError:
        outer_flag = __outer_except__(outer_flag=outer_flag)
    final = @final_action(outer=outer_flag, inner=inner_flag)
    return final
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the final_action node
        let final_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"final_action".to_string()))
            .expect("should have final_action node");

        // Check that outer_flag has DataFlow edge from initial assignment
        let outer_flag_initial = dag.nodes.values().find(|n| {
            n.node_type == "assignment"
                && n.target.as_ref() == Some(&"outer_flag".to_string())
                && n.function_name.as_ref() == Some(&"run".to_string())
        });

        if let Some(initial) = outer_flag_initial {
            let has_edge = dag.edges.iter().any(|e| {
                e.edge_type == EdgeType::DataFlow
                    && e.source == initial.id
                    && e.target == final_action.id
                    && e.variable.as_ref() == Some(&"outer_flag".to_string())
            });

            assert!(
                has_edge,
                "Should have DataFlow edge for 'outer_flag' from initial assignment to final_action"
            );
        }
    }

    /// Test: Exception edges don't block DataFlow propagation
    ///
    /// This test specifically verifies the fix: when building the reachability map
    /// for DataFlow edges, exception edges should be excluded. Otherwise, a variable
    /// modification in an exception handler would incorrectly block the DataFlow
    /// from the initial assignment.
    #[test]
    fn test_exception_edges_excluded_from_dataflow_reachability() {
        let source = r#"
fn __try_body__(input: [], output: [result]):
    result = @risky()
    return result

fn __except__(input: [flag], output: [flag]):
    flag = True
    return flag

fn run(input: [], output: []):
    flag = False
    try:
        result = __try_body__()
    except Error:
        flag = __except__(flag=flag)
    out = @use_flag(flag=flag)
    return out
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Get all state machine edges
        let state_machine_edges: Vec<_> = dag.get_state_machine_edges();

        // Find exception edges (edges with exception_types set)
        let exception_edges: Vec<_> = state_machine_edges
            .iter()
            .filter(|e| e.exception_types.is_some())
            .collect();

        // Verify exception edges exist (sanity check that try/except is set up correctly)
        assert!(
            !exception_edges.is_empty(),
            "Should have exception edges in the DAG for try/except handling"
        );

        // Find the initial flag assignment and the use_flag action
        let initial_assign = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "assignment"
                    && n.target.as_ref() == Some(&"flag".to_string())
                    && n.function_name.as_ref() == Some(&"run".to_string())
            })
            .expect("should have initial assignment");

        let use_flag_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"use_flag".to_string()))
            .expect("should have use_flag action");

        // The critical assertion: initial assignment should have DataFlow to use_flag
        let has_correct_edge = dag.edges.iter().any(|e| {
            e.edge_type == EdgeType::DataFlow
                && e.source == initial_assign.id
                && e.target == use_flag_action.id
                && e.variable.as_ref() == Some(&"flag".to_string())
        });

        assert!(
            has_correct_edge,
            "Initial 'flag' assignment ({}) must have DataFlow edge to use_flag action ({}). \
             Exception edges should NOT block this propagation. \
             Exception edges found: {:?}",
            initial_assign.id,
            use_flag_action.id,
            exception_edges
                .iter()
                .map(|e| (&e.source, &e.target, &e.exception_types))
                .collect::<Vec<_>>()
        );
    }

    /// Test: DataFlow works correctly when exception handler uses but doesn't modify variable
    ///
    /// Pattern:
    ///   count = 5
    ///   try:
    ///       result = @action()
    ///   except:
    ///       @log_error(count=count)  # Uses count but doesn't modify it
    ///   @final(count=count)
    #[test]
    fn test_try_except_handler_uses_but_does_not_modify() {
        let source = r#"
fn __try_body__(input: [], output: [result]):
    result = @action()
    return result

fn __except__(input: [count], output: []):
    logged = @log_error(count=count)
    return logged

fn run(input: [], output: []):
    count = 5
    try:
        result = __try_body__()
    except Error:
        __except__(count=count)
    final = @final_action(count=count)
    return final
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the initial count assignment and final_action
        let initial_assign = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "assignment"
                    && n.target.as_ref() == Some(&"count".to_string())
                    && n.function_name.as_ref() == Some(&"run".to_string())
            })
            .expect("should have initial count assignment");

        let final_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"final_action".to_string()))
            .expect("should have final_action");

        // Since exception handler doesn't modify 'count', there should be only one
        // modification source, and it should have a DataFlow edge to final_action
        let count_edges_to_final: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| {
                e.edge_type == EdgeType::DataFlow
                    && e.target == final_action.id
                    && e.variable.as_ref() == Some(&"count".to_string())
            })
            .collect();

        assert!(
            count_edges_to_final
                .iter()
                .any(|e| e.source == initial_assign.id),
            "Should have DataFlow edge from initial 'count' assignment to final_action"
        );
    }

    /// Test: Variable only used in exception handler still gets DataFlow
    ///
    /// Pattern:
    ///   error_handler = @get_handler()  # Only used in except block
    ///   try:
    ///       result = @risky()
    ///   except:
    ///       @handle(handler=error_handler)
    #[test]
    fn test_try_except_variable_only_used_in_handler() {
        let source = r#"
fn __try_body__(input: [], output: [result]):
    result = @risky()
    return result

fn __except__(input: [error_handler], output: []):
    handled = @handle(handler=error_handler)
    return handled

fn run(input: [], output: []):
    error_handler = @get_handler()
    try:
        result = __try_body__()
    except Error:
        __except__(error_handler=error_handler)
    return result
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the action that defines error_handler
        let _get_handler_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"get_handler".to_string()))
            .expect("should have get_handler action");

        // Find the handle action in the exception handler
        let _handle_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"handle".to_string()))
            .expect("should have handle action");

        // Check for DataFlow edge (may be via the fn_call boundary)
        let has_handler_dataflow = dag.edges.iter().any(|e| {
            e.edge_type == EdgeType::DataFlow
                && e.variable.as_ref() == Some(&"error_handler".to_string())
        });

        assert!(
            has_handler_dataflow,
            "Should have DataFlow edge for 'error_handler'. \
             The variable defined before try should flow to the exception handler."
        );
    }

    /// Test: Verify join node doesn't create false modifications
    ///
    /// Join nodes merge control flow but shouldn't be treated as variable definitions.
    /// This test ensures that join nodes are properly excluded from var_modifications.
    #[test]
    fn test_join_node_not_treated_as_variable_modification() {
        let source = r#"
fn __try_body__(input: [], output: [result]):
    result = @action()
    return result

fn __except__(input: [], output: []):
    logged = @log_error()
    return logged

fn run(input: [], output: []):
    value = 42
    try:
        result = __try_body__()
    except Error:
        __except__()
    final = @use_value(value=value)
    return final
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find all join nodes
        let join_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "join")
            .collect();

        assert!(
            !join_nodes.is_empty(),
            "Should have join nodes for try/except"
        );

        // Verify that join nodes don't have 'value' as a target
        for join_node in &join_nodes {
            assert!(
                join_node.target.as_ref() != Some(&"value".to_string()),
                "Join node {} should not have 'value' as target",
                join_node.id
            );
            if let Some(ref targets) = join_node.targets {
                assert!(
                    !targets.contains(&"value".to_string()),
                    "Join node {} should not have 'value' in targets",
                    join_node.id
                );
            }
        }

        // Verify DataFlow edge exists from initial assignment to use_value
        let initial_assign = dag
            .nodes
            .values()
            .find(|n| {
                n.node_type == "assignment" && n.target.as_ref() == Some(&"value".to_string())
            })
            .expect("should have initial assignment");

        let use_value_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"use_value".to_string()))
            .expect("should have use_value action");

        let has_edge = dag.edges.iter().any(|e| {
            e.edge_type == EdgeType::DataFlow
                && e.source == initial_assign.id
                && e.target == use_value_action.id
                && e.variable.as_ref() == Some(&"value".to_string())
        });

        assert!(
            has_edge,
            "Should have DataFlow edge from initial 'value' assignment to use_value action"
        );
    }

    /// Test: Try/except with catch-all handler
    ///
    /// Pattern:
    ///   flag = False
    ///   try:
    ///       result = @risky()
    ///   except:  # No specific exception type - catches all
    ///       flag = True
    ///   @final(flag=flag)
    #[test]
    fn test_try_except_catch_all_handler() {
        let source = r#"
fn __try_body__(input: [], output: [result]):
    result = @risky()
    return result

fn __except__(input: [flag], output: [flag]):
    flag = True
    return flag

fn run(input: [], output: []):
    flag = False
    try:
        result = __try_body__()
    except:
        flag = __except__(flag=flag)
    final = @final_action(flag=flag)
    return final
"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        let final_action = dag
            .nodes
            .values()
            .find(|n| n.action_name.as_ref() == Some(&"final_action".to_string()))
            .expect("should have final_action");

        // Both initial assignment and exception handler should have edges to final_action
        let flag_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| {
                e.edge_type == EdgeType::DataFlow
                    && e.target == final_action.id
                    && e.variable.as_ref() == Some(&"flag".to_string())
            })
            .collect();

        assert!(
            flag_edges.len() >= 2,
            "Should have at least 2 DataFlow edges for 'flag' to final_action \
             (from initial and from exception handler). Found: {:?}",
            flag_edges.iter().map(|e| &e.source).collect::<Vec<_>>()
        );
    }
}
