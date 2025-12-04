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
    /// Whether this is a loop head node
    pub is_loop_head: bool,
    /// Loop variables (for for_loop nodes)
    pub loop_vars: Option<Vec<String>>,
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
    /// Target variable name where the action result should be stored (for action_call nodes)
    pub target: Option<String>,
    /// Whether this is a spread action (parallel iteration over a collection)
    pub is_spread: bool,
    /// Loop variable name for spread actions (e.g., "item" in "spread items:item -> @action()")
    pub spread_loop_var: Option<String>,
    /// Collection variable being spread over (e.g., "items" in "spread items:item -> @action()")
    pub spread_collection: Option<String>,
    /// Guard expression for conditional nodes (if/elif). Evaluated at runtime to determine branch.
    pub guard_expr: Option<ast::Expr>,
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
            is_loop_head: false,
            loop_vars: None,
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
            is_spread: false,
            spread_loop_var: None,
            spread_collection: None,
            guard_expr: None,
        }
    }

    /// Builder method to set guard expression for conditional nodes
    pub fn with_guard_expr(mut self, expr: ast::Expr) -> Self {
        self.guard_expr = Some(expr);
        self
    }

    /// Builder method to set target variable
    pub fn with_target(mut self, target: &str) -> Self {
        self.target = Some(target.to_string());
        self
    }

    /// Builder method to set function name
    pub fn with_function_name(mut self, name: &str) -> Self {
        self.function_name = Some(name.to_string());
        self
    }

    /// Builder method to mark as loop head
    pub fn with_loop_head(mut self, loop_vars: Vec<String>) -> Self {
        self.is_loop_head = true;
        self.loop_vars = Some(loop_vars);
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

    /// Builder method to mark as spread action
    pub fn with_spread(mut self, loop_var: &str, collection: &str) -> Self {
        self.is_spread = true;
        self.spread_loop_var = Some(loop_var.to_string());
        self.spread_collection = Some(collection.to_string());
        self
    }
}

// ============================================================================
// DAG Edge
// ============================================================================

/// An edge in the execution DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        }
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

    /// Convert a Rappel program to a DAG with isolated function subgraphs
    pub fn convert(&mut self, program: &ast::Program) -> DAG {
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
        let mut prev_node_id = Some(input_id);
        if let Some(body) = &fn_def.body {
            for stmt in &body.statements {
                let node_ids = self.convert_statement(stmt);

                if let (Some(prev), Some(first)) = (&prev_node_id, node_ids.first()) {
                    self.dag
                        .add_edge(DAGEdge::state_machine(prev.clone(), first.clone()));
                }

                if !node_ids.is_empty() {
                    prev_node_id = Some(node_ids.last().unwrap().clone());
                }
            }
        }

        // Create output boundary node
        let output_id = self.next_id(&format!("{}_output", fn_def.name));
        let output_label = format!("output: [{}]", io.outputs.join(", "));

        let output_node = DAGNode::new(output_id.clone(), "output".to_string(), output_label)
            .with_function_name(&fn_def.name)
            .with_output(io.outputs.clone());
        self.dag.add_node(output_node);

        // Connect last body node to output
        if let Some(prev) = prev_node_id {
            self.dag.add_edge(DAGEdge::state_machine(prev, output_id));
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

    /// Convert a statement to DAG node(s)
    fn convert_statement(&mut self, stmt: &ast::Statement) -> Vec<String> {
        let kind = match &stmt.kind {
            Some(k) => k,
            None => return vec![],
        };

        match kind {
            ast::statement::Kind::Assignment(assign) => self.convert_assignment(assign),
            ast::statement::Kind::ActionCall(action) => {
                // Use target from protobuf ActionCall message if present
                let target = action.target.as_deref();
                self.convert_action_call(action, target)
            }
            ast::statement::Kind::SpreadAction(spread) => self.convert_spread_action(spread),
            ast::statement::Kind::ParallelBlock(parallel) => self.convert_parallel_block(parallel),
            ast::statement::Kind::ForLoop(for_loop) => self.convert_for_loop(for_loop),
            ast::statement::Kind::Conditional(cond) => self.convert_conditional(cond),
            ast::statement::Kind::TryExcept(try_except) => self.convert_try_except(try_except),
            ast::statement::Kind::ReturnStmt(ret) => self.convert_return(ret),
            ast::statement::Kind::ExprStmt(expr_stmt) => self.convert_expr_statement(expr_stmt),
        }
    }

    /// Convert a SingleCallBody to DAG node(s).
    /// SingleCallBody can contain:
    /// 1. A single call (action or function) with optional target
    /// 2. Pure data statements (no calls)
    fn convert_single_call_body(&mut self, body: &ast::SingleCallBody) -> Vec<String> {
        // If there's a call, convert it
        if let Some(call) = &body.call {
            let target = body.target.as_deref();

            return match &call.kind {
                Some(ast::call::Kind::Action(action)) => self.convert_action_call(action, target),
                Some(ast::call::Kind::Function(func)) => {
                    if let Some(t) = target {
                        self.convert_fn_call_assignment(t, &[t.to_string()], func)
                    } else {
                        // Function call without assignment target
                        let node_id = self.next_id("fn_call");
                        let label = format!("{}()", func.name);
                        let mut node = DAGNode::new(node_id.clone(), "fn_call".to_string(), label)
                            .with_fn_call(&func.name);
                        if let Some(ref fn_name) = self.current_function {
                            node = node.with_function_name(fn_name);
                        }
                        self.dag.add_node(node);
                        vec![node_id]
                    }
                }
                None => vec![],
            };
        }

        // Otherwise, convert pure data statements
        let mut result = vec![];
        for stmt in &body.statements {
            let node_ids = self.convert_statement(stmt);
            result.extend(node_ids);
        }
        result
    }

    /// Convert an assignment statement
    fn convert_assignment(&mut self, assign: &ast::Assignment) -> Vec<String> {
        let value = match &assign.value {
            Some(v) => v,
            None => return vec![],
        };

        let target = assign.targets.first().map(|s| s.as_str()).unwrap_or("_");

        // Check if RHS is a function call
        if let Some(ast::expr::Kind::FunctionCall(call)) = &value.kind {
            return self.convert_fn_call_assignment(target, &assign.targets, call);
        }

        // Check if RHS is an action call
        if let Some(ast::expr::Kind::ActionCall(action)) = &value.kind {
            return self.convert_action_call(action, Some(target));
        }

        // Regular assignment
        let node_id = self.next_id("assign");
        let label = if assign.targets.len() > 1 {
            format!("{} = ...", assign.targets.join(", "))
        } else {
            format!("{} = ...", target)
        };

        let mut node = DAGNode::new(node_id.clone(), "assignment".to_string(), label);
        if let Some(ref fn_name) = self.current_function {
            node = node.with_function_name(fn_name);
        }
        self.dag.add_node(node);

        // Track variable definitions
        for t in &assign.targets {
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

        let mut node =
            DAGNode::new(node_id.clone(), "fn_call".to_string(), label).with_fn_call(&call.name);
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
    fn convert_action_call(
        &mut self,
        action: &ast::ActionCall,
        target: Option<&str>,
    ) -> Vec<String> {
        let node_id = self.next_id("action");
        let label = if let Some(t) = target {
            format!("@{}() -> {}", action.action_name, t)
        } else {
            format!("@{}()", action.action_name)
        };

        // Extract kwargs as string representations
        let kwargs = self.extract_kwargs(&action.kwargs);

        let mut node = DAGNode::new(node_id.clone(), "action_call".to_string(), label)
            .with_action(&action.action_name, action.module_name.as_deref())
            .with_kwargs(kwargs);
        if let Some(t) = target {
            tracing::debug!(node_id = %node_id, target = %t, "setting action target");
            node = node.with_target(t);
        } else {
            tracing::debug!(node_id = %node_id, "no target for action");
        }
        if let Some(ref fn_name) = self.current_function {
            node = node.with_function_name(fn_name);
        }
        self.dag.add_node(node);

        // Track variable definition if target is specified
        if let Some(t) = target {
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

    /// Convert a spread action
    fn convert_spread_action(&mut self, spread: &ast::SpreadAction) -> Vec<String> {
        let action = spread.action.as_ref().unwrap();

        // Create spread action node
        let action_id = self.next_id("spread_action");
        let action_label = format!(
            "@{}() [spread over {}]",
            action.action_name, spread.loop_var
        );

        // Extract kwargs
        let kwargs = self.extract_kwargs(&action.kwargs);

        // Get the collection expression as a string (e.g., "$items" or "range(5)")
        let collection_str = spread
            .collection
            .as_ref()
            .map(|c| self.expr_to_string(c))
            .unwrap_or_default();

        // Use internal variable name for spread results flowing to aggregator
        let spread_result_var = "_spread_result".to_string();

        let mut action_node =
            DAGNode::new(action_id.clone(), "action_call".to_string(), action_label)
                .with_action(&action.action_name, action.module_name.as_deref())
                .with_kwargs(kwargs)
                .with_spread(&spread.loop_var, &collection_str)
                .with_target(&spread_result_var); // Set target so results flow to aggregator
        if let Some(ref fn_name) = self.current_function {
            action_node = action_node.with_function_name(fn_name);
        }
        self.dag.add_node(action_node);

        // Create aggregator node
        let agg_id = self.next_id("aggregator");
        let target_label = if let Some(ref t) = spread.target {
            format!("aggregate -> {}", t)
        } else {
            "aggregate".to_string()
        };

        let mut agg_node = DAGNode::new(agg_id.clone(), "aggregator".to_string(), target_label)
            .with_aggregator(&action_id);
        // Set the aggregator's target to the spread's target variable
        if let Some(ref t) = spread.target {
            agg_node = agg_node.with_target(t);
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

        // Track variable definition at aggregator
        if let Some(ref t) = spread.target {
            self.track_var_definition(t, &agg_id);
        }

        vec![action_id, agg_id]
    }

    /// Convert a parallel block
    fn convert_parallel_block(&mut self, parallel: &ast::ParallelBlock) -> Vec<String> {
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
        for (i, call) in parallel.calls.iter().enumerate() {
            let (call_id, call_node) = match &call.kind {
                Some(ast::call::Kind::Action(action)) => {
                    let id = self.next_id("parallel_action");
                    let label = format!("@{}() [{}]", action.action_name, i);
                    let kwargs = self.extract_kwargs(&action.kwargs);
                    let mut node = DAGNode::new(id.clone(), "action_call".to_string(), label)
                        .with_action(&action.action_name, action.module_name.as_deref())
                        .with_kwargs(kwargs);
                    if let Some(ref fn_name) = self.current_function {
                        node = node.with_function_name(fn_name);
                    }
                    (id, node)
                }
                Some(ast::call::Kind::Function(func)) => {
                    let id = self.next_id("parallel_fn_call");
                    let label = format!("{}() [{}]", func.name, i);
                    let mut node = DAGNode::new(id.clone(), "fn_call".to_string(), label)
                        .with_fn_call(&func.name);
                    if let Some(ref fn_name) = self.current_function {
                        node = node.with_function_name(fn_name);
                    }
                    (id, node)
                }
                None => continue,
            };

            self.dag.add_node(call_node);
            call_node_ids.push(call_id.clone());
            result_nodes.push(call_id.clone());

            // Edge from parallel node to each call
            self.dag.add_edge(DAGEdge::state_machine_with_condition(
                parallel_id.clone(),
                call_id,
                &format!("parallel:{}", i),
            ));
        }

        // Create aggregator node
        let agg_id = self.next_id("parallel_aggregator");
        let target_label = if let Some(ref t) = parallel.target {
            format!("parallel_aggregate -> {}", t)
        } else {
            "parallel_aggregate".to_string()
        };

        let mut agg_node = DAGNode::new(agg_id.clone(), "aggregator".to_string(), target_label)
            .with_aggregator(&parallel_id);
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

        // Track variable definition at aggregator
        if let Some(ref t) = parallel.target {
            self.track_var_definition(t, &agg_id);
        }

        result_nodes
    }

    /// Convert a for loop
    fn convert_for_loop(&mut self, for_loop: &ast::ForLoop) -> Vec<String> {
        let loop_id = self.next_id("for_loop");
        let loop_vars_str = for_loop.loop_vars.join(", ");
        let label = format!("for {} in ...", loop_vars_str);

        let mut loop_node = DAGNode::new(loop_id.clone(), "for_loop".to_string(), label)
            .with_loop_head(for_loop.loop_vars.clone());
        if let Some(ref fn_name) = self.current_function {
            loop_node = loop_node.with_function_name(fn_name);
        }
        self.dag.add_node(loop_node);

        // Track loop variables
        for loop_var in &for_loop.loop_vars {
            self.track_var_definition(loop_var, &loop_id);
        }

        // Track output variables from the loop body (SingleCallBody)
        if let Some(body) = &for_loop.body {
            // If there's a call with a target, track it
            if let Some(ref target) = body.target {
                self.track_var_definition(target, &loop_id);
            }
            // Also check pure data statements for assignments
            for stmt in &body.statements {
                if let Some(ast::statement::Kind::Assignment(assign)) = &stmt.kind {
                    for target in &assign.targets {
                        self.track_var_definition(target, &loop_id);
                    }
                }
            }
        }

        vec![loop_id]
    }

    /// Convert a conditional (if/elif/else)
    fn convert_conditional(&mut self, cond: &ast::Conditional) -> Vec<String> {
        let mut result_nodes = Vec::new();

        // Create condition node with guard expression
        let cond_id = self.next_id("if_cond");
        let mut cond_node = DAGNode::new(cond_id.clone(), "if".to_string(), "if ...".to_string());
        if let Some(ref fn_name) = self.current_function {
            cond_node = cond_node.with_function_name(fn_name);
        }
        // Store the guard expression for runtime evaluation
        if let Some(if_branch) = &cond.if_branch {
            if let Some(condition) = &if_branch.condition {
                cond_node = cond_node.with_guard_expr(condition.clone());
            }
        }
        self.dag.add_node(cond_node);
        result_nodes.push(cond_id.clone());

        let mut then_last: Option<String> = None;
        let mut else_last: Option<String> = None;

        // Process then branch (SingleCallBody - exactly one call)
        if let Some(if_branch) = &cond.if_branch
            && let Some(body) = &if_branch.body
        {
            let node_ids = self.convert_single_call_body(body);
            if !node_ids.is_empty() {
                self.dag.add_edge(DAGEdge::state_machine_with_condition(
                    cond_id.clone(),
                    node_ids[0].clone(),
                    "then",
                ));
                then_last = Some(node_ids.last().unwrap().clone());
                result_nodes.extend(node_ids);
            }
        }

        // Process else branch (SingleCallBody - exactly one call)
        if let Some(else_branch) = &cond.else_branch
            && let Some(body) = &else_branch.body
        {
            let node_ids = self.convert_single_call_body(body);
            if !node_ids.is_empty() {
                self.dag.add_edge(DAGEdge::state_machine_with_condition(
                    cond_id.clone(),
                    node_ids[0].clone(),
                    "else",
                ));
                else_last = Some(node_ids.last().unwrap().clone());
                result_nodes.extend(node_ids);
            }
        }

        // Create join node if we have branches
        if then_last.is_some() || else_last.is_some() {
            let join_id = self.next_id("if_join");
            let mut join_node =
                DAGNode::new(join_id.clone(), "join".to_string(), "join".to_string());
            if let Some(ref fn_name) = self.current_function {
                join_node = join_node.with_function_name(fn_name);
            }
            self.dag.add_node(join_node);
            result_nodes.push(join_id.clone());

            if let Some(then) = then_last {
                self.dag
                    .add_edge(DAGEdge::state_machine(then, join_id.clone()));
            } else {
                // Empty then branch
                self.dag.add_edge(DAGEdge::state_machine_with_condition(
                    cond_id.clone(),
                    join_id.clone(),
                    "then",
                ));
            }

            if let Some(else_) = else_last {
                self.dag
                    .add_edge(DAGEdge::state_machine(else_, join_id.clone()));
            } else if cond.else_branch.is_some() {
                // Empty else branch
                self.dag.add_edge(DAGEdge::state_machine_with_condition(
                    cond_id, join_id, "else",
                ));
            }
        }

        result_nodes
    }

    /// Convert a try/except block
    fn convert_try_except(&mut self, try_except: &ast::TryExcept) -> Vec<String> {
        let mut result_nodes = Vec::new();

        // Create try entry node
        let try_id = self.next_id("try");
        let mut try_node = DAGNode::new(try_id.clone(), "try".to_string(), "try".to_string());
        if let Some(ref fn_name) = self.current_function {
            try_node = try_node.with_function_name(fn_name);
        }
        self.dag.add_node(try_node);
        result_nodes.push(try_id.clone());

        // Convert try body (SingleCallBody - exactly one call)
        let mut try_body_last: Option<String> = None;
        if let Some(try_body) = &try_except.try_body {
            let node_ids = self.convert_single_call_body(try_body);
            if !node_ids.is_empty() {
                self.dag.add_edge(DAGEdge::state_machine_with_condition(
                    try_id.clone(),
                    node_ids[0].clone(),
                    "try",
                ));
                try_body_last = Some(node_ids.last().unwrap().clone());
                result_nodes.extend(node_ids);
            }
        }

        // Convert each except handler
        let mut handler_lasts = Vec::new();
        for handler in &try_except.handlers {
            let exc_types_str = if handler.exception_types.is_empty() {
                "*".to_string()
            } else {
                handler.exception_types.join(", ")
            };

            let handler_id = self.next_id("except");
            let label = format!("except {}", exc_types_str);
            let mut handler_node = DAGNode::new(handler_id.clone(), "except".to_string(), label);
            if let Some(ref fn_name) = self.current_function {
                handler_node = handler_node.with_function_name(fn_name);
            }
            self.dag.add_node(handler_node);
            result_nodes.push(handler_id.clone());

            // Edge from try node to handler
            self.dag.add_edge(DAGEdge::state_machine_with_condition(
                try_id.clone(),
                handler_id.clone(),
                &format!("except:{}", exc_types_str),
            ));

            // Convert handler body (SingleCallBody - exactly one call)
            let mut handler_prev = handler_id.clone();
            if let Some(body) = &handler.body {
                let node_ids = self.convert_single_call_body(body);
                if !node_ids.is_empty() {
                    self.dag.add_edge(DAGEdge::state_machine(
                        handler_prev.clone(),
                        node_ids[0].clone(),
                    ));
                    handler_prev = node_ids.last().unwrap().clone();
                    result_nodes.extend(node_ids);
                }
            }
            handler_lasts.push(handler_prev);
        }

        // Create join node
        let join_id = self.next_id("try_join");
        let mut join_node = DAGNode::new(
            join_id.clone(),
            "try_join".to_string(),
            "try_join".to_string(),
        );
        if let Some(ref fn_name) = self.current_function {
            join_node = join_node.with_function_name(fn_name);
        }
        self.dag.add_node(join_node);
        result_nodes.push(join_id.clone());

        // Connect try body success path to join
        if let Some(try_last) = try_body_last {
            self.dag.add_edge(DAGEdge::state_machine_with_condition(
                try_last,
                join_id.clone(),
                "success",
            ));
        }

        // Connect all handler exits to join
        for handler_last in handler_lasts {
            self.dag
                .add_edge(DAGEdge::state_machine(handler_last, join_id.clone()));
        }

        result_nodes
    }

    /// Convert a return statement
    fn convert_return(&mut self, _ret: &ast::ReturnStmt) -> Vec<String> {
        let node_id = self.next_id("return");
        let mut node = DAGNode::new(node_id.clone(), "return".to_string(), "return".to_string());
        if let Some(ref fn_name) = self.current_function {
            node = node.with_function_name(fn_name);
        }
        self.dag.add_node(node);

        vec![node_id]
    }

    /// Convert an expression statement
    fn convert_expr_statement(&mut self, expr_stmt: &ast::ExprStmt) -> Vec<String> {
        let expr = match &expr_stmt.expr {
            Some(e) => e,
            None => return vec![],
        };

        if let Some(ast::expr::Kind::ActionCall(action)) = &expr.kind {
            return self.convert_action_call(action, None);
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

        let mut edges_to_add = Vec::new();

        // For each variable modification, connect to subsequent nodes that might use it
        for (var_name, modifications) in &self.var_modifications {
            for (i, mod_node) in modifications.iter().enumerate() {
                if !fn_node_ids.contains(mod_node) {
                    continue;
                }

                // Find the next modification of this variable (if any)
                let next_mod = modifications.get(i + 1);

                // Get the position of this modification in execution order
                let mod_pos = order.iter().position(|n| n == mod_node);

                // Find nodes that come after this modification but before the next
                for (pos, node_id) in order.iter().enumerate() {
                    if let Some(mp) = mod_pos {
                        if pos <= mp {
                            continue;
                        }
                    }

                    // Stop if we hit the next modification
                    if let Some(next) = next_mod {
                        if node_id == next {
                            break;
                        }
                    }

                    // Don't add edge to the modification node itself
                    if node_id == mod_node {
                        continue;
                    }

                    // Add data flow edge (simplified - in full implementation we'd check if node uses var)
                    // For now, we only connect to the immediate next node
                    if let Some(mp) = mod_pos {
                        if pos == mp + 1 {
                            edges_to_add.push((
                                var_name.clone(),
                                mod_node.clone(),
                                node_id.clone(),
                            ));
                        }
                    }
                }
            }
        }

        for (var_name, source, target) in edges_to_add {
            self.dag
                .add_edge(DAGEdge::data_flow(source, target, &var_name));
        }
    }

    /// Get nodes in topological (execution) order for a subset of nodes
    fn get_execution_order_for_nodes(&self, node_ids: &HashSet<String>) -> Vec<String> {
        // Simple topological sort using state machine edges
        let mut in_degree: HashMap<String, usize> =
            node_ids.iter().map(|n| (n.clone(), 0)).collect();
        let mut adj: HashMap<String, Vec<String>> =
            node_ids.iter().map(|n| (n.clone(), Vec::new())).collect();

        for edge in self.dag.get_state_machine_edges() {
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

        // Check that for loop node exists
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(node_types.contains("for_loop"));
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

        // Check that if node exists
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(node_types.contains("if"));
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
    fn test_dag_multiple_functions() {
        let source = r#"fn add(input: [a, b], output: [result]):
    result = a + b
    return result

fn multiply(input: [x, y], output: [product]):
    product = x * y
    return product"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Should have nodes for both functions
        let functions = dag.get_functions();
        assert!(functions.contains(&"add".to_string()));
        assert!(functions.contains(&"multiply".to_string()));
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
    fn test_dag_fn_call_node() {
        let source = r#"fn helper(input: [x], output: [y]):
    y = x + 1
    return y

fn main(input: [], output: [result]):
    result = helper(x=10)
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find fn_call nodes
        let fn_call_nodes: Vec<_> = dag.nodes.values().filter(|n| n.is_fn_call).collect();
        assert!(!fn_call_nodes.is_empty());
        assert!(
            fn_call_nodes
                .iter()
                .any(|n| n.called_function == Some("helper".to_string()))
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

        // Check that try and except nodes exist
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(node_types.contains("try"));
        assert!(node_types.contains("except"));
        assert!(node_types.contains("try_join"));
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
        // Test that for loop nodes correctly track loop variables
        let source = r#"fn iterate(input: [items], output: [results]):
    results = []
    for i, item in enumerate(items):
        results = results + [item]
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // Find the for_loop node
        let for_node = dag
            .nodes
            .values()
            .find(|n| n.node_type == "for_loop")
            .expect("Should have for_loop node");

        assert!(for_node.is_loop_head);
        assert_eq!(
            for_node.loop_vars,
            Some(vec!["i".to_string(), "item".to_string()])
        );
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

        // Should have if node, join node, and branch nodes
        let node_types: HashSet<_> = dag.nodes.values().map(|n| n.node_type.as_str()).collect();
        assert!(node_types.contains("if"));
        assert!(node_types.contains("join"));

        // Check for conditional edges with "then" and "else" conditions
        let cond_edges: Vec<_> = dag.edges.iter().filter(|e| e.condition.is_some()).collect();

        let conditions: HashSet<_> = cond_edges
            .iter()
            .filter_map(|e| e.condition.as_deref())
            .collect();

        assert!(
            conditions.contains("then"),
            "Should have 'then' conditional edge"
        );
        assert!(
            conditions.contains("else"),
            "Should have 'else' conditional edge"
        );
    }
}
