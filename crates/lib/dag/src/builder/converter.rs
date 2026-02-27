//! IR -> DAG conversion and graph builder.

use std::collections::HashMap;

use waymark_proto::ast as ir;

use super::super::models::{ConvertedSubgraph, DAG, DAGEdge, DagConversionError};
use super::super::nodes::{InputNode, OutputNode, ReturnNode};
use super::super::validate::validate_dag;

/// Convert IR programs into a DAG with control + data-flow edges.
///
/// Design overview:
/// - Each IR statement becomes one or more DAG nodes (assignments, calls, joins, etc).
/// - State-machine edges encode control flow, including branches, loops, and exceptions.
/// - Data-flow edges link variable definitions to later uses so scheduling and replay
///   can trace dependencies.
/// - Function calls are expanded by cloning callee nodes with a stable prefix, then
///   wiring caller -> callee arguments via synthetic assignments.
///
/// Example IR:
/// - results = parallel: @a() @b()
///   Yields a parallel node, two call nodes, and a join/aggregator node connected by
///   control edges, plus data-flow edges from each call's result into the aggregator.
pub struct DAGConverter {
    pub dag: DAG,
    pub node_counter: usize,
    pub current_function: Option<String>,
    pub function_defs: HashMap<String, ir::FunctionDef>,
    pub current_scope_vars: HashMap<String, String>,
    pub var_modifications: HashMap<String, Vec<String>>,
    pub loop_exit_stack: Vec<String>,
    pub loop_incr_stack: Vec<String>,
    pub try_depth: usize,
}

impl Default for DAGConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl DAGConverter {
    pub fn new() -> Self {
        Self {
            dag: DAG::default(),
            node_counter: 0,
            current_function: None,
            function_defs: HashMap::new(),
            current_scope_vars: HashMap::new(),
            var_modifications: HashMap::new(),
            loop_exit_stack: Vec::new(),
            loop_incr_stack: Vec::new(),
            try_depth: 0,
        }
    }

    /// Convert a full IR program into a flattened, executable DAG.
    ///
    /// This chooses an entry function, expands all reachable function calls,
    /// remaps exception edges to expanded call entries, adds global data-flow
    /// edges, and validates the resulting graph.
    ///
    /// Example entry selection:
    /// - If a function named "main" exists, it is used as the entry point.
    /// - Otherwise, the first non-dunder function becomes the entry.
    pub fn convert(&mut self, program: &ir::Program) -> Result<DAG, DagConversionError> {
        let unexpanded = self.convert_with_pointers(program)?;

        let mut entry_fn: Option<String> = None;
        for func in &program.functions {
            if func.name == "main" {
                entry_fn = Some(func.name.clone());
                break;
            }
        }
        if entry_fn.is_none() {
            for func in &program.functions {
                if !func.name.starts_with("__") {
                    entry_fn = Some(func.name.clone());
                    break;
                }
            }
        }
        if entry_fn.is_none() {
            entry_fn = program.functions.first().map(|func| func.name.clone());
        }
        let entry_fn = entry_fn.unwrap_or_else(|| "main".to_string());

        let mut dag = self.expand_functions(&unexpanded, &entry_fn)?;
        self.remap_exception_targets(&mut dag);
        self.add_global_data_flow_edges(&mut dag);
        validate_dag(&dag)?;
        Ok(dag)
    }

    /// Convert each function into its own DAG fragment without inlining calls.
    ///
    /// The resulting graph preserves per-function node ids and keeps function
    /// calls as call nodes. This is primarily used as the input to
    /// expand_functions, which will inline and prefix the callee graphs.
    pub fn convert_with_pointers(
        &mut self,
        program: &ir::Program,
    ) -> Result<DAG, DagConversionError> {
        self.dag = DAG::default();
        self.node_counter = 0;
        self.function_defs.clear();

        for func in &program.functions {
            self.function_defs.insert(func.name.clone(), func.clone());
        }

        for func in &program.functions {
            self.convert_function(func)?;
        }

        let dag = self.dag.clone();
        self.dag = DAG::default();
        Ok(dag)
    }

    /// Convert a single function body into a DAG fragment.
    ///
    /// This creates input/output nodes, converts each statement in order, and
    /// adds per-function data-flow edges based on variable definitions.
    ///
    /// Example IR:
    /// - def main(x): y = x + 1; return y
    ///   Produces input -> assignment -> return -> output with x/y data edges.
    pub fn convert_function(&mut self, fn_def: &ir::FunctionDef) -> Result<(), DagConversionError> {
        self.current_function = Some(fn_def.name.clone());
        self.current_scope_vars.clear();
        self.var_modifications.clear();

        let io = fn_def.io.clone().unwrap_or_default();

        let input_id = self.next_id(&format!("{}_input", fn_def.name));
        let input_node = InputNode::new(
            input_id.clone(),
            io.inputs.clone(),
            Some(fn_def.name.clone()),
        );
        self.dag.add_node(input_node.into());

        for var in &io.inputs {
            self.track_var_definition(var, &input_id);
        }

        let mut frontier = vec![input_id.clone()];
        let body = fn_def.body.clone().unwrap_or_default();
        for stmt in &body.statements {
            let converted = self.convert_statement(stmt)?;
            if converted.is_noop {
                continue;
            }
            if let Some(entry) = &converted.entry {
                for prev in &frontier {
                    self.dag.add_edge(DAGEdge::state_machine(prev, entry));
                }
            }
            frontier = converted.exits.clone();
        }

        let output_id = self.next_id(&format!("{}_output", fn_def.name));
        let output_node = OutputNode::new(
            output_id.clone(),
            io.outputs.clone(),
            Some(fn_def.name.clone()),
        );
        self.dag.add_node(output_node.into());

        for prev in &frontier {
            self.dag.add_edge(DAGEdge::state_machine(prev, &output_id));
        }

        let return_nodes: Vec<String> = self
            .dag
            .nodes
            .iter()
            .filter(|(_, node)| {
                node.node_type() == "return" && node.function_name() == Some(fn_def.name.as_str())
            })
            .map(|(node_id, _)| node_id.clone())
            .collect();

        for return_id in return_nodes {
            let already_connected = self
                .dag
                .edges
                .iter()
                .any(|edge| edge.source == return_id && edge.target == output_id);
            if !already_connected {
                self.dag
                    .add_edge(DAGEdge::state_machine(return_id, &output_id));
            }
        }

        self.add_data_flow_edges_for_function(&fn_def.name);
        self.current_function = None;
        Ok(())
    }

    /// Generate a stable node id for the current conversion session.
    pub fn next_id(&mut self, prefix: &str) -> String {
        self.node_counter += 1;
        format!("{prefix}_{}", self.node_counter)
    }

    /// Convert a block into a connected subgraph.
    ///
    /// This stitches statement graphs together with state-machine edges and
    /// returns entry/exits for the caller to connect.
    pub fn convert_block(
        &mut self,
        block: &ir::Block,
    ) -> Result<ConvertedSubgraph, DagConversionError> {
        let mut nodes: Vec<String> = Vec::new();
        let mut entry: Option<String> = None;
        let mut frontier: Option<Vec<String>> = None;

        for stmt in &block.statements {
            let converted = self.convert_statement(stmt)?;
            nodes.extend(converted.nodes.clone());

            if converted.is_noop {
                continue;
            }

            if entry.is_none() {
                entry = converted.entry.clone();
            }

            if let (Some(prevs), Some(next_entry)) = (frontier.as_ref(), converted.entry.as_ref()) {
                for prev in prevs {
                    self.dag.add_edge(DAGEdge::state_machine(prev, next_entry));
                }
            }

            frontier = Some(converted.exits.clone());
        }

        if entry.is_none() {
            return Ok(ConvertedSubgraph::noop());
        }

        Ok(ConvertedSubgraph {
            entry,
            exits: frontier.unwrap_or_default(),
            nodes,
            is_noop: false,
        })
    }

    /// Convert a single statement into a subgraph with entry/exit nodes.
    pub fn convert_statement(
        &mut self,
        stmt: &ir::Statement,
    ) -> Result<ConvertedSubgraph, DagConversionError> {
        let kind = stmt.kind.as_ref();
        match kind {
            Some(ir::statement::Kind::Assignment(assign)) => {
                let node_ids = self.convert_assignment(assign);
                self.wrap_node_ids(node_ids)
            }
            Some(ir::statement::Kind::ActionCall(action)) => {
                let node_ids = self.convert_action_call_with_targets(action, &[]);
                self.wrap_node_ids(node_ids)
            }
            Some(ir::statement::Kind::SpreadAction(spread)) => {
                let node_ids = self.convert_spread_action(spread);
                self.wrap_node_ids(node_ids)
            }
            Some(ir::statement::Kind::ParallelBlock(parallel)) => {
                let node_ids = self.convert_parallel_block(parallel);
                self.wrap_node_ids(node_ids)
            }
            Some(ir::statement::Kind::ForLoop(loop_stmt)) => self.convert_for_loop(loop_stmt),
            Some(ir::statement::Kind::WhileLoop(loop_stmt)) => self.convert_while_loop(loop_stmt),
            Some(ir::statement::Kind::Conditional(cond)) => self.convert_conditional(cond),
            Some(ir::statement::Kind::TryExcept(try_except)) => self.convert_try_except(try_except),
            Some(ir::statement::Kind::ReturnStmt(ret)) => {
                let node_ids = self.convert_return(ret);
                if node_ids.is_empty() {
                    return Ok(ConvertedSubgraph::noop());
                }
                Ok(ConvertedSubgraph {
                    entry: Some(node_ids[0].clone()),
                    exits: Vec::new(),
                    nodes: node_ids,
                    is_noop: false,
                })
            }
            Some(ir::statement::Kind::BreakStmt(_)) => self.convert_break(),
            Some(ir::statement::Kind::ContinueStmt(_)) => self.convert_continue(),
            Some(ir::statement::Kind::ExprStmt(expr_stmt)) => {
                let node_ids = self.convert_expr_statement(expr_stmt);
                self.wrap_node_ids(node_ids)
            }
            Some(ir::statement::Kind::SleepStmt(sleep_stmt)) => {
                let node_ids = self.convert_sleep(sleep_stmt);
                self.wrap_node_ids(node_ids)
            }
            None => Ok(ConvertedSubgraph::noop()),
        }
    }

    fn wrap_node_ids(
        &self,
        node_ids: Vec<String>,
    ) -> Result<ConvertedSubgraph, DagConversionError> {
        if node_ids.is_empty() {
            return Ok(ConvertedSubgraph::noop());
        }
        Ok(ConvertedSubgraph {
            entry: Some(node_ids[0].clone()),
            exits: vec![node_ids[node_ids.len() - 1].clone()],
            nodes: node_ids,
            is_noop: false,
        })
    }

    /// Convert a return statement into a ReturnNode.
    pub fn convert_return(&mut self, ret: &ir::ReturnStmt) -> Vec<String> {
        let node_id = self.next_id("return");
        let mut node = ReturnNode::new(
            node_id.clone(),
            None,
            None,
            None,
            self.current_function.clone(),
        );
        if let Some(value) = &ret.value {
            node.assign_expr = Some(value.clone());
            node.target = Some("result".to_string());
        }
        let has_target = node.target.is_some();
        self.dag.add_node(node.into());
        if has_target {
            self.track_var_definition("result", &node_id);
        }
        vec![node_id]
    }
}

pub fn convert_to_dag(program: &ir::Program) -> Result<DAG, DagConversionError> {
    DAGConverter::new().convert(program)
}

#[cfg(test)]
mod tests {
    use super::super::test_helpers::{build_dag, parse_program_source};
    use super::*;

    #[test]
    fn test_convert_with_pointers_keeps_function_call_node() {
        let program = parse_program_source(
            r#"
            fn helper(input: [x], output: [y]):
                y = x + 1
                return y

            fn main(input: [a], output: [b]):
                b = helper(a)
                return b
            "#,
        );

        let mut converter = DAGConverter::new();
        let dag = converter
            .convert_with_pointers(&program)
            .expect("convert with pointers");

        assert!(
            dag.nodes.values().any(|node| node.node_type() == "fn_call"),
            "convert_with_pointers should preserve fn_call nodes"
        );
    }

    #[test]
    fn test_convert_to_dag_happy_path_expands_function_calls() {
        let dag = build_dag(
            r#"
            fn helper(input: [x], output: [y]):
                y = x + 1
                return y

            fn main(input: [a], output: [b]):
                b = helper(a)
                return b
            "#,
        );

        assert!(
            dag.nodes.values().all(|node| node.node_type() != "fn_call"),
            "convert_to_dag should inline function calls in the expanded DAG"
        );
        assert!(
            !dag.edges.is_empty(),
            "expanded DAG should contain state/data-flow edges"
        );
    }
}
