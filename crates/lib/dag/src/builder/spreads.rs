//! Spread/parallel conversion helpers.

use waymark_proto::ast as ir;

use super::super::models::DAGEdge;
use super::super::nodes::{
    ActionCallNode, ActionCallParams, AggregatorNode, FnCallNode, FnCallParams, ParallelNode,
};
use super::converter::DAGConverter;

/// Convert spread and parallel constructs into DAG nodes.
impl DAGConverter {
    /// Convert a spread action without explicit targets.
    pub fn convert_spread_action(&mut self, spread: &ir::SpreadAction) -> Vec<String> {
        self.convert_spread_action_with_targets(spread, &[])
    }

    /// Convert a spread expression into an action + aggregator pair.
    ///
    /// Example IR:
    /// - results = spread items: @do(item)
    ///   Produces a spread action node (one per item at runtime) and an
    ///   aggregator node that collects the spread results into results.
    pub fn convert_spread_expr(
        &mut self,
        spread: &ir::SpreadExpr,
        targets: &[String],
    ) -> Vec<String> {
        let action = match spread.action.as_ref() {
            Some(action) => action.clone(),
            None => return Vec::new(),
        };
        let action_id = self.next_id("spread_action");
        let kwargs = self.extract_kwargs(&action.kwargs);
        let kwarg_exprs = self.extract_kwarg_exprs(&action.kwargs);
        let collection_expr = spread.collection.as_deref().cloned().unwrap_or(ir::Expr {
            kind: None,
            span: None,
        });
        let spread_result_var = "_spread_result".to_string();

        let agg_id = self.next_id("aggregator");
        let module_name = action.module_name.clone();
        let action_node = ActionCallNode::new(
            action_id.clone(),
            action.action_name.clone(),
            ActionCallParams {
                module_name,
                kwargs,
                kwarg_exprs,
                policies: action.policies.clone(),
                targets: None,
                target: Some(spread_result_var.clone()),
                parallel_index: None,
                aggregates_to: Some(agg_id.clone()),
                spread_loop_var: Some(spread.loop_var.clone()),
                spread_collection_expr: Some(collection_expr),
                function_name: self.current_function.clone(),
            },
        );
        self.dag.add_node(action_node.into());

        let agg_node = AggregatorNode::new(
            agg_id.clone(),
            action_id.clone(),
            if targets.is_empty() {
                None
            } else {
                Some(targets.to_vec())
            },
            None,
            "aggregate",
            self.current_function.clone(),
        );
        self.dag.add_node(agg_node.into());

        self.dag
            .add_edge(DAGEdge::state_machine(&action_id, &agg_id));
        self.dag
            .add_edge(DAGEdge::data_flow(&action_id, &agg_id, spread_result_var));

        for target in targets {
            self.track_var_definition(target, &agg_id);
        }

        vec![action_id, agg_id]
    }

    /// Convert a spread action statement into action + aggregator nodes.
    pub fn convert_spread_action_with_targets(
        &mut self,
        spread: &ir::SpreadAction,
        targets: &[String],
    ) -> Vec<String> {
        let action = match spread.action.as_ref() {
            Some(action) => action.clone(),
            None => return Vec::new(),
        };
        let action_id = self.next_id("spread_action");
        let kwargs = self.extract_kwargs(&action.kwargs);
        let kwarg_exprs = self.extract_kwarg_exprs(&action.kwargs);
        let collection_expr = spread.collection.as_ref().cloned().unwrap_or(ir::Expr {
            kind: None,
            span: None,
        });
        let spread_result_var = "_spread_result".to_string();

        let agg_id = self.next_id("aggregator");
        let module_name = action.module_name.clone();
        let action_node = ActionCallNode::new(
            action_id.clone(),
            action.action_name.clone(),
            ActionCallParams {
                module_name,
                kwargs,
                kwarg_exprs,
                policies: action.policies.clone(),
                targets: None,
                target: Some(spread_result_var.clone()),
                parallel_index: None,
                aggregates_to: Some(agg_id.clone()),
                spread_loop_var: Some(spread.loop_var.clone()),
                spread_collection_expr: Some(collection_expr),
                function_name: self.current_function.clone(),
            },
        );
        self.dag.add_node(action_node.into());

        let agg_node = AggregatorNode::new(
            agg_id.clone(),
            action_id.clone(),
            if targets.is_empty() {
                None
            } else {
                Some(targets.to_vec())
            },
            None,
            "aggregate",
            self.current_function.clone(),
        );
        self.dag.add_node(agg_node.into());

        self.dag
            .add_edge(DAGEdge::state_machine(&action_id, &agg_id));
        self.dag
            .add_edge(DAGEdge::data_flow(&action_id, &agg_id, spread_result_var));

        for target in targets {
            self.track_var_definition(target, &agg_id);
        }

        vec![action_id, agg_id]
    }

    /// Convert a parallel block statement without assignment targets.
    pub fn convert_parallel_block(&mut self, parallel: &ir::ParallelBlock) -> Vec<String> {
        self.convert_parallel_block_with_targets(&parallel.calls, &[])
    }

    /// Convert a parallel expression with assignment targets.
    pub fn convert_parallel_expr(
        &mut self,
        parallel: &ir::ParallelExpr,
        targets: &[String],
    ) -> Vec<String> {
        self.convert_parallel_block_with_targets(&parallel.calls, targets)
    }

    /// Convert a parallel block/expression into a parallel node + calls + join.
    ///
    /// Example IR:
    /// - a, b = parallel: @x() @y()
    ///   Produces a parallel node, two call nodes, and an aggregator/join node.
    pub fn convert_parallel_block_with_targets(
        &mut self,
        calls: &[ir::Call],
        targets: &[String],
    ) -> Vec<String> {
        let mut result_nodes: Vec<String> = Vec::new();

        let parallel_id = self.next_id("parallel");
        let parallel_node = ParallelNode::new(parallel_id.clone(), self.current_function.clone());
        self.dag.add_node(parallel_node.into());
        result_nodes.push(parallel_id.clone());

        let list_aggregate = targets.len() == 1;
        let agg_id = self.next_id("parallel_aggregator");
        let has_fn_calls = calls
            .iter()
            .any(|call| matches!(call.kind.as_ref(), Some(ir::call::Kind::Function(_))));

        let mut call_node_ids: Vec<String> = Vec::new();
        for (idx, call) in calls.iter().enumerate() {
            let call_target = if list_aggregate {
                None
            } else if idx < targets.len() {
                Some(targets[idx].clone())
            } else {
                None
            };
            let kind = call.kind.as_ref();
            let node_id = match kind {
                Some(ir::call::Kind::Action(action)) => {
                    let call_id = self.next_id("parallel_action");
                    let kwargs = self.extract_kwargs(&action.kwargs);
                    let kwarg_exprs = self.extract_kwarg_exprs(&action.kwargs);
                    let module_name = action.module_name.clone();
                    let node = ActionCallNode::new(
                        call_id.clone(),
                        action.action_name.clone(),
                        ActionCallParams {
                            module_name,
                            kwargs,
                            kwarg_exprs,
                            policies: action.policies.clone(),
                            targets: None,
                            target: call_target.clone(),
                            parallel_index: Some(idx as i32),
                            aggregates_to: if list_aggregate {
                                Some(agg_id.clone())
                            } else {
                                None
                            },
                            spread_loop_var: None,
                            spread_collection_expr: None,
                            function_name: self.current_function.clone(),
                        },
                    );
                    self.dag.add_node(node.into());
                    call_id
                }
                Some(ir::call::Kind::Function(func)) => {
                    let call_id = self.next_id("parallel_fn_call");
                    let (kwargs, kwarg_exprs) = self.extract_fn_call_args(func);
                    let node = FnCallNode::new(
                        call_id.clone(),
                        func.name.clone(),
                        FnCallParams {
                            kwargs,
                            kwarg_exprs,
                            targets: None,
                            target: call_target.clone(),
                            assign_expr: None,
                            parallel_index: Some(idx as i32),
                            aggregates_to: if list_aggregate {
                                Some(agg_id.clone())
                            } else {
                                None
                            },
                            function_name: self.current_function.clone(),
                        },
                    );
                    self.dag.add_node(node.into());
                    call_id
                }
                None => continue,
            };

            if let Some(target) = &call_target {
                self.track_var_definition(target, &node_id);
            }

            call_node_ids.push(node_id.clone());
            result_nodes.push(node_id.clone());

            self.dag.add_edge(DAGEdge::state_machine_with_condition(
                &parallel_id,
                &node_id,
                format!("parallel:{idx}"),
            ));
        }

        let aggregator_targets = if has_fn_calls {
            Vec::new()
        } else {
            targets.to_vec()
        };
        let agg_node = AggregatorNode::new(
            agg_id.clone(),
            parallel_id.clone(),
            if aggregator_targets.is_empty() {
                None
            } else {
                Some(aggregator_targets)
            },
            None,
            "parallel",
            self.current_function.clone(),
        );
        self.dag.add_node(agg_node.into());
        result_nodes.push(agg_id.clone());

        for call_id in &call_node_ids {
            self.dag.add_edge(DAGEdge::state_machine(call_id, &agg_id));
        }

        result_nodes
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_helpers::build_dag_with_pointers;

    #[test]
    fn test_convert_spread_expr_happy_path() {
        let dag = build_dag_with_pointers(
            r#"
            fn main(input: [], output: [results]):
                results = spread [1, 2]: item -> @noop()
                return results
            "#,
        );

        assert!(
            dag.nodes
                .values()
                .any(|node| node.node_type() == "action_call"),
            "spread expression should create action call node"
        );
        assert!(
            dag.nodes
                .values()
                .any(|node| node.node_type() == "aggregator"),
            "spread expression should create aggregator node"
        );
    }
}
