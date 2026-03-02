//! Conditional conversion helpers.

use waymark_proto::ast as ir;

use super::super::models::{ConvertedSubgraph, DAGEdge, DagConversionError};
use super::super::nodes::{BranchNode, JoinNode};
use super::converter::DAGConverter;

/// Convert conditional blocks into branch/join nodes.
impl DAGConverter {
    /// Convert an if/elif/else tree into a branch + optional join graph.
    pub fn convert_conditional(
        &mut self,
        cond: &ir::Conditional,
    ) -> Result<ConvertedSubgraph, DagConversionError> {
        let mut nodes: Vec<String> = Vec::new();

        let branch_id = self.next_id("branch");
        let branch_node =
            BranchNode::new(branch_id.clone(), "branch", self.current_function.clone());
        self.dag.add_node(branch_node.into());
        nodes.push(branch_id.clone());

        let if_branch = cond
            .if_branch
            .as_ref()
            .ok_or_else(|| DagConversionError("conditional missing if_branch".to_string()))?;
        let if_guard = if_branch
            .condition
            .as_ref()
            .ok_or_else(|| DagConversionError("if branch missing guard expression".to_string()))?
            .clone();

        let if_body_graph = if let Some(block) = &if_branch.block_body {
            self.convert_block(block)?
        } else {
            ConvertedSubgraph::noop()
        };
        nodes.extend(if_body_graph.nodes.clone());

        let mut prior_guards = vec![if_guard.clone()];
        let mut elif_graphs: Vec<(ir::Expr, ConvertedSubgraph)> = Vec::new();

        for elif_branch in &cond.elif_branches {
            let elif_cond = elif_branch
                .condition
                .as_ref()
                .ok_or_else(|| {
                    DagConversionError("elif branch missing guard expression".to_string())
                })?
                .clone();
            let compound_guard = self.build_compound_guard(&prior_guards, Some(&elif_cond))?;
            prior_guards.push(elif_cond);

            let graph = if let Some(block) = &elif_branch.block_body {
                self.convert_block(block)?
            } else {
                ConvertedSubgraph::noop()
            };
            nodes.extend(graph.nodes.clone());
            elif_graphs.push((compound_guard, graph));
        }

        let else_graph = if let Some(else_branch) = &cond.else_branch {
            if let Some(block) = &else_branch.block_body {
                self.convert_block(block)?
            } else {
                ConvertedSubgraph::noop()
            }
        } else {
            ConvertedSubgraph::noop()
        };
        nodes.extend(else_graph.nodes.clone());

        let join_needed = if_body_graph.is_noop
            || !if_body_graph.exits.is_empty()
            || elif_graphs
                .iter()
                .any(|(_, graph)| graph.is_noop || !graph.exits.is_empty())
            || else_graph.is_noop
            || !else_graph.exits.is_empty();

        let mut join_id: Option<String> = None;
        if join_needed {
            let join_node_id = self.next_id("join");
            let join_node = JoinNode::new(
                join_node_id.clone(),
                "join",
                None,
                None,
                self.current_function.clone(),
            );
            self.dag.add_node(join_node.into());
            nodes.push(join_node_id.clone());
            join_id = Some(join_node_id);
        }

        let connect_guarded_branch =
            |converter: &mut DAGConverter,
             guard: ir::Expr,
             graph: ConvertedSubgraph,
             join_target: Option<&String>| {
                if graph.is_noop {
                    if let Some(join) = join_target {
                        converter
                            .dag
                            .add_edge(DAGEdge::state_machine_with_guard(&branch_id, join, guard));
                    }
                    return;
                }
                if let Some(entry) = graph.entry.as_ref() {
                    converter
                        .dag
                        .add_edge(DAGEdge::state_machine_with_guard(&branch_id, entry, guard));
                }
                if let Some(join) = join_target {
                    for exit_node in &graph.exits {
                        converter
                            .dag
                            .add_edge(DAGEdge::state_machine(exit_node, join));
                    }
                }
            };

        let connect_else_branch = |converter: &mut DAGConverter,
                                   graph: ConvertedSubgraph,
                                   join_target: Option<&String>| {
            if graph.is_noop {
                if let Some(join) = join_target {
                    converter
                        .dag
                        .add_edge(DAGEdge::state_machine_else(&branch_id, join));
                }
                return;
            }
            if let Some(entry) = graph.entry.as_ref() {
                converter
                    .dag
                    .add_edge(DAGEdge::state_machine_else(&branch_id, entry));
            }
            if let Some(join) = join_target {
                for exit_node in &graph.exits {
                    converter
                        .dag
                        .add_edge(DAGEdge::state_machine(exit_node, join));
                }
            }
        };

        connect_guarded_branch(self, if_guard, if_body_graph, join_id.as_ref());
        for (guard, graph) in elif_graphs {
            connect_guarded_branch(self, guard, graph, join_id.as_ref());
        }
        connect_else_branch(self, else_graph, join_id.as_ref());

        Ok(ConvertedSubgraph {
            entry: Some(branch_id),
            exits: join_id.into_iter().collect(),
            nodes,
            is_noop: false,
        })
    }

    /// Build a guard for elif branches: not prior_guards and current_condition.
    pub fn build_compound_guard(
        &self,
        prior_guards: &[ir::Expr],
        current_condition: Option<&ir::Expr>,
    ) -> Result<ir::Expr, DagConversionError> {
        let mut parts: Vec<ir::Expr> = Vec::new();
        for guard in prior_guards {
            let expr = ir::Expr {
                kind: Some(ir::expr::Kind::UnaryOp(Box::new(ir::UnaryOp {
                    op: ir::UnaryOperator::UnaryOpNot as i32,
                    operand: Some(Box::new(guard.clone())),
                }))),
                span: None,
            };
            parts.push(expr);
        }

        if let Some(condition) = current_condition {
            parts.push(condition.clone());
        }

        if parts.is_empty() {
            return Err(DagConversionError(
                "build_compound_guard called with no prior conditions and no current condition"
                    .to_string(),
            ));
        }
        if parts.len() == 1 {
            return Ok(parts.remove(0));
        }

        let mut result = parts.remove(0);
        for part in parts {
            result = ir::Expr {
                kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                    left: Some(Box::new(result)),
                    op: ir::BinaryOperator::BinaryOpAnd as i32,
                    right: Some(Box::new(part)),
                }))),
                span: None,
            };
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_helpers::build_dag_with_pointers;

    #[test]
    fn test_convert_conditional_happy_path() {
        let dag = build_dag_with_pointers(
            r#"
            fn main(input: [x], output: [y]):
                if x > 0:
                    y = 1
                else:
                    y = 2
                return y
            "#,
        );

        assert!(
            dag.nodes.values().any(|node| node.node_type() == "branch"),
            "conditional should create a branch node"
        );
        assert!(
            dag.nodes.values().any(|node| node.node_type() == "join"),
            "conditional should create a join node"
        );
    }
}
