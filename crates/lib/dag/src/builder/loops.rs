//! Loop conversion helpers.

use waymark_proto::ast as ir;

use super::super::models::{ConvertedSubgraph, DAGEdge, DagConversionError};
use super::super::nodes::{AssignmentNode, BranchNode, BreakNode, ContinueNode, JoinNode};
use super::converter::DAGConverter;

/// Convert loop constructs into explicit DAG nodes.
impl DAGConverter {
    /// Build a guard expression for loop continuation based on collection length.
    ///
    /// Example:
    /// - loop_i_var="__loop_1_i", collection=items
    ///   guard becomes "__loop_1_i < len(items)"
    pub fn build_loop_guard(loop_i_var: &str, collection: Option<&ir::Expr>) -> Option<ir::Expr> {
        let collection = collection?.clone();
        let len_call = ir::FunctionCall {
            name: "len".to_string(),
            args: Vec::new(),
            kwargs: vec![ir::Kwarg {
                name: "items".to_string(),
                value: Some(collection),
            }],
            global_function: ir::GlobalFunction::Len as i32,
        };
        Some(ir::Expr {
            kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                left: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Variable(ir::Variable {
                        name: loop_i_var.to_string(),
                    })),
                    span: None,
                })),
                op: ir::BinaryOperator::BinaryOpLt as i32,
                right: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::FunctionCall(len_call)),
                    span: None,
                })),
            }))),
            span: None,
        })
    }

    /// Convert a for-loop into explicit loop init/cond/body/incr/exit nodes.
    ///
    /// Example IR:
    /// - for item in items: @work(item)
    ///   Expands into init (i = 0), cond, extract, body, incr, and loop join.
    pub fn convert_for_loop(
        &mut self,
        for_loop: &ir::ForLoop,
    ) -> Result<ConvertedSubgraph, DagConversionError> {
        let mut nodes: Vec<String> = Vec::new();

        let loop_id = self.next_id("loop");
        let loop_vars_str = for_loop.loop_vars.join(", ");
        let collection_expr = for_loop.iterable.as_ref().cloned();
        let collection_str = collection_expr
            .as_ref()
            .map(|expr| self.expr_to_string(expr))
            .unwrap_or_default();

        let loop_i_var = format!("__loop_{loop_id}_i");
        let continue_guard = Self::build_loop_guard(&loop_i_var, collection_expr.as_ref());
        let break_guard = continue_guard.as_ref().map(|guard| ir::Expr {
            kind: Some(ir::expr::Kind::UnaryOp(Box::new(ir::UnaryOp {
                op: ir::UnaryOperator::UnaryOpNot as i32,
                operand: Some(Box::new(guard.clone())),
            }))),
            span: None,
        });

        let init_id = self.next_id("loop_init");
        let init_label = format!("{loop_i_var} = 0");
        let init_expr = ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IntValue(0)),
            })),
            span: None,
        };
        let init_node = AssignmentNode::new(
            init_id.clone(),
            vec![loop_i_var.clone()],
            None,
            Some(init_expr),
            Some(init_label),
            self.current_function.clone(),
        );
        self.dag.add_node(init_node.into());
        self.track_var_definition(&loop_i_var, &init_id);
        nodes.push(init_id.clone());

        let cond_id = self.next_id("loop_cond");
        let cond_label = format!("for {loop_vars_str} in {collection_str}");
        let cond_node = BranchNode::new(cond_id.clone(), cond_label, self.current_function.clone());
        self.dag.add_node(cond_node.into());
        nodes.push(cond_id.clone());

        self.dag
            .add_edge(DAGEdge::state_machine(&init_id, &cond_id));

        let extract_id = self.next_id("loop_extract");
        let collection_expr = collection_expr.ok_or_else(|| {
            DagConversionError(format!(
                "for-loop collection expression is None for loop '{loop_vars_str}'"
            ))
        })?;
        let index_expr = ir::Expr {
            kind: Some(ir::expr::Kind::Index(Box::new(ir::IndexAccess {
                object: Some(Box::new(collection_expr.clone())),
                index: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Variable(ir::Variable {
                        name: loop_i_var.clone(),
                    })),
                    span: None,
                })),
            }))),
            span: None,
        };
        let extract_label = format!("{loop_vars_str} = {collection_str}[{loop_i_var}]");
        let extract_node = AssignmentNode::new(
            extract_id.clone(),
            for_loop.loop_vars.clone(),
            None,
            Some(index_expr),
            Some(extract_label),
            self.current_function.clone(),
        );
        self.dag.add_node(extract_node.into());
        nodes.push(extract_id.clone());

        for loop_var in &for_loop.loop_vars {
            self.track_var_definition(loop_var, &extract_id);
        }

        if let Some(guard) = continue_guard.clone() {
            self.dag.add_edge(DAGEdge::state_machine_with_guard(
                &cond_id,
                &extract_id,
                guard,
            ));
        } else {
            self.dag
                .add_edge(DAGEdge::state_machine(&cond_id, &extract_id));
        }

        let exit_id = self.next_id("loop_exit");
        self.loop_exit_stack.push(exit_id.clone());
        let incr_id = self.next_id("loop_incr");
        self.loop_incr_stack.push(incr_id.clone());

        let mut body_targets: Vec<String> = Vec::new();
        let body_graph = if let Some(block) = &for_loop.block_body {
            Self::collect_assigned_targets(&block.statements, &mut body_targets);
            self.convert_block(block)?
        } else {
            ConvertedSubgraph::noop()
        };

        self.loop_incr_stack.pop();
        self.loop_exit_stack.pop();
        nodes.extend(body_graph.nodes.clone());

        if !body_graph.is_noop
            && let Some(entry) = &body_graph.entry
        {
            self.dag
                .add_edge(DAGEdge::state_machine(&extract_id, entry));
        }

        let incr_label = format!("{loop_i_var} = {loop_i_var} + 1");
        let incr_expr = ir::Expr {
            kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                left: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Variable(ir::Variable {
                        name: loop_i_var.clone(),
                    })),
                    span: None,
                })),
                op: ir::BinaryOperator::BinaryOpAdd as i32,
                right: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(1)),
                    })),
                    span: None,
                })),
            }))),
            span: None,
        };
        let incr_node = AssignmentNode::new(
            incr_id.clone(),
            vec![loop_i_var.clone()],
            None,
            Some(incr_expr),
            Some(incr_label),
            self.current_function.clone(),
        );
        self.dag.add_node(incr_node.into());
        self.track_var_definition(&loop_i_var, &incr_id);
        nodes.push(incr_id.clone());

        if body_graph.is_noop {
            self.dag
                .add_edge(DAGEdge::state_machine(&extract_id, &incr_id));
        } else {
            for exit_node in &body_graph.exits {
                self.dag
                    .add_edge(DAGEdge::state_machine(exit_node, &incr_id));
            }
        }

        self.dag
            .add_edge(DAGEdge::state_machine(&incr_id, &cond_id).with_loop_back(true));

        let exit_label = format!("end for {loop_vars_str}");
        let mut exit_node = JoinNode::new(
            exit_id.clone(),
            exit_label,
            None,
            None,
            self.current_function.clone(),
        );
        if !body_targets.is_empty() {
            exit_node.targets = Some(body_targets.clone());
            exit_node.target = body_targets.first().cloned();
            for target in &body_targets {
                self.track_var_definition(target, &exit_id);
            }
        }
        self.dag.add_node(exit_node.into());
        nodes.push(exit_id.clone());

        if let Some(guard) = break_guard {
            self.dag
                .add_edge(DAGEdge::state_machine_with_guard(&cond_id, &exit_id, guard));
        } else {
            self.dag
                .add_edge(DAGEdge::state_machine(&cond_id, &exit_id));
        }

        Ok(ConvertedSubgraph {
            entry: Some(init_id),
            exits: vec![exit_id],
            nodes,
            is_noop: false,
        })
    }

    /// Convert a while-loop into explicit condition/body/continue/exit nodes.
    pub fn convert_while_loop(
        &mut self,
        while_loop: &ir::WhileLoop,
    ) -> Result<ConvertedSubgraph, DagConversionError> {
        let mut nodes: Vec<String> = Vec::new();

        let condition = while_loop
            .condition
            .as_ref()
            .ok_or_else(|| DagConversionError("while loop missing condition".to_string()))?
            .clone();
        let condition_str = self.expr_to_string(&condition);

        let cond_id = self.next_id("loop_cond");
        let cond_label = format!("while {condition_str}");
        let cond_node = BranchNode::new(cond_id.clone(), cond_label, self.current_function.clone());
        self.dag.add_node(cond_node.into());
        nodes.push(cond_id.clone());

        let continue_guard = condition.clone();
        let break_guard = ir::Expr {
            kind: Some(ir::expr::Kind::UnaryOp(Box::new(ir::UnaryOp {
                op: ir::UnaryOperator::UnaryOpNot as i32,
                operand: Some(Box::new(condition)),
            }))),
            span: None,
        };

        let exit_id = self.next_id("loop_exit");
        self.loop_exit_stack.push(exit_id.clone());
        let continue_id = self.next_id("loop_continue");
        self.loop_incr_stack.push(continue_id.clone());

        let mut body_targets: Vec<String> = Vec::new();
        let body_graph = if let Some(block) = &while_loop.block_body {
            Self::collect_assigned_targets(&block.statements, &mut body_targets);
            self.convert_block(block)?
        } else {
            ConvertedSubgraph::noop()
        };

        self.loop_incr_stack.pop();
        self.loop_exit_stack.pop();
        nodes.extend(body_graph.nodes.clone());

        let continue_node = AssignmentNode::new(
            continue_id.clone(),
            Vec::new(),
            None,
            None,
            Some("loop_continue".to_string()),
            self.current_function.clone(),
        );
        self.dag.add_node(continue_node.into());
        nodes.push(continue_id.clone());

        if body_graph.is_noop {
            self.dag.add_edge(DAGEdge::state_machine_with_guard(
                &cond_id,
                &continue_id,
                continue_guard,
            ));
        } else if let Some(entry) = &body_graph.entry {
            self.dag.add_edge(DAGEdge::state_machine_with_guard(
                &cond_id,
                entry,
                continue_guard,
            ));
        }

        if !body_graph.is_noop {
            for exit_node in &body_graph.exits {
                self.dag
                    .add_edge(DAGEdge::state_machine(exit_node, &continue_id));
            }
        }

        self.dag
            .add_edge(DAGEdge::state_machine(&continue_id, &cond_id).with_loop_back(true));

        let exit_label = format!("end while {condition_str}");
        let mut exit_node = JoinNode::new(
            exit_id.clone(),
            exit_label,
            None,
            None,
            self.current_function.clone(),
        );
        if !body_targets.is_empty() {
            exit_node.targets = Some(body_targets.clone());
            exit_node.target = body_targets.first().cloned();
            for target in &body_targets {
                self.track_var_definition(target, &exit_id);
            }
        }
        self.dag.add_node(exit_node.into());
        nodes.push(exit_id.clone());

        self.dag.add_edge(DAGEdge::state_machine_with_guard(
            &cond_id,
            &exit_id,
            break_guard,
        ));

        Ok(ConvertedSubgraph {
            entry: Some(cond_id),
            exits: vec![exit_id],
            nodes,
            is_noop: false,
        })
    }

    /// Convert a break statement by wiring to the loop exit node.
    pub fn convert_break(&mut self) -> Result<ConvertedSubgraph, DagConversionError> {
        let loop_exit = self
            .loop_exit_stack
            .last()
            .cloned()
            .ok_or_else(|| DagConversionError("break statement outside of loop".to_string()))?;

        let node_id = self.next_id("break");
        let node = BreakNode::new(node_id.clone(), self.current_function.clone());
        self.dag.add_node(node.into());

        self.dag
            .add_edge(DAGEdge::state_machine(&node_id, loop_exit));

        Ok(ConvertedSubgraph {
            entry: Some(node_id.clone()),
            exits: Vec::new(),
            nodes: vec![node_id],
            is_noop: false,
        })
    }

    /// Convert a continue statement by wiring to the loop increment node.
    pub fn convert_continue(&mut self) -> Result<ConvertedSubgraph, DagConversionError> {
        let loop_incr =
            self.loop_incr_stack.last().cloned().ok_or_else(|| {
                DagConversionError("continue statement outside of loop".to_string())
            })?;

        let node_id = self.next_id("continue");
        let node = ContinueNode::new(node_id.clone(), self.current_function.clone());
        self.dag.add_node(node.into());

        self.dag
            .add_edge(DAGEdge::state_machine(&node_id, loop_incr));

        Ok(ConvertedSubgraph {
            entry: Some(node_id.clone()),
            exits: Vec::new(),
            nodes: vec![node_id],
            is_noop: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_helpers::build_dag_with_pointers;

    #[test]
    fn test_convert_for_loop_happy_path() {
        let dag = build_dag_with_pointers(
            r#"
            fn main(input: [items], output: [total]):
                total = 0
                for item in items:
                    total = total + item
                return total
            "#,
        );

        assert!(
            dag.nodes
                .keys()
                .any(|node_id| node_id.starts_with("loop_init")),
            "for loop should create loop init node"
        );
        assert!(
            dag.nodes
                .keys()
                .any(|node_id| node_id.starts_with("loop_cond")),
            "for loop should create loop condition node"
        );
        assert!(
            dag.nodes
                .keys()
                .any(|node_id| node_id.starts_with("loop_exit")),
            "for loop should create loop exit node"
        );
        assert!(
            dag.edges.iter().any(|edge| edge.is_loop_back),
            "for loop should emit at least one loop-back edge"
        );
    }
}
