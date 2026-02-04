//! Shared helpers for DAG conversion.

use crate::messages::ast as ir;

use super::super::nodes::{
    ActionCallNode, AggregatorNode, AssignmentNode, DAGNode, FnCallNode, JoinNode, ReturnNode,
};
use super::converter::DAGConverter;

impl DAGConverter {
    pub fn track_var_definition(&mut self, var_name: &str, node_id: &str) {
        self.current_scope_vars
            .insert(var_name.to_string(), node_id.to_string());
        self.var_modifications
            .entry(var_name.to_string())
            .or_default()
            .push(node_id.to_string());
    }

    pub fn push_unique_target(targets: &mut Vec<String>, target: &str) {
        if !targets.iter().any(|existing| existing == target) {
            targets.push(target.to_string());
        }
    }

    pub fn targets_for_node(node: &DAGNode) -> Vec<String> {
        match node {
            DAGNode::Assignment(AssignmentNode {
                targets, target, ..
            }) => {
                if !targets.is_empty() {
                    return targets.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::ActionCall(ActionCallNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::FnCall(FnCallNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::Aggregator(AggregatorNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::Return(ReturnNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::Join(JoinNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    pub fn collect_assigned_targets(statements: &[ir::Statement], targets: &mut Vec<String>) {
        for stmt in statements {
            let kind = stmt.kind.as_ref();
            match kind {
                Some(ir::statement::Kind::Assignment(assign)) => {
                    for target in &assign.targets {
                        Self::push_unique_target(targets, target);
                    }
                }
                Some(ir::statement::Kind::Conditional(cond)) => {
                    if let Some(branch) = &cond.if_branch
                        && let Some(block) = &branch.block_body
                    {
                        Self::collect_assigned_targets(&block.statements, targets);
                    }
                    for elif_branch in &cond.elif_branches {
                        if let Some(block) = &elif_branch.block_body {
                            Self::collect_assigned_targets(&block.statements, targets);
                        }
                    }
                    if let Some(else_branch) = &cond.else_branch
                        && let Some(block) = &else_branch.block_body
                    {
                        Self::collect_assigned_targets(&block.statements, targets);
                    }
                }
                Some(ir::statement::Kind::ForLoop(loop_stmt)) => {
                    if let Some(block) = &loop_stmt.block_body {
                        Self::collect_assigned_targets(&block.statements, targets);
                    }
                }
                Some(ir::statement::Kind::WhileLoop(loop_stmt)) => {
                    if let Some(block) = &loop_stmt.block_body {
                        Self::collect_assigned_targets(&block.statements, targets);
                    }
                }
                Some(ir::statement::Kind::TryExcept(try_except)) => {
                    if let Some(block) = &try_except.try_block {
                        Self::collect_assigned_targets(&block.statements, targets);
                    }
                    for handler in &try_except.handlers {
                        if let Some(block) = &handler.block_body {
                            Self::collect_assigned_targets(&block.statements, targets);
                        }
                    }
                }
                Some(
                    ir::statement::Kind::ParallelBlock(_)
                    | ir::statement::Kind::SpreadAction(_)
                    | ir::statement::Kind::ActionCall(_)
                    | ir::statement::Kind::ReturnStmt(_)
                    | ir::statement::Kind::BreakStmt(_)
                    | ir::statement::Kind::ContinueStmt(_)
                    | ir::statement::Kind::ExprStmt(_),
                )
                | None => {
                    continue;
                }
            }
        }
    }
}
