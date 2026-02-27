//! Shared helpers for DAG conversion.

use waymark_proto::ast as ir;

use super::super::models::DAGNode;
use super::super::nodes::{
    ActionCallNode, AggregatorNode, AssignmentNode, FnCallNode, JoinNode, ReturnNode,
};
use super::converter::DAGConverter;

/// Utility helpers shared across conversion helpers.
impl DAGConverter {
    /// Record that a variable is defined at the given node.
    pub fn track_var_definition(&mut self, var_name: &str, node_id: &str) {
        self.current_scope_vars
            .insert(var_name.to_string(), node_id.to_string());
        self.var_modifications
            .entry(var_name.to_string())
            .or_default()
            .push(node_id.to_string());
    }

    /// Append a target if it is not already present.
    pub fn push_unique_target(targets: &mut Vec<String>, target: &str) {
        if !targets.iter().any(|existing| existing == target) {
            targets.push(target.to_string());
        }
    }

    /// Return assignment targets for nodes that bind variables.
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

    /// Collect assignment targets from a statement list, recursively.
    ///
    /// This is used to determine loop join node targets so data-flow edges
    /// can carry loop-carried variables out of the loop body.
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
                    | ir::statement::Kind::SleepStmt(_)
                    | ir::statement::Kind::ExprStmt(_),
                )
                | None => {
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::models::DAGNode;
    use super::super::super::nodes::AssignmentNode;
    use super::*;

    #[test]
    fn test_utils_happy_path_track_and_targets() {
        let mut converter = DAGConverter::new();
        converter.track_var_definition("value", "node_1");
        assert_eq!(
            converter
                .current_scope_vars
                .get("value")
                .map(String::as_str),
            Some("node_1")
        );

        let mut targets = vec!["value".to_string()];
        DAGConverter::push_unique_target(&mut targets, "value");
        DAGConverter::push_unique_target(&mut targets, "other");
        assert_eq!(targets, vec!["value".to_string(), "other".to_string()]);

        let assignment = DAGNode::Assignment(AssignmentNode::new(
            "assign_1",
            vec!["value".to_string()],
            None,
            None,
            None,
            Some("main".to_string()),
        ));
        assert_eq!(
            DAGConverter::targets_for_node(&assignment),
            vec!["value".to_string()]
        );
    }
}
