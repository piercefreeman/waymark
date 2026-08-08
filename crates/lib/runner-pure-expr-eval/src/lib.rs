//! An expression evaluator that does not allow side effects.

use std::collections::HashMap;

use waymark_proto::ast as ir;

/// The side effect applicator that does not allow side effect.
#[derive(Debug, Copy)]
pub struct PureApplicator<NodeId>(core::marker::PhantomData<NodeId>);

impl<NodeId> Clone for PureApplicator<NodeId> {
    fn clone(&self) -> Self {
        Self(core::marker::PhantomData)
    }
}

impl<NodeId> Default for PureApplicator<NodeId> {
    fn default() -> Self {
        Self(core::marker::PhantomData)
    }
}

/// Error returned when any side effect is applied.
#[derive(Debug, thiserror::Error)]
#[error("side effects are not allowed in guard expressions")]
pub struct SideEffectsNotAllowed;

impl<NodeId: Clone> waymark_runner_eval_core::SideEffectApplicator for PureApplicator<NodeId> {
    type ActionCallError = SideEffectsNotAllowed;
    type NodeId = NodeId;

    fn action_call(
        &mut self,
        _iteration_index: Option<i32>,
        _params: waymark_runner_eval_core::ActionCallParams<Self::NodeId>,
    ) -> Result<waymark_runner_expr::ActionResultValue<Self::NodeId>, Self::ActionCallError> {
        Err(SideEffectsNotAllowed)
    }
}

/// Evaluate the expression with [`PureApplicator`] and produce a value.
pub fn expr_to_value<NodeId: Clone>(
    expr: &ir::Expr,
    local_scope: Option<&HashMap<String, waymark_runner_expr::ValueExpr<NodeId>>>,
) -> Result<
    waymark_runner_expr::ValueExpr<NodeId>,
    waymark_runner_eval_core::ExprToValueError<SideEffectsNotAllowed>,
> {
    let applicator = PureApplicator::default();
    let mut evaluator = waymark_runner_eval_core::CoreEvaluator(applicator);
    evaluator.expr_to_value(expr, local_scope)
}
