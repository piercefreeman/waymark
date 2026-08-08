use crate::RunnerStateError;

impl crate::RunnerState {
    pub fn as_core_eval(&mut self) -> waymark_runner_eval_core::CoreEvaluator<&mut Self> {
        waymark_runner_eval_core::CoreEvaluator(self)
    }
}

impl From<waymark_runner_eval_core::ExprToValueError<RunnerStateError>> for RunnerStateError {
    fn from(value: waymark_runner_eval_core::ExprToValueError<RunnerStateError>) -> Self {
        Self(value.to_string())
    }
}

impl waymark_runner_eval_core::SideEffectApplicator for &mut crate::RunnerState {
    type ActionCallError = RunnerStateError;
    type NodeId = waymark_ids::ExecutionId;

    fn action_call(
        &mut self,
        iteration_index: Option<i32>,
        params: waymark_runner_eval_core::ActionCallParams<Self::NodeId>,
    ) -> Result<waymark_runner_expr::ActionResultValue<Self::NodeId>, Self::ActionCallError> {
        let waymark_runner_eval_core::ActionCallParams { action, targets } = params;
        self.queue_action_spec(action, targets, iteration_index)
    }
}
