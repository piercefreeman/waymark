pub trait ActionCorrelator {
    /// Action correlation identity.
    /// Used to match the completion to the context.
    type ActionId: for<'a> From<&'a waymark_worker_core::ActionRequest>
        + for<'a> From<&'a waymark_worker_core::ActionCompletion>;

    /// Incomplete action context.
    type ActionContext;

    /// Correlated item for the action completion.
    type CorrelatedItem;

    /// Capture the context to maintain for action requests.
    fn capture_request_context(
        &mut self,
        request: waymark_worker_core::ActionRequest,
    ) -> Self::ActionContext;

    /// Capture the context to maintain for action requests.
    fn combine(
        &mut self,
        context: Self::ActionContext,
        completion: waymark_worker_core::ActionCompletion,
    ) -> Self::CorrelatedItem;
}
