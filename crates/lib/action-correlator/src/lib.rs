pub struct HashMap<ActionCorrelator: waymark_action_correlator_core::ActionCorrelator> {
    pub correlator: ActionCorrelator,
    pub incomplete_action_contexts:
        std::collections::HashMap<ActionCorrelator::ActionId, ActionCorrelator::ActionContext>,
}

impl<ActionCorrelator> HashMap<ActionCorrelator>
where
    ActionCorrelator: waymark_action_correlator_core::ActionCorrelator,
    ActionCorrelator::ActionId: core::hash::Hash + Eq,
{
    pub fn insert_request(
        &mut self,
        request: waymark_worker_core::ActionRequest,
    ) -> Option<ActionCorrelator::ActionContext> {
        let id = ActionCorrelator::ActionId::from(&request);
        let context = self.correlator.capture_request_context(request);
        self.incomplete_action_contexts.insert(id, context)
    }

    pub fn correlate_completion(
        &mut self,
        completion: waymark_worker_core::ActionCompletion,
    ) -> Result<ActionCorrelator::CorrelatedItem, ActionCorrelator::ActionId> {
        let id = ActionCorrelator::ActionId::from(&completion);
        let Some(incomplete_action_context) = self.incomplete_action_contexts.remove(&id) else {
            return Err(id);
        };
        Ok(self
            .correlator
            .combine(incomplete_action_context, completion))
    }
}
