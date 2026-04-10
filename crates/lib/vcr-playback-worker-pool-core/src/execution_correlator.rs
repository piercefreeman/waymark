use waymark_worker_core::{ActionCompletion, ActionRequest};

pub struct CorrelatedActionCompletion {
    pub completion: ActionCompletion,
    pub delay: std::time::Duration,
}

pub trait ExecutionCorrelator {
    type Error;

    fn correlate(&self, request: ActionRequest) -> Result<CorrelatedActionCompletion, Self::Error>;
}
