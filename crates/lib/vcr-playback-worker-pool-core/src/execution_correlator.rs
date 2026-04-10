use std::ops::Deref;

use waymark_worker_core::{ActionCompletion, ActionRequest};

pub struct CorrelatedActionCompletion {
    pub completion: ActionCompletion,
    pub delay: std::time::Duration,
}

pub trait ExecutionCorrelator {
    type Error;

    fn correlate(&self, request: ActionRequest) -> Result<CorrelatedActionCompletion, Self::Error>;
}

impl<T> ExecutionCorrelator for T
where
    T: Deref,
    <T as Deref>::Target: self::ExecutionCorrelator,
{
    type Error = <<T as Deref>::Target as self::ExecutionCorrelator>::Error;

    fn correlate(&self, request: ActionRequest) -> Result<CorrelatedActionCompletion, Self::Error> {
        <Self as Deref>::deref(self).correlate(request)
    }
}
