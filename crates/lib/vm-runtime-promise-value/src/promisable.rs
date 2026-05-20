use waymark_vm_runtime_promise_core::{PromiseStateId, UnresolvedPromiseError};

use crate::PromiseValue;

impl<T> waymark_vm_runtime_promise_core::Suspendable for PromiseValue<T> {
    fn from_pending(promise_state_id: PromiseStateId) -> Self {
        Self::Pending(promise_state_id)
    }

    fn as_pending(&self) -> Option<PromiseStateId> {
        let Self::Pending(promise_state_id) = self else {
            return None;
        };
        Some(*promise_state_id)
    }
}

impl<T> waymark_vm_runtime_promise_core::Resolvable for PromiseValue<T> {
    type ReadyValue = T;

    fn from_ready(value: Self::ReadyValue) -> Self {
        Self::Ready(value)
    }

    fn into_ready(self) -> Result<Self::ReadyValue, (UnresolvedPromiseError, Self)> {
        self.require_ready()
    }

    fn as_ready(&self) -> Result<&Self::ReadyValue, UnresolvedPromiseError> {
        self.require_ready_ref()
    }

    fn as_ready_mut(&mut self) -> Result<&mut Self::ReadyValue, UnresolvedPromiseError> {
        self.require_ready_mut()
    }
}

#[cfg(test)]
mod tests {
    use waymark_vm_runtime_promise_core::{Resolvable as _, Suspendable as _};

    use super::*;

    #[test]
    fn require_resolved_returns_owned_value_for_ready_promises() {
        let promise = PromiseValue::<String>::from_ready("ready".into());

        assert_eq!(promise.into_ready().expect("promise is resolved"), "ready");
    }

    #[test]
    fn require_resolved_ref_and_mut_report_pending_promise_ids() {
        let promise = PromiseValue::<String>::from_pending(PromiseStateId(3));
        let err = promise
            .as_ready()
            .expect_err("pending promise should fail by reference");
        assert_eq!(err.promise_state_id, PromiseStateId(3));

        let promise = PromiseValue::<String>::from_pending(PromiseStateId(5));
        let err = promise
            .as_ready()
            .expect_err("pending promise should fail by mutable reference");
        assert_eq!(err.promise_state_id, PromiseStateId(5));
    }

    #[test]
    fn require_resolved_mut_returns_mutable_reference_for_ready_promises() {
        let mut promise = PromiseValue::<String>::from_ready("ready".into());

        promise
            .as_ready_mut()
            .expect("promise is resolved")
            .push_str(" now");

        assert_eq!(
            promise.as_ready_mut().expect("promise remains resolved"),
            "ready now"
        );
    }
}
