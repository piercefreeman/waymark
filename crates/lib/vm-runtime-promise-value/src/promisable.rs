use waymark_vm_runtime_core::PromiseStateHandle;
use waymark_vm_runtime_promise_core::{PromiseStateId, UnresolvedPromiseError};

use crate::PromiseValue;

impl<T> waymark_vm_runtime_core::Suspendable for PromiseValue<T> {
    fn from_pending(promise_state_handle: PromiseStateHandle) -> Self {
        Self::Pending(promise_state_handle)
    }

    fn as_pending(&self) -> Option<PromiseStateId> {
        let Self::Pending(promise_state_handle) = self else {
            return None;
        };
        Some(promise_state_handle.id())
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
    use waymark_vm_runtime_core::Suspendable as _;
    use waymark_vm_runtime_promise_core::Resolvable as _;

    use super::*;

    fn promise_state_handle() -> PromiseStateHandle {
        waymark_vm_runtime_core::PromiseStates::<(), (), (), core::convert::Infallible>::new()
            .prepare()
    }

    #[test]
    fn require_resolved_returns_owned_value_for_ready_promises() {
        let promise = PromiseValue::<String>::from_ready("ready".into());

        assert_eq!(promise.into_ready().expect("promise is resolved"), "ready");
    }

    #[test]
    fn require_resolved_ref_and_mut_report_pending_promise_ids() {
        let promise_state_handle = promise_state_handle();
        let promise_state_id = promise_state_handle.id();

        let promise = PromiseValue::<String>::from_pending(promise_state_handle.clone());
        let err = promise
            .as_ready()
            .expect_err("pending promise should fail by reference");
        assert_eq!(err.promise_state_id, promise_state_id);

        let mut promise = PromiseValue::<String>::from_pending(promise_state_handle);
        let err = promise
            .as_ready_mut()
            .expect_err("pending promise should fail by mutable reference");
        assert_eq!(err.promise_state_id, promise_state_id);
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
