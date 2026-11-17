use waymark_vm_runtime_core::{
    PromisableReadiness, PromisableReadinessMut, PromisableReadinessRef, PromiseStateHandle,
};

use crate::PromiseValue;

impl<T> waymark_vm_runtime_core::Promisable for PromiseValue<T> {
    type ReadyValue = T;

    fn from_ready(value: Self::ReadyValue) -> Self {
        Self::Ready(value)
    }

    fn from_pending(promise_state_handle: PromiseStateHandle) -> Self {
        Self::Pending(promise_state_handle)
    }

    fn promisable_readiness(&self) -> PromisableReadinessRef<'_, Self::ReadyValue> {
        match self {
            Self::Ready(value) => PromisableReadinessRef::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadinessRef::Pending(promise_state_handle)
            }
        }
    }

    fn promisable_readiness_mut(&mut self) -> PromisableReadinessMut<'_, Self::ReadyValue> {
        match self {
            Self::Ready(value) => PromisableReadinessMut::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadinessMut::Pending(promise_state_handle)
            }
        }
    }

    fn into_promisable_readiness(self) -> PromisableReadiness<Self::ReadyValue> {
        match self {
            Self::Ready(value) => PromisableReadiness::Ready(value),
            Self::Pending(promise_state_handle) => {
                PromisableReadiness::Pending(promise_state_handle)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use waymark_vm_runtime_core::Promisable as _;

    use super::*;

    fn promise_state_handle() -> PromiseStateHandle {
        waymark_vm_runtime_core::PromiseStates::<(), (), (), core::convert::Infallible>::new()
            .prepare()
    }

    #[test]
    fn try_ready_returns_owned_value_for_ready_promises() {
        let promise = PromiseValue::<String>::from_ready("ready".into());

        assert_eq!(
            promise
                .into_promisable_readiness()
                .try_ready()
                .expect("promise is resolved"),
            "ready"
        );
    }

    #[test]
    fn try_ready_by_ref_and_mut_report_pending_handles() {
        let promise_state_handle = promise_state_handle();
        let promise_state_id = promise_state_handle.id();

        let promise = PromiseValue::<String>::from_pending(promise_state_handle.clone());
        let err = promise
            .promisable_readiness()
            .try_ready()
            .expect_err("pending promise should fail by reference");
        assert_eq!(err.pending.id(), promise_state_id);

        let mut promise = PromiseValue::<String>::from_pending(promise_state_handle);
        let err = promise
            .promisable_readiness_mut()
            .try_ready()
            .expect_err("pending promise should fail by mutable reference");
        assert_eq!(err.pending.id(), promise_state_id);
    }

    #[test]
    fn try_ready_by_mut_returns_mutable_reference_for_ready_promises() {
        let mut promise = PromiseValue::<String>::from_ready("ready".into());

        promise
            .promisable_readiness_mut()
            .try_ready()
            .expect("promise is resolved")
            .push_str(" now");

        assert_eq!(
            promise
                .promisable_readiness_mut()
                .try_ready()
                .expect("promise remains resolved"),
            "ready now"
        );
    }
}
