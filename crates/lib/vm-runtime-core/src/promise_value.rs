use crate::PromiseStateId;

/// A promise value.
///
/// Wraps a non-promise value type with the possibilities of either it being
/// immediately available (`Resolved`) or being in a placeholder
/// state (`Pending`) waiting for a wrapped value to be resolved with.
#[derive(Clone, Debug)]
pub enum Promise<Value> {
    /// Placeholder for a value and the ID of the associated state that holds
    /// continuations to resume when the promise resolves.
    Pending(PromiseStateId),

    /// Actual value.
    Resolved(Value),
}

/// A resolved promise was required but the actual value was pending.
#[derive(Debug, thiserror::Error)]
#[error("an unresolved async value is used where a resolved value is expected")]
pub struct UnresolvedPromiseError {
    /// The ID of the promise state.
    ///
    /// For reconstructing the promise back if needed.
    pub promise_state_id: PromiseStateId,
}

impl<Value> Promise<Value> {
    /// Require a promise to be resolved and unwrap it into the raw value.
    pub fn require_resolved(self) -> Result<Value, UnresolvedPromiseError> {
        match self {
            Promise::Pending(promise_state_id) => Err(UnresolvedPromiseError { promise_state_id }),
            Promise::Resolved(value) => Ok(value),
        }
    }

    /// Require a promise to be resolved and unwrap it into the raw value ref.
    pub fn require_resolved_ref(&self) -> Result<&Value, UnresolvedPromiseError> {
        match self {
            Promise::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
            Promise::Resolved(value) => Ok(value),
        }
    }

    /// Require a promise to be resolved and unwrap it into the raw value mut ref.
    pub fn require_resolved_mut(&mut self) -> Result<&mut Value, UnresolvedPromiseError> {
        match self {
            Promise::Pending(promise_state_id) => Err(UnresolvedPromiseError {
                promise_state_id: *promise_state_id,
            }),
            Promise::Resolved(value) => Ok(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Promise;
    use crate::PromiseStateId;

    #[test]
    fn require_resolved_returns_owned_value_for_ready_promises() {
        let promise = Promise::Resolved(String::from("ready"));

        assert_eq!(
            promise.require_resolved().expect("promise is resolved"),
            "ready"
        );
    }

    #[test]
    fn require_resolved_ref_and_mut_report_pending_promise_ids() {
        let promise = Promise::<String>::Pending(PromiseStateId(3));
        let err = promise
            .require_resolved_ref()
            .expect_err("pending promise should fail by reference");
        assert_eq!(err.promise_state_id, PromiseStateId(3));

        let mut promise = Promise::<String>::Pending(PromiseStateId(5));
        let err = promise
            .require_resolved_mut()
            .expect_err("pending promise should fail by mutable reference");
        assert_eq!(err.promise_state_id, PromiseStateId(5));
    }

    #[test]
    fn require_resolved_mut_returns_mutable_reference_for_ready_promises() {
        let mut promise = Promise::Resolved(String::from("ready"));

        promise
            .require_resolved_mut()
            .expect("promise is resolved")
            .push_str(" now");

        assert_eq!(
            promise
                .require_resolved_ref()
                .expect("promise remains resolved"),
            "ready now"
        );
    }
}
