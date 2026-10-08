use derive_where::derive_where;
use index_type::typed_vec::TypedVec;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{PromiseState, SettlingAlreadySettledPromiseError, WaitersFor};

/// A list of promise states.
#[derive_where(
    Debug;
    FunctionId, StateId, Value, RaisedException,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>,
)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "
            FunctionId: serde::Serialize,
            StateId: serde::Serialize,
            Value: serde::Serialize,
            RaisedException: serde::Serialize,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Serialize,
        ",
        deserialize = "
            FunctionId: serde::Deserialize<'de>,
            StateId: serde::Deserialize<'de>,
            Value: serde::Deserialize<'de>,
            RaisedException: serde::Deserialize<'de>,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Deserialize<'de>,
        ",
    ))
)]
pub struct PromiseStates<FunctionId, StateId, Value, RaisedException>(
    #[cfg_attr(feature = "serde", serde(with = "waymark_typed_vec_serde"))]
    TypedVec<PromiseStateId, PromiseState<FunctionId, StateId, Value, RaisedException>>,
)
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern;

impl<FunctionId, StateId, Value, RaisedException> Default
    for PromiseStates<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<FunctionId, StateId, Value, RaisedException>
    PromiseStates<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// Create a new list of promised states.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a new waiting promise state and return its id.
    ///
    /// Ids are indexes into an append-only list: entries are never
    /// removed or compacted, so an id is never handed out for a different
    /// promise — upholding the unique-per-VM invariant documented on
    /// [`PromiseStateId`].
    pub fn prepare(&mut self) -> PromiseStateId {
        self.0.push(PromiseState::Waiting(Vec::new()))
    }
}

/// Error returned when a promise state lookup refers to an unknown ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("promise state {promise_state_id:?} not found")]
pub struct PromiseStateNotFoundError {
    /// The promise state ID that could not be found.
    pub promise_state_id: PromiseStateId,
}

impl<FunctionId, StateId, Value, RaisedException>
    PromiseStates<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// Borrow a promise state by `promise_state_id`.
    pub fn get(
        &self,
        promise_state_id: PromiseStateId,
    ) -> Result<&PromiseState<FunctionId, StateId, Value, RaisedException>, PromiseStateNotFoundError>
    {
        self.0
            .get(promise_state_id)
            .ok_or(PromiseStateNotFoundError { promise_state_id })
    }

    /// Mutably borrow a promise state by `promise_state_id`.
    pub fn get_mut(
        &mut self,
        promise_state_id: PromiseStateId,
    ) -> Result<
        &mut PromiseState<FunctionId, StateId, Value, RaisedException>,
        PromiseStateNotFoundError,
    > {
        self.0
            .get_mut(promise_state_id)
            .ok_or(PromiseStateNotFoundError { promise_state_id })
    }

    /// Return an iterator over the IDs of all currently waiting
    /// (i.e. [`PromiseState::Waiting`]) promise states.
    pub fn waiting_ids(&self) -> impl Iterator<Item = PromiseStateId> + '_ {
        self.0.iter_enumerated().filter_map(|(id, state)| {
            if matches!(state, PromiseState::Waiting(_)) {
                Some(id)
            } else {
                None
            }
        })
    }
}

/// Errors returned when settling a promise state.
#[derive(Debug, thiserror::Error)]
pub enum SettlePromiseError<Value> {
    /// The requested promise state ID does not exist.
    #[error(transparent)]
    PromiseStateNotFound(PromiseStateNotFoundError),

    /// The promise state has already settled.
    #[error(transparent)]
    AlreadySettled(SettlingAlreadySettledPromiseError<Value>),
}

impl<FunctionId, StateId, Value, RaisedException>
    PromiseStates<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// Idempotently resolve a promise at a given `promise_state_id` with
    /// the provided `value`.
    ///
    /// Returns a list of waiters to notify, or an error if this promise
    /// has already settled.
    pub fn resolve(
        &mut self,
        promise_state_id: PromiseStateId,
        value: Value,
    ) -> Result<WaitersFor<FunctionId, StateId, Value, RaisedException>, SettlePromiseError<Value>>
    {
        let promise_state = self
            .get_mut(promise_state_id)
            .map_err(SettlePromiseError::PromiseStateNotFound)?;

        promise_state
            .resolve(value)
            .map_err(SettlePromiseError::AlreadySettled)
    }

    /// Idempotently reject a promise at a given `promise_state_id`.
    ///
    /// Returns a list of waiters to notify, or an error if this promise
    /// has already settled.
    pub fn reject(
        &mut self,
        promise_state_id: PromiseStateId,
        exception: RaisedException,
    ) -> Result<
        WaitersFor<FunctionId, StateId, Value, RaisedException>,
        SettlePromiseError<RaisedException>,
    > {
        let promise_state = self
            .get_mut(promise_state_id)
            .map_err(SettlePromiseError::PromiseStateNotFound)?;

        promise_state
            .reject(exception)
            .map_err(SettlePromiseError::AlreadySettled)
    }
}

impl<Value> SettlePromiseError<Value> {
    /// Map the `Value` of this [`SettlePromiseError`] into `OtherValue` using
    /// function `f`.
    pub fn map<OtherValue>(
        self,
        f: impl FnOnce(Value) -> OtherValue,
    ) -> SettlePromiseError<OtherValue> {
        match self {
            Self::PromiseStateNotFound(error) => SettlePromiseError::PromiseStateNotFound(error),
            Self::AlreadySettled(error) => {
                let SettlingAlreadySettledPromiseError { new_value } = error;
                let new_value = f(new_value);
                SettlePromiseError::AlreadySettled(SettlingAlreadySettledPromiseError { new_value })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PromiseStateId, PromiseStateNotFoundError, PromiseStates, SettlePromiseError};
    use crate::test_helpers::TestException;
    use crate::{
        Continuation, ExceptionHandlers, Frame, FrameKind, PromiseState, PromiseWaiter, RegisterId,
        Registers, SettledPromiseState,
    };

    fn continuation(
        dst: RegisterId,
        resume_state: usize,
    ) -> Continuation<&'static str, usize, i32, TestException, crate::ResumeWithValue> {
        Continuation::capture(
            Frame {
                func: "example",
                state: 0,
                regs: Registers::new(2),
                exception: None,
                exception_handler_blocks: ExceptionHandlers::new(),
                kind: FrameKind::TopLevel,
            },
            resume_state,
            dst,
        )
    }

    #[test]
    fn prepare_allocates_waiting_states_in_order() {
        let mut states = PromiseStates::<&'static str, usize, i32, TestException>::new();

        let first = states.prepare();
        let second = states.prepare();

        assert_eq!(first, PromiseStateId(0));
        assert_eq!(second, PromiseStateId(1));
        assert!(matches!(
            states.get(first).expect("promise state exists"),
            PromiseState::Waiting(continuations) if continuations.is_empty()
        ));
        assert!(matches!(
            states.get(second).expect("promise state exists"),
            PromiseState::Waiting(continuations) if continuations.is_empty()
        ));
    }

    #[test]
    fn resolve_updates_state_and_returns_continuations() {
        let mut states = PromiseStates::<&'static str, usize, i32, TestException>::new();
        let promise_state_id = states.prepare();
        let state = states
            .get_mut(promise_state_id)
            .expect("promise state exists");
        *state = PromiseState::Waiting(vec![PromiseWaiter::Await(continuation(RegisterId(0), 4))]);

        let continuations = states
            .resolve(promise_state_id, 23)
            .expect("prepared promise should resolve");

        assert_eq!(continuations.len(), 1);
        assert!(matches!(
            states.get(promise_state_id).expect("promise state exists"),
            PromiseState::Settled(SettledPromiseState::Resolved(value)) if *value == 23
        ));
    }

    #[test]
    fn resolve_rejects_unknown_promise_state_ids() {
        let mut states = PromiseStates::<&'static str, usize, i32, TestException>::new();

        let Err(SettlePromiseError::PromiseStateNotFound(PromiseStateNotFoundError {
            promise_state_id,
        })) = states.resolve(PromiseStateId(4), 23)
        else {
            panic!("unknown promise state IDs should be rejected");
        };

        assert_eq!(promise_state_id, PromiseStateId(4));
    }

    #[test]
    fn reject_preserves_exceptional_results() {
        let mut states = PromiseStates::<&'static str, usize, i32, TestException>::new();
        let promise_state_id = states.prepare();

        let continuations = states
            .reject(promise_state_id, TestException("ValueError"))
            .expect("prepared promise should resolve exceptionally");

        assert!(continuations.is_empty());
        assert!(matches!(
            states.get(promise_state_id).expect("promise state exists"),
            PromiseState::Settled(SettledPromiseState::Rejected(TestException("ValueError")))
        ));
    }
}
