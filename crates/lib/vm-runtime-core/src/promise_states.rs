use derive_where::derive_where;
use waymark_atomic_owners_counter::AtomicOwnersCounter;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{PromiseState, PromiseStateHandle, SettlingAlreadySettledPromiseError, WaitersFor};

/// One entry of the promise states table: a promise state, and the count of
/// the handles that hold it, with the table as one more owner.
///
/// Stored as the promise state alone.
#[derive_where(
    Debug;
    FunctionId, StateId, Value, RaisedException,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>,
)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(
        transparent,
        bound(
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
        )
    )
)]
struct PromiseStatesEntry<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// The promise state.
    state: PromiseState<FunctionId, StateId, Value, RaisedException>,

    /// The table's owner of the state's count.
    #[cfg_attr(feature = "serde", serde(skip))]
    owners_counter: AtomicOwnersCounter,
}

/// The promise states of a runtime, keyed by their promise state ids.
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
    waymark_id_map::IdMap<
        PromiseStateId,
        PromiseStatesEntry<FunctionId, StateId, Value, RaisedException>,
    >,
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

    /// Allocate a new waiting promise state and return the first handle
    /// holding it.
    ///
    /// The map mints the id and never hands it out again — upholding the
    /// unique-per-VM invariant documented on [`PromiseStateId`].
    pub fn prepare(&mut self) -> PromiseStateHandle {
        let owners_counter = AtomicOwnersCounter::new();
        let handle_owners_counter = owners_counter.clone();

        let id = self.0.insert(PromiseStatesEntry {
            state: PromiseState::Waiting(Vec::new()),
            owners_counter,
        });

        PromiseStateHandle::new(id, handle_owners_counter)
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
        let entry = self
            .0
            .get(promise_state_id)
            .ok_or(PromiseStateNotFoundError { promise_state_id })?;

        Ok(&entry.state)
    }

    /// Mutably borrow a promise state by `promise_state_id`.
    pub fn get_mut(
        &mut self,
        promise_state_id: PromiseStateId,
    ) -> Result<
        &mut PromiseState<FunctionId, StateId, Value, RaisedException>,
        PromiseStateNotFoundError,
    > {
        let entry = self
            .0
            .get_mut(promise_state_id)
            .ok_or(PromiseStateNotFoundError { promise_state_id })?;

        Ok(&mut entry.state)
    }

    /// Borrow the promise state `promise_state_handle` holds.
    ///
    /// The handle is borrowed for the lookup only; the state borrows the
    /// table alone.
    pub fn by_handle<'state>(
        &'state self,
        promise_state_handle: &PromiseStateHandle,
    ) -> Result<
        &'state PromiseState<FunctionId, StateId, Value, RaisedException>,
        PromiseStateNotFoundError,
    > {
        self.get(promise_state_handle.id())
    }

    /// Mutably borrow the promise state `promise_state_handle` holds.
    ///
    /// The handle is borrowed for the lookup only; the state borrows the
    /// table alone.
    pub fn mut_by_handle<'state>(
        &'state mut self,
        promise_state_handle: &PromiseStateHandle,
    ) -> Result<
        &'state mut PromiseState<FunctionId, StateId, Value, RaisedException>,
        PromiseStateNotFoundError,
    > {
        self.get_mut(promise_state_handle.id())
    }

    /// Remove every settled promise state that no handle holds any more,
    /// leaving the table as its only owner.
    ///
    /// Passes repeat until one removes nothing: a removed state's value can
    /// hold the last handle to another settled state.
    ///
    /// Counting alone never frees settled states that hold each other's
    /// handles in a cycle; the sweep relies on no such cycle forming.
    pub fn sweep(&mut self) {
        loop {
            let unheld: Vec<PromiseStateId> = self
                .0
                .iter()
                .filter(|(_, entry)| {
                    matches!(entry.state, PromiseState::Settled(_))
                        && entry.owners_counter.strong() == std::num::NonZeroUsize::MIN
                })
                .map(|(id, _)| id)
                .collect();

            if unheld.is_empty() {
                return;
            }

            for id in unheld {
                self.0.remove(id);
            }
        }
    }

    /// Return an iterator over the IDs of all currently waiting
    /// (i.e. [`PromiseState::Waiting`]) promise states.
    pub fn waiting_ids(&self) -> impl Iterator<Item = PromiseStateId> + '_ {
        self.0.iter().filter_map(|(id, entry)| {
            if matches!(entry.state, PromiseState::Waiting(_)) {
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

        let first = states.prepare().id();
        let second = states.prepare().id();

        assert_eq!(first, PromiseStateId::from_raw(0));
        assert_eq!(second, PromiseStateId::from_raw(1));
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
        let promise_state_id = states.prepare().id();
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
        })) = states.resolve(PromiseStateId::from_raw(4), 23)
        else {
            panic!("unknown promise state IDs should be rejected");
        };

        assert_eq!(promise_state_id, PromiseStateId::from_raw(4));
    }

    #[test]
    fn sweep_removes_settled_states_no_handle_holds() {
        let mut states = PromiseStates::<&'static str, usize, i32, TestException>::new();
        let unheld = states.prepare();
        let held = states.prepare();
        let waiting = states.prepare().id();

        let unheld_id = unheld.id();
        let held_id = held.id();
        drop(unheld);
        states
            .resolve(unheld_id, 1)
            .expect("waiting promise resolves");
        states
            .resolve(held_id, 2)
            .expect("waiting promise resolves");

        states.sweep();

        assert!(states.get(unheld_id).is_err());
        assert!(matches!(
            states.by_handle(&held),
            Ok(PromiseState::Settled(SettledPromiseState::Resolved(2)))
        ));
        assert!(states.mut_by_handle(&held).is_ok());
        assert!(states.get(waiting).is_ok());

        drop(held);
        states.sweep();

        assert!(states.get(held_id).is_err());
        assert!(states.get(waiting).is_ok());
    }

    #[test]
    fn sweep_frees_a_chain_in_one_call() {
        let mut states = PromiseStates::<
            &'static str,
            usize,
            Option<crate::PromiseStateHandle>,
            TestException,
        >::new();
        let inner = states.prepare();
        let outer = states.prepare();
        let inner_id = inner.id();
        let outer_id = outer.id();
        drop(outer);
        states
            .resolve(inner_id, None)
            .expect("waiting promise resolves");
        // The outer state's value holds the only other handle to the inner
        // one, so the inner state is freed only once the outer one is.
        states
            .resolve(outer_id, Some(inner))
            .expect("waiting promise resolves");

        states.sweep();

        assert!(states.get(outer_id).is_err());
        assert!(states.get(inner_id).is_err());
    }

    #[test]
    fn reject_preserves_exceptional_results() {
        let mut states = PromiseStates::<&'static str, usize, i32, TestException>::new();
        let promise_state_id = states.prepare().id();

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
