use derive_where::derive_where;
use waymark_atomic_owners_counter::AtomicOwnersCounter;
use waymark_vm_runtime_promise_core::{PromiseStateId, TracePromiseStateIds};

use crate::{
    PromiseState, PromiseStateHandle, SettledPromiseState, SettlingAlreadySettledPromiseError,
    WaitersFor,
};

#[cfg(feature = "serde")]
mod serde_impl;

#[cfg(feature = "serde")]
pub(crate) use self::serde_impl::deserialize_promise_states;

/// One entry of the promise states table: a promise state, and the count of
/// the handles that hold it, with the table as one more owner.
///
/// Stored as the promise state alone, and read back by the promise states
/// table's snapshot reader with the counter of its id.
#[derive_where(
    Debug;
    FunctionId, StateId, Value, RaisedException,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>,
)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(
        transparent,
        bound(serialize = "
            FunctionId: serde::Serialize,
            StateId: serde::Serialize,
            Value: serde::Serialize,
            RaisedException: serde::Serialize,
            waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Serialize,
        ")
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

impl<FunctionId, StateId, Value, RaisedException>
    PromiseStatesEntry<FunctionId, StateId, Value, RaisedException>
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern,
{
    /// Whether the state is settled and the table is its only owner.
    fn is_settled_and_not_referenced(&self) -> bool {
        matches!(self.state, PromiseState::Settled(_))
            && self.owners_counter.strong() == std::num::NonZeroUsize::MIN
    }
}

/// The promise states of a runtime, keyed by their promise state ids.
///
/// Read back only as part of a [`crate::RuntimeState`].
#[derive_where(Default)]
#[derive_where(
    Debug;
    FunctionId, StateId, Value, RaisedException,
    waymark_vm_runtime_exception::MatchPatternOf<RaisedException>,
)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(bound(serialize = "
        FunctionId: serde::Serialize,
        StateId: serde::Serialize,
        Value: serde::Serialize,
        RaisedException: serde::Serialize,
        waymark_vm_runtime_exception::MatchPatternOf<RaisedException>: serde::Serialize,
    "))
)]
pub struct PromiseStates<FunctionId, StateId, Value, RaisedException>(
    PromiseStatesEntriesMap<FunctionId, StateId, Value, RaisedException>,
)
where
    RaisedException: waymark_vm_runtime_exception::HasMatchPattern;

/// The promise states table's entries by their promise state ids.
type PromiseStatesEntriesMap<FunctionId, StateId, Value, RaisedException> = waymark_id_map::IdMap<
    PromiseStateId,
    PromiseStatesEntry<FunctionId, StateId, Value, RaisedException>,
>;

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

    /// Remove every settled promise state that no handle references any more,
    /// leaving the table as its only owner.
    ///
    /// A removed state's value can hold the last handle to another settled
    /// state, which goes in the same call. The table is scanned once, and
    /// each removed state's value is traced once, so a call costs the
    /// table's size plus the size of the removed values, which dropping them
    /// costs anyway.
    ///
    /// Counting alone never frees settled states that hold each other's
    /// handles in a cycle; the sweep relies on no such cycle forming.
    pub fn sweep(&mut self)
    where
        Value: TracePromiseStateIds,
        RaisedException: TracePromiseStateIds,
    {
        let mut not_referenced: Vec<PromiseStateId> = self
            .0
            .iter()
            .filter(|(_, entry)| entry.is_settled_and_not_referenced())
            .map(|(id, _)| id)
            .collect();
        let mut referenced_by_removed = Vec::new();

        while let Some(id) = not_referenced.pop() {
            // Two handles to one state in a removed value push its id twice;
            // the second pop finds it gone.
            let Some(entry) = self.0.remove(id) else {
                continue;
            };

            if let PromiseState::Settled(settled) = &entry.state {
                let mut visit = |id| referenced_by_removed.push(id);
                match settled {
                    SettledPromiseState::Resolved(value) => {
                        value.trace_promise_state_ids(&mut visit);
                    }
                    SettledPromiseState::Rejected(exception) => {
                        exception.trace_promise_state_ids(&mut visit);
                    }
                }
            }
            drop(entry);

            for id in referenced_by_removed.drain(..) {
                if self
                    .0
                    .get(id)
                    .is_some_and(PromiseStatesEntry::is_settled_and_not_referenced)
                {
                    not_referenced.push(id);
                }
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
    use std::cell::Cell;
    use std::rc::Rc;

    use super::{PromiseStateId, PromiseStateNotFoundError, PromiseStates, SettlePromiseError};
    use crate::test_helpers::{TestException, TestHandles, TestHandlesException, TestNumber};
    use crate::{
        Continuation, ExceptionHandlers, Frame, FrameKind, PromiseState, PromiseStateHandle,
        PromiseWaiter, RegisterId, Registers, SettledPromiseState,
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
    fn sweep_removes_settled_states_not_referenced() {
        let mut states = PromiseStates::<&'static str, usize, TestNumber, TestException>::new();
        let not_referenced = states.prepare();
        let referenced = states.prepare();
        let waiting = states.prepare().id();

        let not_referenced_id = not_referenced.id();
        let referenced_id = referenced.id();
        drop(not_referenced);
        states
            .resolve(not_referenced_id, TestNumber(1))
            .expect("waiting promise resolves");
        states
            .resolve(referenced_id, TestNumber(2))
            .expect("waiting promise resolves");

        states.sweep();

        assert!(states.get(not_referenced_id).is_err());
        assert!(matches!(
            states.by_handle(&referenced),
            Ok(PromiseState::Settled(SettledPromiseState::Resolved(
                TestNumber(2)
            )))
        ));
        assert!(states.mut_by_handle(&referenced).is_ok());
        assert!(states.get(waiting).is_ok());

        drop(referenced);
        states.sweep();

        assert!(states.get(referenced_id).is_err());
        assert!(states.get(waiting).is_ok());
    }

    #[test]
    fn sweep_frees_a_chain_in_one_call_tracing_each_removed_value_once() {
        const CHAIN_LENGTH: usize = 100;

        let mut states = PromiseStates::<&'static str, usize, TestHandles, TestException>::new();
        let traces = Rc::new(Cell::new(0));

        // Each state's value holds the only other handle to the next state,
        // so a state is not referenced any more only once the previous one
        // is removed.
        // The last state's value holds no handle.
        let ids: Vec<_> = (0..CHAIN_LENGTH).map(|_| states.prepare()).collect();
        let mut next = None;
        for handle in ids.iter().rev() {
            let id = handle.id();
            states
                .resolve(
                    id,
                    TestHandles {
                        handles: next.take().into_iter().collect(),
                        traces: Rc::clone(&traces),
                    },
                )
                .expect("waiting promise resolves");
            next = Some(handle.clone());
        }
        let chain_ids: Vec<_> = ids.iter().map(PromiseStateHandle::id).collect();
        drop(next);
        drop(ids);

        states.sweep();

        assert!(chain_ids.iter().all(|id| states.get(*id).is_err()));
        assert_eq!(traces.get(), CHAIN_LENGTH);
    }

    #[test]
    fn sweep_frees_a_state_referenced_twice_by_one_value() {
        let mut states = PromiseStates::<&'static str, usize, TestHandles, TestException>::new();
        let traces = Rc::new(Cell::new(0));
        let inner = states.prepare();
        let outer = states.prepare();
        let inner_id = inner.id();
        let outer_id = outer.id();
        drop(outer);
        states
            .resolve(
                inner_id,
                TestHandles {
                    handles: Vec::new(),
                    traces: Rc::clone(&traces),
                },
            )
            .expect("waiting promise resolves");
        states
            .resolve(
                outer_id,
                TestHandles {
                    handles: vec![inner.clone(), inner],
                    traces: Rc::clone(&traces),
                },
            )
            .expect("waiting promise resolves");

        states.sweep();

        assert!(states.get(outer_id).is_err());
        assert!(states.get(inner_id).is_err());
        assert_eq!(traces.get(), 2);
    }

    #[test]
    fn sweep_frees_a_state_referenced_by_a_rejected_state_in_one_call() {
        let mut states =
            PromiseStates::<&'static str, usize, TestHandles, TestHandlesException>::new();
        let traces = Rc::new(Cell::new(0));
        let inner = states.prepare();
        let outer = states.prepare();
        let inner_id = inner.id();
        let outer_id = outer.id();
        drop(outer);
        states
            .resolve(
                inner_id,
                TestHandles {
                    handles: Vec::new(),
                    traces: Rc::clone(&traces),
                },
            )
            .expect("waiting promise resolves");
        // The outer state's exception holds the only other handle to the
        // inner state.
        states
            .reject(
                outer_id,
                TestHandlesException {
                    handles: vec![inner],
                    traces: Rc::clone(&traces),
                },
            )
            .expect("waiting promise rejects");

        states.sweep();

        assert!(states.get(outer_id).is_err());
        assert!(states.get(inner_id).is_err());
        assert_eq!(traces.get(), 2);
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
