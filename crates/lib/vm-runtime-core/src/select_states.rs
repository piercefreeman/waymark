use std::collections::BTreeMap;

use crate::{Continuation, RegisterId, ResumeSelectArm, ResumeWithValue, SelectArm};

/// An opaque identifier of a select state.
///
/// # Invariant: unique per VM
///
/// An id value belongs to at most one select over the entire lifetime of
/// the VM that issued it - including snapshot/restore cycles. The inert
/// handling of losing select arms rests on exactly this property: a stale
/// arm entry finding its id absent means its select was already claimed.
/// A reused id would let such a stale arm claim an unrelated select.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SelectStateId(pub usize);

/// A handle to an inserted select: the capability to mint its arms'
/// claims.
///
/// Returned by [`SelectStates::insert`] and the only way to create
/// [`SelectStateClaim`]s - so every planted arm provably refers to a select
/// that was actually inserted.
#[derive(Debug, Clone, Copy)]
pub struct SelectStateHandle {
    /// The select this handle mints claims for.
    select_state_id: SelectStateId,
}

impl SelectStateHandle {
    /// Create a claim through an arm of this select, delivering to the
    /// `dst` register and resuming from the `resume` state.
    pub fn arm<StateId>(&self, dst: RegisterId, resume: StateId) -> SelectStateClaim<StateId> {
        SelectStateClaim {
            select_state_id: self.select_state_id,
            arm: SelectArm::new(dst, resume),
        }
    }
}

/// A select's claim to a promise's settlement, planted as one arm of
/// the select.
///
/// Opaque: only mintable through a [`SelectStateHandle`], and only consumed by
/// the settlement paths claiming the select.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SelectStateClaim<StateId> {
    /// The select this arm belongs to.
    select_state_id: SelectStateId,

    /// The arm's delivery target.
    arm: SelectArm<StateId>,
}

/// The select continuations of a runtime, keyed by their select state ids.
///
/// A select continuation is inserted when its frame waits on multiple
/// promises at once, and claimed - exactly once - by the first of its arms
/// to settle.
#[derive(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "FunctionId: serde::Serialize, StateId: serde::Serialize, Value: serde::Serialize",
        deserialize = "FunctionId: serde::Deserialize<'de>, StateId: serde::Deserialize<'de>, Value: serde::Deserialize<'de>",
    ))
)]
pub struct SelectStates<FunctionId, StateId, Value> {
    /// The id to assign to the next select state.
    ///
    /// Monotonically increasing and never rewound - upholding the
    /// unique-per-VM invariant documented on [`SelectStateId`].
    next_select_state_id: usize,

    /// The select continuations by their ids.
    //
    // A `BTreeMap` keeps the serialized snapshot form deterministic.
    continuations:
        BTreeMap<SelectStateId, Continuation<FunctionId, StateId, Value, ResumeSelectArm>>,
}

impl<FunctionId, StateId, Value> Default for SelectStates<FunctionId, StateId, Value> {
    fn default() -> Self {
        Self {
            next_select_state_id: 0,
            continuations: BTreeMap::new(),
        }
    }
}

impl<FunctionId, StateId, Value> SelectStates<FunctionId, StateId, Value> {
    /// Create a new empty select states collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a select continuation, returning the handle that mints its
    /// arms.
    pub fn insert(
        &mut self,
        continuation: Continuation<FunctionId, StateId, Value, ResumeSelectArm>,
    ) -> SelectStateHandle {
        let select_state_id = SelectStateId(self.next_select_state_id);
        self.next_select_state_id += 1;

        self.continuations.insert(select_state_id, continuation);

        SelectStateHandle { select_state_id }
    }

    /// Claim the select through the given arm's claim, producing the
    /// continuation resumable through that arm.
    ///
    /// Returns `None` if the select has already been claimed - the normal
    /// fate of every losing select arm.
    pub(crate) fn claim(
        &mut self,
        select_state_claim: SelectStateClaim<StateId>,
    ) -> Option<Continuation<FunctionId, StateId, Value, ResumeWithValue>> {
        let SelectStateClaim {
            select_state_id,
            arm,
        } = select_state_claim;

        let continuation = self.continuations.remove(&select_state_id)?;
        Some(continuation.into_arm(arm))
    }
}

#[cfg(test)]
mod tests {
    use waymark_vm_runtime_promise_value::PromiseValue;

    use super::{SelectStateId, SelectStates};
    use crate::{Continuation, ExceptionHandlers, Frame, FrameKind, Registers, StateCalls};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestReadyValue;

    type TestValue = PromiseValue<TestReadyValue>;

    impl waymark_vm_runtime_value::RootValueAccess for TestReadyValue {
        type RootValue = TestValue;
    }

    fn select_continuation() -> Continuation<&'static str, usize, TestValue, crate::ResumeSelectArm>
    {
        Continuation::capture_select(Frame {
            func: "example",
            state: 0,
            regs: Registers::new(2),
            exception: None,
            exception_handler_blocks: ExceptionHandlers::new(),
            state_calls: StateCalls::new(),
            kind: FrameKind::TopLevel,
        })
    }

    #[test]
    fn insert_mints_monotonically_increasing_ids() {
        let mut select_states = SelectStates::new();

        assert_eq!(
            select_states.insert(select_continuation()).select_state_id,
            SelectStateId(0)
        );
        assert_eq!(
            select_states.insert(select_continuation()).select_state_id,
            SelectStateId(1)
        );
    }

    #[test]
    fn claim_removes_the_select_continuation_exactly_once() {
        let mut select_states = SelectStates::new();
        let handle = select_states.insert(select_continuation());

        assert!(
            select_states
                .claim(handle.arm(crate::RegisterId(0), 0))
                .is_some()
        );
        assert!(
            select_states
                .claim(handle.arm(crate::RegisterId(0), 0))
                .is_none()
        );
    }

    #[test]
    fn ids_are_not_reused_after_claims() {
        let mut select_states = SelectStates::new();
        let first = select_states.insert(select_continuation());
        select_states
            .claim(first.arm(crate::RegisterId(0), 0))
            .expect("first select is inserted");

        assert_eq!(
            select_states.insert(select_continuation()).select_state_id,
            SelectStateId(1)
        );
    }
}
