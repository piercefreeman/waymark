use std::collections::VecDeque;

use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{
    Frame, PromiseStateNotFoundError, PromiseStates, SettlePromiseStateError, SettledPromiseState,
    Waiter,
};

/// Errors returned when settling a promise at the runtime state and
/// notifying its waiters.
#[derive(Debug, thiserror::Error)]
pub enum SettlePromiseError<Value> {
    /// Resolving the promise state in the store failed.
    #[error(transparent)]
    PromiseState(SettlePromiseStateError<Value>),

    /// A fired race arm targeted a race promise that does not exist.
    ///
    /// This is an invariant violation: an unsettled race promise is never
    /// removed, so a dangling race arm means the runtime state is corrupted.
    /// When this error is returned the settlement propagation was aborted
    /// midway - the runtime state is partially mutated and must be
    /// discarded.
    #[error("race arm target: {0}")]
    RaceArmTargetNotFound(#[source] PromiseStateNotFoundError),
}

impl<Value> SettlePromiseError<Value> {
    /// Map the `Value` of this [`SettlePromiseError`] into `OtherValue` using
    /// function `f`.
    pub fn map<OtherValue>(
        self,
        f: impl FnOnce(Value) -> OtherValue,
    ) -> SettlePromiseError<OtherValue> {
        match self {
            Self::PromiseState(error) => SettlePromiseError::PromiseState(error.map(f)),
            Self::RaceArmTargetNotFound(error) => SettlePromiseError::RaceArmTargetNotFound(error),
        }
    }
}

/// The state shape of the runtime.
///
/// Public struct, but the runtime itself will shield the access to
/// the fields by holding it without public visibility.
///
/// The access to the runtime state is indirectly provided to the interpreters
/// via the [`crate::CaptureRuntimeView`].
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RuntimeState<FunctionId, StateId, Value> {
    /// The queue of the ready-to-execute frames.
    ///
    /// Due to the nature of the asyncrony and continuations support, we require
    /// that at runtime all the values can be promises.
    //
    // TODO: replace with a more restricted interface
    pub ready: VecDeque<Frame<FunctionId, StateId, Value>>,

    /// A state of the promises of this runtime.
    //
    // TODO: the promise states should be garbage-collected together with
    // the promise values that refer to them. We can implement this without
    // a full garbage-collector by holding the promises in a weak-rc-map
    // or something like that.
    pub promise_states: PromiseStates<FunctionId, StateId, Value>,

    /// Sequential counter of effects produced by this runtime.
    ///
    /// Incremented each time the runtime emits an effect.
    pub effect_counter: EffectNumber,
}

impl<FunctionId, StateId, Value> RuntimeState<FunctionId, StateId, Value>
where
    Value: Clone,
{
    /// Provide an async computation value for a given promise.
    ///
    /// Notifies all waiters: resumes the suspended frames with the value and
    /// fires the race arms.
    pub fn resolve_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        value: Value,
    ) -> Result<(), SettlePromiseError<Value>> {
        let waiters = self
            .promise_states
            .resolve(promise_state_id, value.clone())
            .map_err(SettlePromiseError::PromiseState)?;

        notify_waiters(
            &mut self.ready,
            &mut self.promise_states,
            waiters,
            SettledPromiseState::Resolved(value),
        )
        .map_err(SettlePromiseError::RaceArmTargetNotFound)
    }

    /// Reject an async computation for a given promise.
    ///
    /// Notifies all waiters: resumes the suspended frames with the raised
    /// exception and fires the race arms.
    pub fn reject_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        exception: waymark_vm_runtime_exception::Exception<Value>,
    ) -> Result<(), SettlePromiseError<waymark_vm_runtime_exception::Exception<Value>>> {
        let waiters = self
            .promise_states
            .reject(promise_state_id, exception.clone())
            .map_err(SettlePromiseError::PromiseState)?;

        notify_waiters(
            &mut self.ready,
            &mut self.promise_states,
            waiters,
            SettledPromiseState::Rejected(exception),
        )
        .map_err(SettlePromiseError::RaceArmTargetNotFound)
    }
}

/// Notify a drained waiter list with the settlement that fired it,
/// propagating through any race arms.
///
/// Resumed frames go to the `ready` queue.  A fired race arm resolves its
/// race promise in `promise_states` with the arm's pre-built resolution
/// value, which drains the race promise's own waiters in turn -
/// a settlement of either kind fires the arm the same way.  An arm that
/// lost its race finds the race promise already settled and is inert by
/// the first-wins semantics.
fn notify_waiters<FunctionId, StateId, Value>(
    ready: &mut VecDeque<Frame<FunctionId, StateId, Value>>,
    promise_states: &mut PromiseStates<FunctionId, StateId, Value>,
    waiters: Vec<Waiter<FunctionId, StateId, Value>>,
    settlement: SettledPromiseState<Value>,
) -> Result<(), PromiseStateNotFoundError>
where
    Value: Clone,
{
    let mut worklist = vec![(waiters, settlement)];

    while let Some((waiters, settlement)) = worklist.pop() {
        for waiter in waiters {
            match waiter {
                Waiter::Continuation(continuation) => {
                    let frame = match &settlement {
                        SettledPromiseState::Resolved(value) => continuation.resume(value.clone()),
                        SettledPromiseState::Rejected(exception) => {
                            continuation.raise_exception(exception.clone())
                        }
                    };
                    ready.push_back(frame);
                }
                Waiter::RaceArm { race, resolution } => {
                    let race_state = promise_states.get_mut(race)?;
                    // The only resolve error is the already-settled one
                    // ([`SettlingAlreadySettledPromiseError`]): this arm
                    // lost the race - inert by the first-wins semantics.
                    if let Ok(race_waiters) = race_state.resolve(resolution.clone()) {
                        worklist.push((race_waiters, SettledPromiseState::Resolved(resolution)));
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use waymark_vm_runtime_effect::EffectNumber;
    use waymark_vm_runtime_exception::Exception;
    use waymark_vm_runtime_promise_value::PromiseValue;

    use super::RuntimeState;
    use crate::{
        Continuation, ExceptionHandlers, Frame, FrameKind, PromiseState, PromiseStates, RegisterId,
        Registers, SettlePromiseError, SettlePromiseStateError, SettledPromiseState, Waiter,
    };
    use waymark_vm_runtime_promise_core::PromiseStateId;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestReadyValue {
        Int(i32),
    }

    type TestValue = PromiseValue<TestReadyValue>;

    impl waymark_vm_runtime_value::RootValueAccess for TestReadyValue {
        type RootValue = TestValue;
    }

    fn frame(state: usize) -> Frame<&'static str, usize, TestValue> {
        Frame {
            func: "example",
            state,
            regs: Registers::new(2),
            exception: None,
            exception_handler_blocks: ExceptionHandlers::new(),
            kind: FrameKind::TopLevel,
        }
    }

    fn continuation(
        dst: RegisterId,
        resume_state: usize,
    ) -> Continuation<&'static str, usize, TestValue, crate::ResumeWithValue> {
        Continuation::capture(frame(0), resume_state, dst)
    }

    #[test]
    fn resolve_promise_enqueues_resumed_frames_and_records_resolved_value() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let promise_state_id = promise_states.prepare();
        let promise_state = promise_states
            .get_mut(promise_state_id)
            .expect("promise state exists");
        *promise_state = PromiseState::Waiting(vec![
            Waiter::Continuation(continuation(RegisterId(0), 3)),
            Waiter::Continuation(continuation(RegisterId(1), 5)),
        ]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            effect_counter: EffectNumber(0),
        };

        runtime
            .resolve_promise(
                promise_state_id,
                PromiseValue::Ready(TestReadyValue::Int(41)),
            )
            .expect("waiting promise should resolve");

        assert_eq!(runtime.ready.len(), 2);

        let PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
            TestReadyValue::Int(value),
        ))) = runtime
            .promise_states
            .get(promise_state_id)
            .expect("promise state exists")
        else {
            panic!("promise state should be settled with a resolved value");
        };
        assert_eq!(*value, 41);

        let first = runtime.ready.pop_front().expect("first resumed frame");
        assert_eq!(first.state, 3);
        let Some(PromiseValue::Ready(TestReadyValue::Int(value))) = first.regs.get(RegisterId(0))
        else {
            panic!("first resumed frame should receive the resolved value");
        };
        assert_eq!(*value, 41);

        let second = runtime.ready.pop_front().expect("second resumed frame");
        assert_eq!(second.state, 5);
        let Some(PromiseValue::Ready(TestReadyValue::Int(value))) = second.regs.get(RegisterId(1))
        else {
            panic!("second resumed frame should receive the resolved value");
        };
        assert_eq!(*value, 41);
    }

    #[test]
    fn resolve_promise_returns_error_when_promise_has_already_settled() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let promise_state_id = promise_states.prepare();
        let promise_state = promise_states
            .get_mut(promise_state_id)
            .expect("promise state exists");
        *promise_state = PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
            TestReadyValue::Int(7),
        )));

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            effect_counter: EffectNumber(0),
        };

        let err = runtime
            .resolve_promise(
                promise_state_id,
                PromiseValue::Ready(TestReadyValue::Int(9)),
            )
            .expect_err("already settled promise should reject a second value");

        let SettlePromiseError::PromiseState(SettlePromiseStateError::AlreadySettled(err)) = err
        else {
            panic!("duplicate resolution should surface an already-settled error");
        };

        let PromiseValue::Ready(TestReadyValue::Int(value)) = err.new_value else {
            panic!("rejected value should be returned to the caller");
        };
        assert_eq!(value, 9);
        assert!(runtime.ready.is_empty());

        let PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
            TestReadyValue::Int(value),
        ))) = runtime
            .promise_states
            .get(promise_state_id)
            .expect("promise state exists")
        else {
            panic!("original resolved value should be preserved");
        };
        assert_eq!(*value, 7);
    }

    #[test]
    fn reject_promise_resumes_waiters_with_raised_exceptions() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let promise_state_id = promise_states.prepare();
        let promise_state = promise_states
            .get_mut(promise_state_id)
            .expect("promise state exists");
        *promise_state =
            PromiseState::Waiting(vec![Waiter::Continuation(continuation(RegisterId(0), 3))]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            effect_counter: EffectNumber(0),
        };

        let exception = Exception {
            type_id: "ValueError".to_owned(),
            details: PromiseValue::Ready(TestReadyValue::Int(41)),
        };

        runtime
            .reject_promise(promise_state_id, exception.clone())
            .expect("waiting promise should resolve exceptionally");

        let PromiseState::Settled(SettledPromiseState::Rejected(stored_exception)) = runtime
            .promise_states
            .get(promise_state_id)
            .expect("promise state exists")
        else {
            panic!("promise state should be settled with an exception");
        };
        assert_eq!(stored_exception.type_id, exception.type_id);
        assert_eq!(stored_exception.details, exception.details);

        let resumed = runtime.ready.pop_front().expect("resumed frame");
        let Some(raised_exception) = resumed.exception else {
            panic!("resumed frame should carry a raised exception");
        };
        assert_eq!(raised_exception.type_id, "ValueError");
    }

    #[test]
    fn reject_promise_leaves_the_resumed_frame_raised() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let promise_state_id = promise_states.prepare();
        let promise_state = promise_states
            .get_mut(promise_state_id)
            .expect("promise state exists");
        *promise_state = PromiseState::Waiting(vec![Waiter::Continuation(Continuation::capture(
            frame(0),
            3,
            RegisterId(0),
        ))]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            effect_counter: EffectNumber(0),
        };

        runtime
            .reject_promise(
                promise_state_id,
                Exception {
                    type_id: "ValueError".to_owned(),
                    details: PromiseValue::Ready(TestReadyValue::Int(41)),
                },
            )
            .expect("waiting promise should resolve exceptionally");

        let resumed = runtime.ready.pop_front().expect("resumed frame");
        assert_eq!(resumed.state, 3);
        let Some(exception) = resumed.exception else {
            panic!("resumed frame should carry the raised exception");
        };
        assert_eq!(exception.type_id, "ValueError");
        assert_eq!(
            exception.details,
            PromiseValue::Ready(TestReadyValue::Int(41))
        );
    }

    fn race_arm(race: PromiseStateId, arm_index: i32) -> Waiter<&'static str, usize, TestValue> {
        Waiter::RaceArm {
            race,
            resolution: PromiseValue::Ready(TestReadyValue::Int(arm_index)),
        }
    }

    fn runtime_state(
        promise_states: PromiseStates<&'static str, usize, TestValue>,
    ) -> RuntimeState<&'static str, usize, TestValue> {
        RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            effect_counter: EffectNumber(0),
        }
    }

    #[test]
    fn resolve_promise_fires_race_arms_and_resolves_the_race() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let source = promise_states.prepare();
        let race = promise_states.prepare();
        *promise_states.get_mut(source).expect("source exists") =
            PromiseState::Waiting(vec![race_arm(race, 0)]);
        *promise_states.get_mut(race).expect("race exists") =
            PromiseState::Waiting(vec![Waiter::Continuation(continuation(RegisterId(0), 7))]);

        let mut runtime = runtime_state(promise_states);

        runtime
            .resolve_promise(source, PromiseValue::Ready(TestReadyValue::Int(41)))
            .expect("source promise should resolve");

        assert!(matches!(
            runtime.promise_states.get(race).expect("race exists"),
            PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
                TestReadyValue::Int(arm_index)
            ))) if *arm_index == 0
        ));

        let resumed = runtime.ready.pop_front().expect("race waiter is resumed");
        assert_eq!(resumed.state, 7);
        assert_eq!(
            resumed.regs.get(RegisterId(0)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(0)))
        );
        assert!(resumed.exception.is_none());
    }

    #[test]
    fn reject_promise_fires_race_arms_resolving_the_race() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let source = promise_states.prepare();
        let race = promise_states.prepare();
        *promise_states.get_mut(source).expect("source exists") =
            PromiseState::Waiting(vec![race_arm(race, 1)]);
        *promise_states.get_mut(race).expect("race exists") =
            PromiseState::Waiting(vec![Waiter::Continuation(continuation(RegisterId(0), 7))]);

        let mut runtime = runtime_state(promise_states);

        runtime
            .reject_promise(
                source,
                Exception {
                    type_id: "ValueError".to_owned(),
                    details: PromiseValue::Ready(TestReadyValue::Int(41)),
                },
            )
            .expect("source promise should reject");

        // The race promise resolves with the arm index even though
        // the settlement that fired the arm was a rejection.
        assert!(matches!(
            runtime.promise_states.get(race).expect("race exists"),
            PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
                TestReadyValue::Int(arm_index)
            ))) if *arm_index == 1
        ));

        let resumed = runtime.ready.pop_front().expect("race waiter is resumed");
        assert_eq!(resumed.state, 7);
        assert_eq!(
            resumed.regs.get(RegisterId(0)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(1)))
        );
        assert!(resumed.exception.is_none());
    }

    #[test]
    fn race_arm_that_lost_the_race_is_inert() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let source = promise_states.prepare();
        let race = promise_states.prepare();
        *promise_states.get_mut(source).expect("source exists") = PromiseState::Waiting(vec![
            race_arm(race, 1),
            Waiter::Continuation(continuation(RegisterId(0), 3)),
        ]);
        *promise_states.get_mut(race).expect("race exists") = PromiseState::Settled(
            SettledPromiseState::Resolved(PromiseValue::Ready(TestReadyValue::Int(0))),
        );

        let mut runtime = runtime_state(promise_states);

        runtime
            .resolve_promise(source, PromiseValue::Ready(TestReadyValue::Int(41)))
            .expect("source promise should resolve despite the lost race");

        // The race promise keeps the winning arm's resolution.
        assert!(matches!(
            runtime.promise_states.get(race).expect("race exists"),
            PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
                TestReadyValue::Int(arm_index)
            ))) if *arm_index == 0
        ));

        // The source's other waiters are still notified.
        let resumed = runtime.ready.pop_front().expect("source waiter is resumed");
        assert_eq!(resumed.state, 3);
        assert_eq!(
            resumed.regs.get(RegisterId(0)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(41)))
        );
        assert!(runtime.ready.is_empty());
    }

    #[test]
    fn race_arms_propagate_through_nested_races() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let source = promise_states.prepare();
        let inner_race = promise_states.prepare();
        let outer_race = promise_states.prepare();
        *promise_states.get_mut(source).expect("source exists") =
            PromiseState::Waiting(vec![race_arm(inner_race, 0)]);
        *promise_states
            .get_mut(inner_race)
            .expect("inner race exists") = PromiseState::Waiting(vec![race_arm(outer_race, 5)]);
        *promise_states
            .get_mut(outer_race)
            .expect("outer race exists") =
            PromiseState::Waiting(vec![Waiter::Continuation(continuation(RegisterId(1), 9))]);

        let mut runtime = runtime_state(promise_states);

        runtime
            .resolve_promise(source, PromiseValue::Ready(TestReadyValue::Int(41)))
            .expect("source promise should resolve");

        assert!(matches!(
            runtime
                .promise_states
                .get(inner_race)
                .expect("inner race exists"),
            PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
                TestReadyValue::Int(arm_index)
            ))) if *arm_index == 0
        ));
        assert!(matches!(
            runtime
                .promise_states
                .get(outer_race)
                .expect("outer race exists"),
            PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
                TestReadyValue::Int(arm_index)
            ))) if *arm_index == 5
        ));

        let resumed = runtime.ready.pop_front().expect("outer waiter is resumed");
        assert_eq!(resumed.state, 9);
        assert_eq!(
            resumed.regs.get(RegisterId(1)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(5)))
        );
    }

    #[test]
    fn race_arm_with_missing_target_returns_the_invariant_violation_error() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let source = promise_states.prepare();
        let missing_race = PromiseStateId(9);
        *promise_states.get_mut(source).expect("source exists") =
            PromiseState::Waiting(vec![race_arm(missing_race, 0)]);

        let mut runtime = runtime_state(promise_states);

        let err = runtime
            .resolve_promise(source, PromiseValue::Ready(TestReadyValue::Int(41)))
            .expect_err("dangling race arm should surface the invariant violation");

        let SettlePromiseError::RaceArmTargetNotFound(err) = err else {
            panic!("dangling race arm should report a race-arm-target error");
        };
        assert_eq!(err.promise_state_id, missing_race);
    }
}
