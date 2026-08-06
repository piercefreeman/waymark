use std::collections::VecDeque;

use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{Frame, PromiseStates, PromiseWaiter, SelectStates, SettlePromiseError};

/// The state shape of the runtime.
///
/// Public struct, but the runtime itself will shield the access to
/// the fields by holding it without public visibility.
///
/// The access to the runtime state is indirectly provided to the interpreters
/// via the [`crate::FullRuntimeView`] and the interpreter-side
/// `CaptureRuntimeView` trait (defined in `waymark-vm-interpreter`).
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

    /// A state of the selects of this runtime.
    pub select_states: SelectStates<FunctionId, StateId, Value>,

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
    /// Notifies all waiters: resumes the continuations and claims the
    /// selects this promise is an arm of - delivering the value either way.
    pub fn resolve_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        value: Value,
    ) -> Result<(), SettlePromiseError<Value>> {
        let waiters = self
            .promise_states
            .resolve(promise_state_id, value.clone())?;

        for waiter in waiters {
            let continuation = match waiter {
                PromiseWaiter::Await(continuation) => continuation,
                PromiseWaiter::Select(select_state_claim) => {
                    // An already-claimed select means this arm lost -
                    // inert by the claimed-exactly-once semantics.
                    let Some(continuation) = self.select_states.claim(select_state_claim) else {
                        continue;
                    };
                    continuation
                }
            };
            let frame = continuation.resume(value.clone());
            self.ready.push_back(frame);
        }

        Ok(())
    }

    /// Reject an async computation for a given promise.
    ///
    /// Notifies all waiters: resumes the continuations and claims the
    /// selects this promise is an arm of - raising the exception either way.
    pub fn reject_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        exception: waymark_vm_runtime_exception::Exception<Value>,
    ) -> Result<(), SettlePromiseError<waymark_vm_runtime_exception::Exception<Value>>> {
        let waiters = self
            .promise_states
            .reject(promise_state_id, exception.clone())?;

        for waiter in waiters {
            let continuation = match waiter {
                PromiseWaiter::Await(continuation) => continuation,
                PromiseWaiter::Select(select_state_claim) => {
                    // An already-claimed select means this arm lost -
                    // inert by the claimed-exactly-once semantics.
                    let Some(continuation) = self.select_states.claim(select_state_claim) else {
                        continue;
                    };
                    continuation
                }
            };
            let frame = continuation.raise_exception(exception.clone());
            self.ready.push_back(frame);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use waymark_vm_runtime_effect::EffectNumber;
    use waymark_vm_runtime_exception::Exception;
    use waymark_vm_runtime_promise_value::PromiseValue;

    use super::RuntimeState;
    use crate::{
        Continuation, ExceptionHandlers, Frame, FrameKind, PromiseState, PromiseStates,
        PromiseWaiter, RegisterId, Registers, SelectStates, SettlePromiseError,
        SettledPromiseState,
    };

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
            PromiseWaiter::Await(continuation(RegisterId(0), 3)),
            PromiseWaiter::Await(continuation(RegisterId(1), 5)),
        ]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            select_states: SelectStates::new(),
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
            select_states: SelectStates::new(),
            effect_counter: EffectNumber(0),
        };

        let err = runtime
            .resolve_promise(
                promise_state_id,
                PromiseValue::Ready(TestReadyValue::Int(9)),
            )
            .expect_err("already settled promise should reject a second value");

        let SettlePromiseError::AlreadySettled(err) = err else {
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
            PromiseState::Waiting(vec![PromiseWaiter::Await(continuation(RegisterId(0), 3))]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            select_states: SelectStates::new(),
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
        *promise_state = PromiseState::Waiting(vec![PromiseWaiter::Await(Continuation::capture(
            frame(0),
            3,
            RegisterId(0),
        ))]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            select_states: SelectStates::new(),
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

    fn runtime_state(
        promise_states: PromiseStates<&'static str, usize, TestValue>,
    ) -> RuntimeState<&'static str, usize, TestValue> {
        RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            select_states: SelectStates::new(),
            effect_counter: EffectNumber(0),
        }
    }

    #[test]
    fn resolve_promise_claims_the_select_and_delivers_to_the_arm() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let source = promise_states.prepare();

        let mut select_states = SelectStates::new();
        let handle = select_states.insert(Continuation::capture_select(frame(0)));

        *promise_states.get_mut(source).expect("source exists") =
            PromiseState::Waiting(vec![PromiseWaiter::Select(handle.arm(RegisterId(1), 7))]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            select_states,
            effect_counter: EffectNumber(0),
        };

        runtime
            .resolve_promise(source, PromiseValue::Ready(TestReadyValue::Int(41)))
            .expect("source promise should resolve");

        let resumed = runtime.ready.pop_front().expect("claimed frame is resumed");
        assert_eq!(resumed.state, 7);
        assert_eq!(
            resumed.regs.get(RegisterId(1)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(41)))
        );
        assert!(resumed.exception.is_none());
    }

    #[test]
    fn reject_promise_claims_the_select_and_raises_at_the_arm() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let source = promise_states.prepare();

        let mut select_states = SelectStates::new();
        let handle = select_states.insert(Continuation::capture_select(frame(0)));

        *promise_states.get_mut(source).expect("source exists") =
            PromiseState::Waiting(vec![PromiseWaiter::Select(handle.arm(RegisterId(1), 7))]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
            select_states,
            effect_counter: EffectNumber(0),
        };

        runtime
            .reject_promise(
                source,
                Exception {
                    type_id: "ValueError".to_owned(),
                    details: PromiseValue::Ready(TestReadyValue::Int(41)),
                },
            )
            .expect("source promise should reject");

        let resumed = runtime.ready.pop_front().expect("claimed frame is resumed");
        assert_eq!(resumed.state, 7);
        let Some(exception) = resumed.exception else {
            panic!("claimed frame should carry the raised exception");
        };
        assert_eq!(exception.type_id, "ValueError");
    }

    #[test]
    fn losing_select_arm_is_inert_and_other_waiters_still_notify() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let winner_source = promise_states.prepare();
        let loser_source = promise_states.prepare();

        let mut select_states = SelectStates::new();
        let handle = select_states.insert(Continuation::capture_select(frame(0)));

        *promise_states
            .get_mut(winner_source)
            .expect("winner source exists") =
            PromiseState::Waiting(vec![PromiseWaiter::Select(handle.arm(RegisterId(0), 3))]);
        *promise_states
            .get_mut(loser_source)
            .expect("loser source exists") = PromiseState::Waiting(vec![
            PromiseWaiter::Select(handle.arm(RegisterId(1), 5)),
            PromiseWaiter::Await(continuation(RegisterId(0), 9)),
        ]);

        let mut runtime = runtime_state(promise_states);
        runtime.select_states = select_states;

        runtime
            .resolve_promise(winner_source, PromiseValue::Ready(TestReadyValue::Int(1)))
            .expect("winner source should resolve");

        let claimed = runtime.ready.pop_front().expect("claimed frame is resumed");
        assert_eq!(claimed.state, 3);
        assert!(runtime.ready.is_empty());

        // The loser arm finds the select already claimed and is inert;
        // the source's other waiters are still notified.
        runtime
            .resolve_promise(loser_source, PromiseValue::Ready(TestReadyValue::Int(2)))
            .expect("loser source should resolve");

        let resumed = runtime.ready.pop_front().expect("plain waiter is resumed");
        assert_eq!(resumed.state, 9);
        assert!(runtime.ready.is_empty(), "the losing arm must not resume");
    }
}
