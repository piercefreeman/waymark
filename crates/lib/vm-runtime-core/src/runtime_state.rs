use std::collections::VecDeque;

use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{
    Frame, PromiseStates, RejectPromiseError, ResolvePromiseError,
    ResolvingAlreadyResolvedPromiseError,
};

/// The state shape of the runtime.
///
/// Public struct, but the runtime itself will shield the access to
/// the fields by holding it without public visibility.
///
/// The access to the runtime state is indirectly provided to the interpreters
/// via the [`crate::CaptureRuntimeView`].
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
}

impl<FunctionId, StateId, Value> RuntimeState<FunctionId, StateId, Value>
where
    Value: Clone,
    StateId: Copy,
{
    /// Provide an async computation value for a given promise.
    ///
    /// Notifies all continuations that wait on it.
    pub fn resolve_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        value: Value,
    ) -> Result<(), ResolvePromiseError<Value>> {
        let continuations = self
            .promise_states
            .resolve(promise_state_id, value.clone())
            .map_err(|error| match error {
                ResolvePromiseError::PromiseStateNotFound(error) => {
                    ResolvePromiseError::PromiseStateNotFound(error)
                }
                ResolvePromiseError::AlreadyResolved(error) => {
                    let ResolvingAlreadyResolvedPromiseError { new_value } = error;
                    ResolvePromiseError::AlreadyResolved(ResolvingAlreadyResolvedPromiseError {
                        new_value,
                    })
                }
            })?;

        for continuation in continuations {
            let frame = continuation.resume(value.clone());
            self.ready.push_back(frame);
        }

        Ok(())
    }
}

impl<FunctionId, StateId, Value> RuntimeState<FunctionId, StateId, Value>
where
    Value: Clone,
    StateId: Copy,
{
    /// Reject an async computation for a given promise.
    ///
    /// Notifies all continuations that wait on it.
    pub fn reject_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        exception: waymark_vm_runtime_exception::Exception<Value>,
    ) -> Result<(), RejectPromiseError<Value>> {
        let continuations = self
            .promise_states
            .reject(promise_state_id, exception.clone())?;

        for continuation in continuations {
            let frame = continuation.raise_exception(exception.clone());
            self.ready.push_back(frame);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use waymark_vm_runtime_exception::Exception;
    use waymark_vm_runtime_promise_value::PromiseValue;

    use super::RuntimeState;
    use crate::{
        Continuation, Frame, FrameKind, PromiseState, PromiseStates, RegisterId, Registers,
        ResolvePromiseError,
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
            exception_handler_blocks: Vec::new(),
            exception: None,
            kind: FrameKind::TopLevel,
        }
    }

    fn continuation(
        dst: RegisterId,
        resume_state: usize,
    ) -> Continuation<&'static str, usize, TestValue, crate::ResumeWithAwait<usize>> {
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
            continuation(RegisterId(0), 3),
            continuation(RegisterId(1), 5),
        ]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
        };

        runtime
            .resolve_promise(
                promise_state_id,
                PromiseValue::Ready(TestReadyValue::Int(41)),
            )
            .expect("waiting promise should resolve");

        assert_eq!(runtime.ready.len(), 2);

        let PromiseState::Resolved(PromiseValue::Ready(TestReadyValue::Int(value))) = runtime
            .promise_states
            .get(promise_state_id)
            .expect("promise state exists")
        else {
            panic!("promise state should be ready with a resolved value");
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
    fn resolve_promise_returns_error_when_promise_is_already_resolved() {
        let mut promise_states = PromiseStates::<&'static str, usize, TestValue>::new();
        let promise_state_id = promise_states.prepare();
        let promise_state = promise_states
            .get_mut(promise_state_id)
            .expect("promise state exists");
        *promise_state = PromiseState::Resolved(PromiseValue::Ready(TestReadyValue::Int(7)));

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
        };

        let err = runtime
            .resolve_promise(
                promise_state_id,
                PromiseValue::Ready(TestReadyValue::Int(9)),
            )
            .expect_err("already resolved promise should reject a second value");

        let ResolvePromiseError::AlreadyResolved(err) = err else {
            panic!("duplicate resolution should surface an already-resolved error");
        };

        let PromiseValue::Ready(TestReadyValue::Int(value)) = err.new_value else {
            panic!("rejected value should be returned to the caller");
        };
        assert_eq!(value, 9);
        assert!(runtime.ready.is_empty());

        let PromiseState::Resolved(PromiseValue::Ready(TestReadyValue::Int(value))) = runtime
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
        *promise_state = PromiseState::Waiting(vec![continuation(RegisterId(0), 3)]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
        };

        let exception = Exception {
            type_id: "ValueError".to_owned(),
            details: PromiseValue::Ready(TestReadyValue::Int(41)),
        };

        runtime
            .reject_promise(promise_state_id, exception.clone())
            .expect("waiting promise should resolve exceptionally");

        let PromiseState::Rejected(stored_exception) = runtime
            .promise_states
            .get(promise_state_id)
            .expect("promise state exists")
        else {
            panic!("promise state should be ready with an exception");
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
        *promise_state =
            PromiseState::Waiting(vec![Continuation::capture(frame(0), 3, RegisterId(0))]);

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
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
}
