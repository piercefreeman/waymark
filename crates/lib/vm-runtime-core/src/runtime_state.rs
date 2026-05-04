use std::collections::VecDeque;

use crate::{Frame, Promise, PromiseStateId, PromiseStates, ResolvePromiseError};

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
    pub ready: VecDeque<Frame<FunctionId, StateId, Promise<Value>>>,

    /// A state of the promises of this runtime.
    //
    // TODO: the promise states should be garbage-collected together with
    // the promise values that refer to them. We can implement this without
    // a full garbage-collector by holding the promises in a weak-rc-map
    // or something like that.
    pub promise_states: PromiseStates<FunctionId, StateId, Promise<Value>>,
}

impl<FunctionId, StateId, Value> RuntimeState<FunctionId, StateId, Value>
where
    Value: Clone,
{
    /// Provide an async computation value for a given promise.
    ///
    /// Notifies all continuations that wait on it.
    pub fn resolve_promise(
        &mut self,
        promise_state_id: PromiseStateId,
        val: Promise<Value>,
    ) -> Result<(), ResolvePromiseError<Promise<Value>>> {
        let continuations = self.promise_states.resolve(promise_state_id, val.clone())?;

        for continuation in continuations {
            let frame = continuation.resume(val.clone());
            self.ready.push_back(frame);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::RuntimeState;
    use crate::{
        Continuation, Frame, FrameKind, Promise, PromiseState, PromiseStates, RegisterId,
        Registers, ResolvePromiseError,
    };

    fn frame(state: usize) -> Frame<&'static str, usize, Promise<i32>> {
        Frame {
            func: "example",
            state,
            regs: Registers::new(2),
            kind: FrameKind::TopLevel,
        }
    }

    fn continuation(
        dst: RegisterId,
        resume_state: usize,
    ) -> Continuation<&'static str, usize, Promise<i32>, crate::ResumeWithValue> {
        Continuation::capture(frame(0), resume_state, dst)
    }

    #[test]
    fn resolve_promise_enqueues_resumed_frames_and_records_resolved_value() {
        let mut promise_states = PromiseStates::<&'static str, usize, Promise<i32>>::new();
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
            .resolve_promise(promise_state_id, Promise::Resolved(41))
            .expect("waiting promise should resolve");

        assert_eq!(runtime.ready.len(), 2);

        let PromiseState::Ready(Promise::Resolved(value)) = runtime
            .promise_states
            .get(promise_state_id)
            .expect("promise state exists")
        else {
            panic!("promise state should be ready with a resolved value");
        };
        assert_eq!(*value, 41);

        let first = runtime.ready.pop_front().expect("first resumed frame");
        assert_eq!(first.state, 3);
        let Some(Promise::Resolved(value)) = first.regs.get(RegisterId(0)) else {
            panic!("first resumed frame should receive the resolved value");
        };
        assert_eq!(*value, 41);

        let second = runtime.ready.pop_front().expect("second resumed frame");
        assert_eq!(second.state, 5);
        let Some(Promise::Resolved(value)) = second.regs.get(RegisterId(1)) else {
            panic!("second resumed frame should receive the resolved value");
        };
        assert_eq!(*value, 41);
    }

    #[test]
    fn resolve_promise_returns_error_when_promise_is_already_resolved() {
        let mut promise_states = PromiseStates::<&'static str, usize, Promise<i32>>::new();
        let promise_state_id = promise_states.prepare();
        let promise_state = promise_states
            .get_mut(promise_state_id)
            .expect("promise state exists");
        *promise_state = PromiseState::Ready(Promise::Resolved(7));

        let mut runtime = RuntimeState {
            ready: VecDeque::new(),
            promise_states,
        };

        let err = runtime
            .resolve_promise(promise_state_id, Promise::Resolved(9))
            .expect_err("already resolved promise should reject a second value");

        let ResolvePromiseError::AlreadyResolved(err) = err else {
            panic!("duplicate resolution should surface an already-resolved error");
        };

        let Promise::Resolved(value) = err.new_value else {
            panic!("rejected value should be returned to the caller");
        };
        assert_eq!(value, 9);
        assert!(runtime.ready.is_empty());

        let PromiseState::Ready(Promise::Resolved(value)) = runtime
            .promise_states
            .get(promise_state_id)
            .expect("promise state exists")
        else {
            panic!("original resolved value should be preserved");
        };
        assert_eq!(*value, 7);
    }
}
