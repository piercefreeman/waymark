/// The settled outcome of a promise.
///
/// A promise settles at most once, with either of the two kinds of outcomes.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SettledPromiseState<Value> {
    /// The promise has been resolved successfully with a value.
    Resolved(Value),

    /// The promise has been rejected with an exception.
    Rejected(waymark_vm_runtime_exception::Exception<Value>),
}

/// An error that occurs when an attempt to settle an already settled
/// promise is made.
///
/// Settling a promise is idempotent, so you could ignore this error if there
/// is no strong need for the promise to be settled with *this* particular
/// operation. The consequence of this error is that the value that was
/// provided for settling the promise has not been able to be stored in
/// the promise.
#[derive(Debug, thiserror::Error)]
#[error("settling an already settled promise")]
pub struct SettlingAlreadySettledPromiseError<Value> {
    /// The new value that has not been able to be stored in
    /// the already-settled promise.
    pub new_value: Value,
}

/// A runtime internal state associated with a promise.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PromiseState<FunctionId, StateId, Value> {
    /// A list of waiters to notify when a promise settles.
    ///
    /// Awaiting on it will add the frame to the list of waiters.
    Waiting(Vec<crate::PromiseWaiter<FunctionId, StateId, Value>>),

    /// A promise that has settled.
    ///
    /// Awaiting on it will resume immediately - with the value on
    /// a resolution, or by raising the exception on a rejection.
    Settled(SettledPromiseState<Value>),
}

impl<FunctionId, StateId, Value> PromiseState<FunctionId, StateId, Value> {
    /// Idempotently resolve a promise.
    ///
    /// Returns a list of waiters to notify, or an error if this promise
    /// has already settled.
    pub fn resolve(
        &mut self,
        value: Value,
    ) -> Result<
        Vec<crate::PromiseWaiter<FunctionId, StateId, Value>>,
        SettlingAlreadySettledPromiseError<Value>,
    > {
        let replaced = std::mem::replace(self, Self::Settled(SettledPromiseState::Resolved(value)));
        match replaced {
            PromiseState::Waiting(waiters) => Ok(waiters),
            PromiseState::Settled(original) => {
                // This shouldn't happen often.
                std::hint::cold_path();

                // Replace the original settlement back as we want
                // first-wins semantics.
                let new_settlement = std::mem::replace(self, Self::Settled(original));

                let Self::Settled(SettledPromiseState::Resolved(new_value)) = new_settlement else {
                    unreachable!();
                };

                Err(SettlingAlreadySettledPromiseError { new_value })
            }
        }
    }

    /// Idempotently reject a promise.
    ///
    /// Returns a list of waiters to notify, or an error if this promise
    /// has already settled.
    #[expect(
        clippy::type_complexity,
        reason = "we purposely avoid alias for the error"
    )]
    pub fn reject(
        &mut self,
        exception: waymark_vm_runtime_exception::Exception<Value>,
    ) -> Result<
        Vec<crate::PromiseWaiter<FunctionId, StateId, Value>>,
        SettlingAlreadySettledPromiseError<waymark_vm_runtime_exception::Exception<Value>>,
    > {
        let replaced = std::mem::replace(
            self,
            Self::Settled(SettledPromiseState::Rejected(exception)),
        );
        match replaced {
            PromiseState::Waiting(waiters) => Ok(waiters),
            PromiseState::Settled(original) => {
                // This shouldn't happen often.
                std::hint::cold_path();

                // Replace the original settlement back as we want
                // first-wins semantics.
                let new_settlement = std::mem::replace(self, Self::Settled(original));

                let Self::Settled(SettledPromiseState::Rejected(new_value)) = new_settlement else {
                    unreachable!();
                };

                Err(SettlingAlreadySettledPromiseError { new_value })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use waymark_vm_runtime_exception::Exception;
    use waymark_vm_runtime_promise_value::PromiseValue;

    use super::{PromiseState, SettledPromiseState};
    use crate::{
        Continuation, ExceptionHandlers, Frame, FrameKind, PromiseWaiter, RegisterId, Registers,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestReadyValue {
        Int(i32),
    }

    type TestValue = PromiseValue<TestReadyValue>;

    impl waymark_vm_runtime_value::RootValueAccess for TestReadyValue {
        type RootValue = TestValue;
    }

    fn continuation(
        dst: RegisterId,
        resume_state: usize,
    ) -> Continuation<&'static str, usize, TestValue, crate::ResumeWithValue> {
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
    fn resolve_waiting_promise_returns_continuations_and_marks_resolved() {
        let mut state =
            PromiseState::Waiting(vec![PromiseWaiter::Await(continuation(RegisterId(1), 3))]);

        let continuations = state
            .resolve(PromiseValue::Ready(TestReadyValue::Int(17)))
            .expect("waiting promise should resolve");

        assert_eq!(continuations.len(), 1);
        assert!(matches!(
            &state,
            PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
                TestReadyValue::Int(value)
            ))) if *value == 17
        ));

        let Some(PromiseWaiter::Await(continuation)) = continuations.into_iter().next() else {
            panic!("continuation waiter is returned");
        };
        let resumed = continuation.resume(PromiseValue::Ready(TestReadyValue::Int(17)));
        assert_eq!(resumed.state, 3);
        assert_eq!(resumed.regs.get(RegisterId(0)), None);
        assert_eq!(
            resumed.regs.get(RegisterId(1)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(17)))
        );
        assert!(resumed.exception.is_none());
    }

    #[test]
    fn resolve_settled_promise_returns_error_and_preserves_original_settlement() {
        let mut state = PromiseState::<&'static str, usize, TestValue>::Settled(
            SettledPromiseState::Resolved(PromiseValue::Ready(TestReadyValue::Int(5))),
        );

        let err = match state.resolve(PromiseValue::Ready(TestReadyValue::Int(9))) {
            Ok(_) => panic!("settled promise should reject a second settlement"),
            Err(err) => err,
        };

        assert!(matches!(
            err.new_value,
            PromiseValue::Ready(TestReadyValue::Int(9))
        ));
        assert!(matches!(
            &state,
            PromiseState::Settled(SettledPromiseState::Resolved(PromiseValue::Ready(
                TestReadyValue::Int(value)
            ))) if *value == 5
        ));
    }

    #[test]
    fn reject_waiting_promise_preserves_exceptional_settlements() {
        let mut state =
            PromiseState::Waiting(vec![PromiseWaiter::Await(continuation(RegisterId(1), 3))]);

        let continuations = state
            .reject(Exception {
                type_id: "ValueError".to_owned(),
                details: PromiseValue::Ready(TestReadyValue::Int(17)),
            })
            .expect("waiting promise should settle exceptionally");

        assert!(matches!(
            &state,
            PromiseState::Settled(SettledPromiseState::Rejected(Exception { type_id, details }))
                if type_id == "ValueError"
                    && *details == PromiseValue::Ready(TestReadyValue::Int(17))
        ));

        let Some(PromiseWaiter::Await(continuation)) = continuations.into_iter().next() else {
            panic!("continuation waiter is returned");
        };
        let resumed = continuation.raise_exception(Exception {
            type_id: "ValueError".to_owned(),
            details: PromiseValue::Ready(TestReadyValue::Int(17)),
        });

        let Some(exception) = resumed.exception else {
            panic!("exceptional resume should raise into the frame");
        };
        assert_eq!(exception.type_id, "ValueError");
        assert_eq!(
            exception.details,
            PromiseValue::Ready(TestReadyValue::Int(17))
        );
    }
}
