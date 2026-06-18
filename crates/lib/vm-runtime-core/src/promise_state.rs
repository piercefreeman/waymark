/// An errors that occurs when an attempt to resolve an already resolved
/// promise is made.
///
/// Resolving a promise is idempotent, so you could ignore this error if there
/// is no strong need for the promise to be resolved with *this* particular
/// operation. The consequence of this error is that the value that was
/// provided for resolving the promise has not been able to be stored in
/// the promise.
#[derive(Debug, thiserror::Error)]
#[error("resolving an already resolved promise")]
pub struct ResolvingAlreadyResolvedPromiseError<Value> {
    /// The new value that has not been able to be stored in
    /// the already-resolved promise.
    pub new_value: Value,
}

/// A runtime internal state associated with a promise.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PromiseState<FunctionId, StateId, Value> {
    /// A list of continuations to resume when a promise resolves.
    ///
    /// Awaiting on it will add the frame to the list of continuations.
    Waiting(Vec<crate::Continuation<FunctionId, StateId, Value, crate::ResumeWithValue>>),

    /// A promise that has been resolved successfully.
    ///
    /// Awaiting on it will resume immediately.
    Resolved(Value),

    /// A promise that has been rejected with an exception.
    ///
    /// Awaiting on it will resume immediately by raising the exception.
    Rejected(waymark_vm_runtime_exception::Exception<Value>),
}

impl<FunctionId, StateId, Value> PromiseState<FunctionId, StateId, Value> {
    /// Idempotently resolve a promise.
    ///
    /// Returns a list of continuations to resume, or an error is this promise
    /// has already been resolved.
    #[allow(clippy::type_complexity)]
    pub fn resolve(
        &mut self,
        value: Value,
    ) -> Result<
        Vec<crate::Continuation<FunctionId, StateId, Value, crate::ResumeWithValue>>,
        ResolvingAlreadyResolvedPromiseError<Value>,
    > {
        let replaced = std::mem::replace(self, Self::Resolved(value));
        let continuations = match replaced {
            PromiseState::Waiting(continuations) => continuations,
            PromiseState::Resolved(old_value) => {
                // This shouldn't happen often.
                std::hint::cold_path();

                // Replace the value back as we want first-wins semantics.
                let new_value = std::mem::replace(self, Self::Resolved(old_value));

                let Self::Resolved(new_value) = new_value else {
                    unreachable!();
                };

                return Err(ResolvingAlreadyResolvedPromiseError { new_value });
            }
            PromiseState::Rejected(old_exception) => {
                std::hint::cold_path();

                let new_value = std::mem::replace(self, Self::Rejected(old_exception));

                let Self::Resolved(new_value) = new_value else {
                    unreachable!();
                };

                return Err(ResolvingAlreadyResolvedPromiseError { new_value });
            }
        };
        Ok(continuations)
    }

    /// Idempotently reject a promise.
    #[allow(clippy::type_complexity)]
    pub fn reject(
        &mut self,
        exception: waymark_vm_runtime_exception::Exception<Value>,
    ) -> Result<
        Vec<crate::Continuation<FunctionId, StateId, Value, crate::ResumeWithValue>>,
        ResolvingAlreadyResolvedPromiseError<waymark_vm_runtime_exception::Exception<Value>>,
    > {
        let replaced = std::mem::replace(self, Self::Rejected(exception));
        let continuations = match replaced {
            PromiseState::Waiting(continuations) => continuations,
            PromiseState::Resolved(old_value) => {
                std::hint::cold_path();

                let new_exception = std::mem::replace(self, Self::Resolved(old_value));

                let Self::Rejected(new_exception) = new_exception else {
                    unreachable!();
                };

                return Err(ResolvingAlreadyResolvedPromiseError {
                    new_value: new_exception,
                });
            }
            PromiseState::Rejected(old_exception) => {
                std::hint::cold_path();

                let new_exception = std::mem::replace(self, Self::Rejected(old_exception));

                let Self::Rejected(new_exception) = new_exception else {
                    unreachable!();
                };

                return Err(ResolvingAlreadyResolvedPromiseError {
                    new_value: new_exception,
                });
            }
        };
        Ok(continuations)
    }
}

#[cfg(test)]
mod tests {
    use waymark_vm_runtime_exception::Exception;
    use waymark_vm_runtime_promise_value::PromiseValue;

    use super::PromiseState;
    use crate::{Continuation, ExceptionHandlers, Frame, FrameKind, RegisterId, Registers};

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
    fn resolve_waiting_promise_returns_continuations_and_marks_ready() {
        let mut state = PromiseState::Waiting(vec![continuation(RegisterId(1), 3)]);

        let continuations = state
            .resolve(PromiseValue::Ready(TestReadyValue::Int(17)))
            .expect("waiting promise should resolve");

        assert_eq!(continuations.len(), 1);
        assert!(matches!(
            &state,
            PromiseState::Resolved(PromiseValue::Ready(TestReadyValue::Int(value))) if *value == 17
        ));

        let resumed = continuations
            .into_iter()
            .next()
            .expect("continuation is returned")
            .resume(PromiseValue::Ready(TestReadyValue::Int(17)));
        assert_eq!(resumed.state, 3);
        assert_eq!(
            resumed.regs.get(RegisterId(1)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(17)))
        );
    }

    #[test]
    fn resolve_ready_promise_returns_error_and_preserves_original_value() {
        let mut state = PromiseState::<&'static str, usize, TestValue>::Resolved(
            PromiseValue::Ready(TestReadyValue::Int(5)),
        );

        let err = match state.resolve(PromiseValue::Ready(TestReadyValue::Int(9))) {
            Ok(_) => panic!("resolved promise should reject a second value"),
            Err(err) => err,
        };

        assert!(matches!(
            err.new_value,
            PromiseValue::Ready(TestReadyValue::Int(9))
        ));
        assert!(matches!(
            &state,
            PromiseState::Resolved(PromiseValue::Ready(TestReadyValue::Int(value))) if *value == 5
        ));
    }

    #[test]
    fn resolve_waiting_promise_preserves_exception_results() {
        let mut state = PromiseState::Waiting(vec![continuation(RegisterId(1), 3)]);

        let continuations = state
            .reject(Exception {
                type_id: "ValueError".to_owned(),
                details: PromiseValue::Ready(TestReadyValue::Int(17)),
            })
            .expect("waiting promise should resolve exceptionally");

        assert!(matches!(
            &state,
            PromiseState::Rejected(Exception { type_id, details })
                if type_id == "ValueError"
                    && *details == PromiseValue::Ready(TestReadyValue::Int(17))
        ));

        let resumed = continuations
            .into_iter()
            .next()
            .expect("continuation is returned")
            .raise_exception(Exception {
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
