use crate::{Continuation, ResumeWithValue};

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
pub enum PromiseState<FunctionId, StateId, Value> {
    /// A list of continuations to resume when a promise resolves.
    ///
    /// Awaiting on it will add the frame to the list of continuations.
    Waiting(Vec<Continuation<FunctionId, StateId, Value, ResumeWithValue>>),

    /// A promise that is ready.
    ///
    /// Awaiting on it will resume immediately.
    Ready(Value),
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
        let ready = Self::Ready(value);
        let replaced = std::mem::replace(self, ready);
        let continuations = match replaced {
            PromiseState::Waiting(continuations) => continuations,
            PromiseState::Ready(old_value) => {
                // This shouldn't happen often.
                std::hint::cold_path();

                // Replace the value back as we want first-wins semantics.
                let new_value = std::mem::replace(self, Self::Ready(old_value));

                // Match on the value we've just provided, guaranteed to be
                // `Ready`.
                let Self::Ready(new_value) = new_value else {
                    unreachable!();
                };

                return Err(ResolvingAlreadyResolvedPromiseError { new_value });
            }
        };
        Ok(continuations)
    }
}

#[cfg(test)]
mod tests {
    use super::PromiseState;
    use crate::{Continuation, Frame, FrameKind, RegisterId, Registers};

    fn continuation(
        dst: RegisterId,
        resume_state: usize,
    ) -> Continuation<&'static str, usize, i32, crate::ResumeWithValue> {
        Continuation::capture(
            Frame {
                func: "example",
                state: 0,
                regs: Registers::new(2),
                kind: FrameKind::TopLevel,
            },
            resume_state,
            dst,
        )
    }

    #[test]
    fn resolve_waiting_promise_returns_continuations_and_marks_ready() {
        let mut state = PromiseState::Waiting(vec![continuation(RegisterId(1), 3)]);

        let continuations = state.resolve(17).expect("waiting promise should resolve");

        assert_eq!(continuations.len(), 1);
        assert!(matches!(&state, PromiseState::Ready(value) if *value == 17));

        let resumed = continuations
            .into_iter()
            .next()
            .expect("continuation is returned")
            .resume(17);
        assert_eq!(resumed.state, 3);
        assert_eq!(resumed.regs.get(RegisterId(1)), Some(&17));
    }

    #[test]
    fn resolve_ready_promise_returns_error_and_preserves_original_value() {
        let mut state = PromiseState::<&'static str, usize, i32>::Ready(5);

        let err = match state.resolve(9) {
            Ok(_) => panic!("resolved promise should reject a second value"),
            Err(err) => err,
        };

        assert_eq!(err.new_value, 9);
        assert!(matches!(&state, PromiseState::Ready(value) if *value == 5));
    }
}
