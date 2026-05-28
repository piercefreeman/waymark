use crate::{Frame, RegisterId};

/// A frame resumed from a continuation, together with the destination register
/// that received the resumed value.
pub struct ResumedFrame<FunctionId, StateId, Value> {
    /// The resumed frame.
    pub frame: Frame<FunctionId, StateId, Value>,

    /// The register populated during resume.
    pub dst: RegisterId,
}

/// Captures the ability to resume the execution from the given state when
/// after a certain async value is resolved.
///
/// `Resumer` determines how we resume the execution for this continuation.
pub struct Continuation<FunctionId, StateId, Value, Resumer> {
    /// The frame to resume the execution from.
    prepared_resume_frame: Frame<FunctionId, StateId, Value>,

    /// Holds the state associated with the logic of how we resume from this
    /// continuation.
    resumer: Resumer,
}

/// Specified that a [`Continuation`] is to resume with an value assigned to
/// a frame register.
///
/// Typical use is for continuations that suspend on an asynchronously obtained
/// value.
pub struct ResumeWithValue {
    /// The register to assign the resulting value of this continuation to.
    pub dst: RegisterId,
}

impl<FunctionId, StateId, Value> Continuation<FunctionId, StateId, Value, ResumeWithValue> {
    /// Capture the given `frame` as a continuation, with a `dst` register
    /// to be populated by a resolved value upon coninuing, and the given
    /// `state` to resume the execution from.
    pub fn capture(
        frame: Frame<FunctionId, StateId, Value>,
        resume: StateId,
        dst: RegisterId,
    ) -> Self {
        let mut prepared_resume_frame = frame;

        // Prepare the frame to start executing from the `resume` state.
        prepared_resume_frame.state = resume;

        Self {
            resumer: ResumeWithValue { dst },
            prepared_resume_frame,
        }
    }

    /// Resume the continuation with the provided value.
    pub fn resume(self, value: Value) -> Frame<FunctionId, StateId, Value> {
        self.resume_with_destination(value).frame
    }

    /// Resume the continuation and report the destination register.
    pub fn resume_with_destination(self, value: Value) -> ResumedFrame<FunctionId, StateId, Value> {
        let Self {
            mut prepared_resume_frame,
            resumer: ResumeWithValue { dst },
        } = self;

        // Assign the value to the register where it belongs.
        prepared_resume_frame.regs.set(dst, value);

        ResumedFrame {
            frame: prepared_resume_frame,
            dst,
        }
    }

    /// Resume the continuation with the provided value immediately.
    ///
    /// This call doesn't have a chance to yield control, so it's a given that
    /// this resume happens without suspending.
    pub fn immediate_resume(
        frame: &mut Frame<FunctionId, StateId, Value>,
        resume: StateId,
        dst: RegisterId,
        value: Value,
    ) {
        // Prepare the frame to resume execution at the `resume` state.
        frame.state = resume;

        // Assign the value to the register where it belongs.
        frame.regs.set(dst, value);
    }
}

#[cfg(test)]
mod tests {
    use super::Continuation;
    use crate::{Frame, FrameKind, RegisterId, Registers};

    fn frame(state: usize) -> Frame<&'static str, usize, i32> {
        Frame {
            func: "example",
            state,
            regs: Registers::new(2),
            kind: FrameKind::TopLevel,
        }
    }

    #[test]
    fn capture_and_resume_update_state_and_destination_register() {
        let continuation = Continuation::capture(frame(1), 7, RegisterId(1));

        let resumed = continuation.resume(42);

        assert_eq!(resumed.state, 7);
        assert_eq!(resumed.regs.get(RegisterId(0)), None);
        assert_eq!(resumed.regs.get(RegisterId(1)), Some(&42));
    }

    #[test]
    fn immediate_resume_updates_frame_in_place() {
        let mut frame = frame(1);

        Continuation::immediate_resume(&mut frame, 9, RegisterId(0), 11);

        assert_eq!(frame.state, 9);
        assert_eq!(frame.regs.get(RegisterId(0)), Some(&11));
    }
}
