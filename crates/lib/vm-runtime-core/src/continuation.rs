use crate::{Frame, RegisterId};

/// Captures the ability to resume the execution from the given state when
/// after a certain async value is resolved.
///
/// `Resumer` determines how we resume the execution for this continuation.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResumeWithValue {
    /// The register to assign the resulting value of this continuation to.
    pub dst: RegisterId,
}

/// Specifies that a [`Continuation`] is to resume through one of the arms
/// of a select, chosen at resume time via [`Continuation::into_arm`].
///
/// Typical use is for continuations that suspend on multiple asynchronously
/// obtained values at once, resuming with whichever settles first.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResumeSelectArm;

/// One arm of a select: where the arm's settlement is delivered.
///
/// Originates in the select's [`crate::PromiseWaiter::Select`] entries -
/// planted one per raced promise - and is consumed by
/// [`Continuation::into_arm`] when the arm settles first.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SelectArm<StateId> {
    /// The register to deliver this arm's resolved value to.
    dst: RegisterId,

    /// The state to resume the execution from for this arm.
    resume: StateId,
}

impl<StateId> SelectArm<StateId> {
    /// Create a select arm delivering to the `dst` register and resuming
    /// from the `resume` state.
    ///
    /// Crate-internal on purpose: select arms are only ever minted by
    /// the runtime itself when a select is installed, so they cannot be
    /// forged from the outside.
    pub(crate) fn new(dst: RegisterId, resume: StateId) -> Self {
        Self { dst, resume }
    }
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
        let Self {
            mut prepared_resume_frame,
            resumer: ResumeWithValue { dst },
        } = self;

        // Assign the value to the register where it belongs.
        prepared_resume_frame.regs.set(dst, value);

        prepared_resume_frame
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

    /// Resume the continuation with a raised exception.
    pub fn raise_exception(
        self,
        exception: waymark_vm_runtime_exception::Exception<Value>,
    ) -> Frame<FunctionId, StateId, Value> {
        let Self {
            mut prepared_resume_frame,
            resumer: _,
        } = self;

        // Register the exception in the frame, without
        // overriding an already existing exception.
        prepared_resume_frame.raise_exception(exception);

        prepared_resume_frame
    }

    /// Resume the continuation with a raised exception immediately.
    pub fn immediate_raise_exception(
        frame: &mut Frame<FunctionId, StateId, Value>,
        resume: StateId,
        exception: waymark_vm_runtime_exception::Exception<Value>,
    ) {
        // Prepare the frame to resume execution at the `resume` state.
        frame.state = resume;

        // Register the exception in the frame, without
        // overriding an already existing exception.
        frame.raise_exception(exception);
    }
}

impl<FunctionId, StateId, Value> Continuation<FunctionId, StateId, Value, ResumeSelectArm> {
    /// Capture the given `frame` as a select continuation.
    ///
    /// The frame is not positioned at any resume state yet: the delivery
    /// target arrives with the claiming arm via [`Continuation::into_arm`].
    pub fn capture_select(frame: Frame<FunctionId, StateId, Value>) -> Self {
        Self {
            resumer: ResumeSelectArm,
            prepared_resume_frame: frame,
        }
    }

    /// Choose the arm that settled, producing an ordinary value-resumable
    /// continuation positioned at that arm's resume state.
    pub fn into_arm(
        self,
        arm: SelectArm<StateId>,
    ) -> Continuation<FunctionId, StateId, Value, ResumeWithValue> {
        let Self {
            mut prepared_resume_frame,
            resumer: ResumeSelectArm,
        } = self;
        let SelectArm { dst, resume } = arm;

        // Complete what a plain capture does upfront: position the frame
        // at the chosen arm's resume state.
        prepared_resume_frame.state = resume;

        Continuation {
            resumer: ResumeWithValue { dst },
            prepared_resume_frame,
        }
    }
}

#[cfg(test)]
mod tests {
    use waymark_vm_runtime_exception::Exception;
    use waymark_vm_runtime_promise_value::PromiseValue;

    use super::{Continuation, SelectArm};
    use crate::{ExceptionHandlers, Frame, FrameKind, RegisterId, Registers};

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

    #[test]
    fn capture_and_resume_update_state_and_destination_register() {
        let continuation = Continuation::capture(frame(1), 7, RegisterId(1));

        let resumed = continuation.resume(PromiseValue::Ready(TestReadyValue::Int(42)));

        assert_eq!(resumed.state, 7);
        assert_eq!(resumed.regs.get(RegisterId(0)), None);
        assert_eq!(
            resumed.regs.get(RegisterId(1)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(42)))
        );
        assert!(resumed.exception.is_none());
    }

    #[test]
    fn immediate_resume_updates_frame_in_place() {
        let mut frame = frame(1);

        Continuation::immediate_resume(
            &mut frame,
            9,
            RegisterId(0),
            PromiseValue::Ready(TestReadyValue::Int(11)),
        );

        assert_eq!(frame.state, 9);
        assert_eq!(
            frame.regs.get(RegisterId(0)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(11)))
        );
        assert!(frame.exception.is_none());
    }

    #[test]
    fn exceptional_resume_marks_the_frame_as_raised_without_a_handler() {
        let continuation = Continuation::capture(frame(1), 7, RegisterId(1));

        let resumed = continuation.raise_exception(Exception {
            type_id: "ValueError".to_owned(),
            details: PromiseValue::Ready(TestReadyValue::Int(42)),
        });

        assert_eq!(resumed.state, 7);
        let Some(exception) = resumed.exception else {
            panic!("exceptional resume should raise into the frame");
        };
        assert_eq!(exception.type_id, "ValueError");
        assert_eq!(
            exception.details,
            PromiseValue::Ready(TestReadyValue::Int(42))
        );
    }

    #[test]
    fn exceptional_resume_keeps_the_resume_state_and_marks_the_frame_raised() {
        let continuation = Continuation::capture(frame(1), 7, RegisterId(1));

        let resumed = continuation.raise_exception(Exception {
            type_id: "ValueError".to_owned(),
            details: PromiseValue::Ready(TestReadyValue::Int(42)),
        });

        assert_eq!(resumed.state, 7);
        let Some(exception) = resumed.exception else {
            panic!("exceptional resume should raise into the frame");
        };
        assert_eq!(exception.type_id, "ValueError");
        assert_eq!(
            exception.details,
            PromiseValue::Ready(TestReadyValue::Int(42))
        );
    }

    #[test]
    fn capture_select_leaves_the_frame_state_untouched() {
        let continuation = Continuation::capture_select(frame(1));

        let resumed = continuation
            .into_arm(SelectArm::new(RegisterId(1), 7))
            .resume(PromiseValue::Ready(TestReadyValue::Int(42)));

        assert_eq!(resumed.state, 7);
        assert_eq!(
            resumed.regs.get(RegisterId(1)),
            Some(&PromiseValue::Ready(TestReadyValue::Int(42)))
        );
        assert!(resumed.exception.is_none());
    }

    #[test]
    fn into_arm_positions_the_frame_for_exceptional_resumes_too() {
        let continuation = Continuation::capture_select(frame(1));

        let resumed = continuation
            .into_arm(SelectArm::new(RegisterId(0), 9))
            .raise_exception(Exception {
                type_id: "ValueError".to_owned(),
                details: PromiseValue::Ready(TestReadyValue::Int(42)),
            });

        assert_eq!(resumed.state, 9);
        let Some(exception) = resumed.exception else {
            panic!("exceptional resume should raise into the frame");
        };
        assert_eq!(exception.type_id, "ValueError");
    }
}
