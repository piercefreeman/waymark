use waymark_vm_bytecode::StateId;

use crate::{Frame, RegisterId};

/// Captures the ability to resume the execution from the given state when
/// after a certain async value is resolved.
pub struct Continuation<Value> {
    /// The frame to resume the execution from.
    prepared_resume_frame: Frame<Value>,

    /// The register to assign the resulting value of this continuation to.
    dst: RegisterId,
}

impl<Value> Continuation<Value> {
    pub fn capture(frame: Frame<Value>, dst: RegisterId, resume: StateId) -> Self {
        let mut prepared_resume_frame = frame;

        // Prepare the frame to resume execution at the `resume` state.
        prepared_resume_frame.state = resume;

        Self {
            dst,
            prepared_resume_frame,
        }
    }

    pub fn resume(self, value: Value) -> Frame<Value> {
        let Self {
            mut prepared_resume_frame,
            dst,
        } = self;

        // Assign the value to the register where it belongs.
        prepared_resume_frame.regs[dst] = value;

        prepared_resume_frame
    }

    pub fn immediate_resume(
        frame: &mut Frame<Value>,
        dst: RegisterId,
        resume: StateId,
        value: Value,
    ) {
        // Prepare the frame to resume execution at the `resume` state.
        frame.state = resume;

        // Assign the value to the register where it belongs.
        frame.regs[dst] = value;
    }
}
