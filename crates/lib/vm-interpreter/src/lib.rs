//! [`Interpreter`] trait definition.

#![warn(missing_docs)]

/// The interpreter for a given instruction set.
///
/// Semantically, this is the building block that enables us to execute
/// a particular set of instructions.
///
/// Runtime is configured with an interpreter that specifies,
/// and it defines which particular instruction set will be supported.
///
/// Should hold the static state (i.e. extcall tables) required
/// for interpreting a particular instruction set.
pub trait Interpreter {
    /// The view into the runtime that this interpreter needs access to
    /// in order to execute the instructions.
    type RuntimeView<'r>: 'r;

    /// The frame type to execute the instructions on.
    type Frame;

    /// The instruction type that this interpreter understands to how execute.
    ///
    /// Typically this would be an enum representing an instruction set.
    type Instruction;

    /// The error that can occur while executing an instruction.
    type Error;

    /// The side-effect that an execution of an in instruction with this
    /// interpreter can trigger.
    type Effect;

    /// Enter the current frame state before instruction dispatch.
    ///
    /// Runtime calls this hook before it starts executing the instructions of
    /// the current frame state. Interpreters can use it for any state-entry
    /// behavior, including but not limited to pending-exception handling.
    /// Implementations that do not need state-entry behavior can rely on the
    /// default implementation, which continues with ordinary instruction
    /// execution.
    fn enter_state<'r>(
        &self,
        _runtime: Self::RuntimeView<'r>,
        frame: Self::Frame,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        Ok(ExecutionOutcome::Continue(frame))
    }

    /// Execute the instruction on a given frame.
    fn execute<'r>(
        &self,
        runtime: Self::RuntimeView<'r>,
        frame: Self::Frame,
        instruction: &Self::Instruction,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error>;
}

/// The outcome of an execution of a single instruction or hook.
pub enum ExecutionOutcome<Frame, Effect> {
    /// Continue executing this frame.
    Continue(Frame),

    /// Exit the frame, and continue with the next one.
    ExitFrame,

    /// Exit the frame and emit a side-effect.
    ExitFrameWithEffect(Effect),
}

impl<Frame, Effect> ExecutionOutcome<Frame, Effect> {
    /// The map one effect to the other leaving the variants as-is.
    pub fn map_effect<OtherEffect>(
        self,
        f: impl FnOnce(Effect) -> OtherEffect,
    ) -> ExecutionOutcome<Frame, OtherEffect> {
        match self {
            ExecutionOutcome::Continue(frame) => ExecutionOutcome::Continue(frame),
            ExecutionOutcome::ExitFrame => ExecutionOutcome::ExitFrame,
            ExecutionOutcome::ExitFrameWithEffect(effect) => {
                ExecutionOutcome::ExitFrameWithEffect(f(effect))
            }
        }
    }
}
