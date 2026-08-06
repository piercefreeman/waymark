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

    /// Run before each instruction execution.
    ///
    /// Runtime calls this hook before dispatching each instruction. Interpreters
    /// can use it for any pre-instruction behavior. Implementations that do not
    /// need pre-instruction behavior can rely on the default implementation,
    /// which continues with ordinary instruction execution.
    fn before_execute<'r>(
        &self,
        _runtime: Self::RuntimeView<'r>,
        frame: Self::Frame,
    ) -> Result<ExecutionOutcome<Self::Frame, Self::Effect>, Self::Error> {
        Ok(ExecutionOutcome::Continue(frame))
    }

    /// Run after each instruction execution that continued in the same state.
    ///
    /// Runtime calls this hook after an instruction executes and returns
    /// [`ExecutionOutcome::Continue`] without a state transition. Interpreters
    /// can use it to react to side-effects that an instruction may have placed
    /// on the frame without returning early — for example, raising an exception
    /// via register mutation. Implementations that do not need post-instruction
    /// behavior can rely on the default implementation, which continues with
    /// ordinary instruction execution.
    fn after_execute<'r>(
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

/// Capture an interpreter's runtime view from an exclusive borrow of
/// a source view.
///
/// Implemented by an interpreter for every source view it can capture its
/// own (typically reduced) [`Interpreter::RuntimeView`] from. The runtime
/// uses this to hand the interpreter the view it actually needs, from
/// whatever view the runtime holds; a composite interpreter uses it the
/// same way for each of its sub-interpreters.
///
/// Capturing borrows the source exclusively rather than consuming it, so
/// one source view can serve any number of sequential captures — which is
/// what lets a composite drive all of its sub-interpreters from the single
/// view it holds.
pub trait CaptureRuntimeView<'source, SourceView> {
    /// The captured runtime view.
    type Captured;

    /// Capture the runtime view from the source view.
    fn capture_runtime_view(source: &'source mut SourceView) -> Self::Captured;
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
