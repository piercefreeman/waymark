use waymark_vm_runtime_core::PromiseStateNotFoundError;
use waymark_vm_runtime_promise_core::UnresolvedPromiseError;

/// The error for the [`crate::CoreSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error<Spec: waymark_vm_instructions_coreset::Spec> {
    /// Awaiting a promise failed.
    #[error("await: {0}")]
    Await(#[source] AwaitError),

    /// Selecting over promises failed.
    #[error("select: {0}")]
    Select(#[source] SelectError),

    /// JumpIf failed.
    #[error("jump if: {0}")]
    JumpIf(#[source] JumpIfError),

    /// Unwinding the current frame failed.
    #[error("unwind: {0}")]
    Unwind(#[source] waymark_vm_runtime_core::UnwindDepthError),

    /// Resuming a transfer after finalization failed.
    #[error("continue unwind: {0}")]
    ContinueUnwind(#[source] waymark_vm_runtime_core::ContinueUnwindError),

    /// Exiting the current frame failed.
    #[error("frame exit: {0}")]
    FrameExit(#[source] FnExitError),

    /// Raising an exception failed.
    #[error("raise: {0}")]
    Raise(#[source] RaiseError),

    /// A function call failed.
    #[error("function call: {0}")]
    Call(#[source] CallError<Spec::FunctionId>),
}

/// Errors produced while handing a function call invocation.
#[derive(Debug, thiserror::Error)]
pub enum CallError<FunctionId> {
    /// The executable did not contain the requested function.
    #[error("function {function_id:?} not found in the executable")]
    FunctionNotFound {
        /// The function ID that was looked up in the executable.
        function_id: FunctionId,
    },
}

/// Errors produced while evaluating a `Select` instruction.
#[derive(Debug, thiserror::Error)]
pub enum SelectError {
    /// The instruction listed no arms.
    ///
    /// This is a mistake in the bytecode - a select with no arms would
    /// never settle and the selecting frame would suspend forever.
    #[error("select has no arms")]
    EmptyArms,

    /// A pending arm source promise no longer existed in the runtime
    /// state.
    #[error("arm source promise state: {0}")]
    SourcePromiseStateNotFound(#[source] PromiseStateNotFoundError),
}

/// Errors produced while evaluating an `Await` instruction.
#[derive(Debug, thiserror::Error)]
pub enum AwaitError {
    /// The pending promise no longer existed in the runtime state.
    #[error("source promise state: {0}")]
    SourcePromiseStateNotFound(#[source] PromiseStateNotFoundError),
}

/// Errors produced while evaluating a `JumpIf` instruction.
#[derive(Debug, thiserror::Error)]
pub enum JumpIfError {
    /// The resolved condition value could not be interpreted as conditional.
    #[error("condition check: {0}")]
    ConditionCheck(#[source] crate::value::NotAConditionalError),
}

/// Errors produced while evaluating a `Raise` instruction.
#[derive(Debug, thiserror::Error)]
pub enum RaiseError {
    /// The source register did not contain an exception value.
    #[error("source value is not an exception")]
    SourceNotException,
}

/// Errors produced while exiting from a function.
#[derive(Debug, thiserror::Error)]
pub enum FnExitError {
    /// Exiting from a function-call frame failed.
    #[error("from fn call: {0}")]
    FnCall(#[source] ReturnFnCallError),

    /// Exiting from the top-level frame encountered an unresolved promise.
    #[error("toplevel: {0}")]
    TopLevel(#[source] UnresolvedPromiseError),
}

/// Errors produced while completing a function-call return.
#[derive(Debug, thiserror::Error)]
pub enum ReturnFnCallError {
    /// The destination promise for the function call no longer exists.
    #[error("function call result promise was not found")]
    ReturnPromiseNotFound,

    /// The destination promise for the function call had already settled.
    #[error("function call result promise has already settled")]
    ReturnPromiseAlreadySettled,
}
