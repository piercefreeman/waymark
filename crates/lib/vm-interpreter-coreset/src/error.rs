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

    /// Returning from a function failed.
    #[error("return: {0}")]
    Return(#[source] FnExitError),

    /// JumpIf failed.
    #[error("jump if: {0}")]
    JumpIf(#[source] JumpIfError),

    /// Managing exception-handler blocks failed.
    #[error("exception handlers: {0}")]
    ExceptionHandlers(#[source] ExceptionHandlersError),

    /// Calling or returning from a shared state failed.
    #[error("state call: {0}")]
    StateCall(#[source] StateCallError),

    /// Bubbling a raised exception failed.
    #[error("bubble exception: {0}")]
    BubbleException(#[source] FnExitError),

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

/// Errors produced while managing exception-handler blocks.
#[derive(Debug, thiserror::Error)]
pub enum ExceptionHandlersError {
    /// A pop tried to remove more blocks than were active.
    #[error("pop: {0}")]
    Pop(#[source] waymark_vm_runtime_core::PopExceptionHandlersError),
}

/// Errors produced while calling or returning from shared states.
#[derive(Debug, thiserror::Error)]
pub enum StateCallError {
    /// Bytecode referred to a nonexistent state-call depth.
    #[error("depth: {0}")]
    Depth(#[source] waymark_vm_runtime_core::StateCallDepthError),

    /// Bytecode tried to return when no shared state was active.
    #[error("return: {0}")]
    Return(#[source] waymark_vm_runtime_core::ReturnStateError),

    /// A state requested an exception-handler depth above the active depth.
    #[error("exception handler depth {target} exceeds active depth {active}")]
    ExceptionHandlerDepth {
        /// Requested handler depth.
        target: usize,

        /// Active handler depth.
        active: usize,
    },
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
