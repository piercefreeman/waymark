use waymark_vm_runtime_core::{PromiseStateNotFoundError, UnresolvedPromiseError};

/// The error for the [`crate::CoreSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error<Spec: waymark_vm_instructions_coreset::Spec> {
    /// Invoking an extcall failed.
    #[error("extcall: {0}")]
    ExtCall(#[source] ExtCallError),

    /// Awaiting a promise failed.
    #[error("await: {0}")]
    Await(#[source] AwaitError),

    /// Returning from a function failed.
    #[error("return: {0}")]
    Return(#[source] ReturnError),

    /// JumpIf failed.
    #[error("jump if: {0}")]
    JumpIf(#[source] JumpIfError),

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
    /// The condition register still held an unresolved promise.
    #[error("unresolved conditional value: {0}")]
    UnresolvedConditionPromise(#[source] UnresolvedPromiseError),

    /// The resolved condition value could not be interpreted as conditional.
    #[error("condition check: {0}")]
    ConditionCheck(#[source] crate::value::NotAConditionalError),
}

/// Errors produced while preparing an extcall invocation.
#[derive(Debug, thiserror::Error)]
pub enum ExtCallError {
    /// An extcall argument still held an unresolved promise.
    #[error("unresolved promise argument at position {arg_pos}: {source}")]
    UnresolvedPromiseArgument {
        /// The zero-based argument position that failed to resolve.
        arg_pos: usize,

        /// The underlying unresolved promise error for the argument.
        #[source]
        source: UnresolvedPromiseError,
    },
}

/// Errors produced while returning from a frame.
#[derive(Debug, thiserror::Error)]
pub enum ReturnError {
    /// Returning from a function-call frame failed.
    #[error("from fn call: {0}")]
    FnCall(#[source] ReturnFnCallError),

    /// Returning from the top-level frame encountered an unresolved promise.
    #[error("toplevel: {0}")]
    TopLevel(#[source] UnresolvedPromiseError),
}

/// Errors produced while completing a function-call return.
#[derive(Debug, thiserror::Error)]
pub enum ReturnFnCallError {
    /// The destination promise for the function call no longer exists.
    #[error("function call result promise was not found")]
    ReturnPromiseNotFound,

    /// The destination promise for the function call had already been resolved.
    #[error("function call result promise has already been resolved")]
    ReturnPromiseAlreadyResolved,
}
