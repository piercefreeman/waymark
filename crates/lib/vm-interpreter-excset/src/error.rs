use waymark_vm_runtime_promise_core::UnresolvedPromiseError;

/// The error for the [`crate::ExcSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Evaluating an `IsException` instruction failed.
    #[error("is exception: {0}")]
    IsException(#[source] crate::value::NotAnExceptionTypeIdError),

    /// Evaluating an `ExceptionDetails` instruction failed.
    #[error("exception details: {0}")]
    ExceptionDetails(#[source] waymark_vm_runtime_exception::NotAnExceptionError),

    /// Evaluating a `CatchException` instruction failed.
    #[error("catch exception: {0}")]
    CatchException(#[source] waymark_vm_runtime_exception::NotAnExceptionError),

    /// Raising or propagating an exception failed.
    #[error("raise: {0}")]
    Raise(#[source] RaiseError),
}

/// Errors produced while raising or propagating an exception value.
#[derive(Debug, thiserror::Error)]
pub enum RaiseError {
    /// Raising from a function-call frame failed while resolving the caller's promise.
    #[error("from fn call: {0}")]
    FnCall(#[source] RaiseFnCallError),

    /// Raising from the top-level frame encountered an unresolved promise.
    #[error("toplevel: {0}")]
    TopLevel(#[source] UnresolvedPromiseError),
}

/// Errors produced while propagating an exception out of a function-call frame.
#[derive(Debug, thiserror::Error)]
pub enum RaiseFnCallError {
    /// The destination promise for the function call no longer exists.
    #[error("function call result promise was not found")]
    ReturnPromiseNotFound,

    /// The destination promise for the function call had already been resolved.
    #[error("function call result promise has already been resolved")]
    ReturnPromiseAlreadyResolved,
}
