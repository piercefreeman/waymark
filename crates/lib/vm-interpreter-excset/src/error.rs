use waymark_vm_runtime_core::RegisterId;

/// The error for the [`crate::ExcSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error<RaiseError, CaptureError> {
    /// Managing exception-handler blocks failed.
    #[error("exception handlers: {0}")]
    ExceptionHandlers(#[source] ExceptionHandlersError),

    /// Raising an exception failed.
    #[error("raise: {0}")]
    Raise(#[source] RaiseError),

    /// Capturing a caught exception into its handler's register failed.
    #[error("capture: {0}")]
    Capture(#[source] CaptureError),

    /// Unwinding an uncaught exception out of the frame failed.
    #[error("unwind: {0}")]
    Unwind(#[source] UnwindError),
}

/// Errors produced while managing exception-handler blocks.
#[derive(Debug, thiserror::Error)]
pub enum ExceptionHandlersError {
    /// A pop tried to remove more blocks than were active.
    #[error("pop: {0}")]
    Pop(#[source] waymark_vm_runtime_core::PopExceptionHandlersError),
}

/// Errors produced while evaluating a `Raise` instruction.
#[derive(Debug, thiserror::Error)]
pub enum RaiseError<ValueError> {
    /// The source register was not initialized.
    #[error("source in register {register:?} is not initialized")]
    MissingSource {
        /// The register that was read.
        register: RegisterId,
    },

    /// The source register's value cannot be raised.
    #[error("source value: {0}")]
    Value(#[source] ValueError),
}

/// Errors produced while unwinding an uncaught exception out of a
/// function-call frame.
#[derive(Debug, thiserror::Error)]
pub enum UnwindError {
    /// The destination promise for the function call no longer exists.
    #[error("function call result promise was not found")]
    ReturnPromiseNotFound,

    /// The destination promise for the function call had already settled.
    #[error("function call result promise has already settled")]
    ReturnPromiseAlreadySettled,
}
