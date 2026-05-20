use waymark_vm_runtime_promise_core::UnresolvedPromiseError;

/// The error for the [`crate::ExtCallSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error<Value: crate::Value> {
    /// Preparing an action call failed.
    #[error("action call: {0}")]
    ActionCall(
        #[source] ActionCallError<<Value as crate::value::CaptureActionCallArgument>::Error>,
    ),

    /// Preparing a sleep suspension failed.
    #[error("sleep: {0}")]
    Sleep(#[source] SleepError<<Value as crate::value::SleepDuration>::Error>),
}

/// Errors produced while preparing an action call invocation.
#[derive(Debug, thiserror::Error)]
pub enum ActionCallError<ArgumentCaptureError> {
    /// An action-call argument couldn't be captured.
    #[error("unable to capture argument at position {arg_pos}: {source}")]
    ArgumentCapture {
        /// The zero-based argument position that failed to resolve.
        arg_pos: usize,

        /// The underlying argument capture error.
        #[source]
        source: ArgumentCaptureError,
    },
}

/// Errors produced while preparing a sleep suspension.
#[derive(Debug, thiserror::Error)]
pub enum SleepError<DurationError> {
    /// The requested sleep duration still held an unresolved promise.
    #[error("unresolved promise duration: {source}")]
    UnresolvedPromiseDuration {
        /// The underlying unresolved promise error for the duration.
        #[source]
        source: UnresolvedPromiseError,
    },

    /// The resolved duration value could not be converted to a concrete delay.
    #[error("invalid sleep duration: {source}")]
    InvalidDuration {
        /// The underlying value-conversion failure.
        #[source]
        source: DurationError,
    },
}
