/// The error for the [`crate::ExtCallSetInterpreter`].
#[derive_where::derive_where(Debug)]
#[derive(thiserror::Error)]
pub enum Error<Operations, Value>
where
    Operations: crate::operations::Operations<Value>,
{
    /// Preparing an action call failed.
    #[error("action call: {0}")]
    ActionCall(
        #[source]
        ActionCallError<crate::operations::CaptureActionCallArgumentErrorFor<Operations, Value>>,
    ),

    /// Preparing a sleep suspension failed.
    #[error("sleep: {0}")]
    Sleep(#[source] SleepError<crate::operations::SleepDurationErrorFor<Operations, Value>>),
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
    /// The resolved duration value could not be converted to a concrete delay.
    #[error("invalid sleep duration: {source}")]
    InvalidDuration {
        /// The underlying value-conversion failure.
        #[source]
        source: DurationError,
    },
}
