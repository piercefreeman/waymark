//! Value requirements.

use waymark_nonzero_duration::NonZeroDuration;

/// Convert a resolved VM value into a sleep duration.
pub trait SleepDuration {
    /// The implementation-specific error returned when conversion fails.
    type Error: std::error::Error + 'static;

    /// Convert the value into a [`NonZeroDuration`].
    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error>;
}

/// Capture the value as an action call argument for executing
/// the corresponding extcall.
pub trait CaptureActionCallArgument {
    /// The implementation-specific error returned when capture fails.
    type Error: std::error::Error + 'static;

    /// The owned argument type passed to the extcall.
    type ActionCallArgument;

    /// Convert the value into an owned action-call argument.
    fn capture_action_call_argument(&self) -> Result<Self::ActionCallArgument, Self::Error>;
}

/// A unifying trait for extcallset value requirements.
pub trait Value: SleepDuration + CaptureActionCallArgument {}

impl<T> Value for T where T: SleepDuration + CaptureActionCallArgument {}
