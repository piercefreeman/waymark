//! Value requirements.

use waymark_nonzero_duration::NonZeroDuration;

/// Convert a resolved VM value into a sleep duration.
pub trait SleepDuration {
    /// The implementation-specific error returned when conversion fails.
    type Error: std::error::Error + 'static;

    /// Convert the value into a [`NonZeroDuration`].
    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error>;
}

/// A unifying trait for extcallset value requirements.
pub trait Value: SleepDuration {}

impl<T> Value for T where T: SleepDuration {}
