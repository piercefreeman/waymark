//! [`waymark_vm_interpreter_extcallset`] trait implementations for [`crate::Value`].

use std::time::{Duration, TryFromFloatSecsError};

use waymark_nonzero_duration::{NonZeroDuration, ZeroDurationError};

use crate::ReadyValue;

/// Errors returned while converting a value into a sleep duration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SleepDurationError {
    /// The pending promise value cannot be used as a sleep duration.
    #[error("a pending promise value cannot be used as a sleep duration")]
    UnresolvedValue,

    /// The value type cannot be used as a sleep duration.
    #[error("the value cannot be used as a sleep duration")]
    UnsupportedValue,

    /// Sleep duration must be strictly positive.
    #[error("sleep duration must be non-zero: {0}")]
    Zero(#[source] ZeroDurationError),

    /// Sleep duration cannot be negative.
    #[error("sleep duration cannot be negative")]
    Negative,

    /// Floating-point seconds could not be converted into a duration.
    #[error("the float value cannot be used as a sleep duration: {0}")]
    FloatConversion(#[source] TryFromFloatSecsError),
}

impl<Flavor: crate::Flavor> waymark_vm_interpreter_extcallset::value::SleepDuration
    for ReadyValue<Flavor>
{
    type Error = SleepDurationError;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        match self {
            Self::Int(value) => {
                let seconds: u64 = (*value).try_into().map_err(|_| Self::Error::Negative)?;
                Duration::from_secs(seconds)
                    .try_into()
                    .map_err(Self::Error::Zero)
            }
            Self::Float(value) => {
                let duration = Duration::try_from_secs_f64(value.get())
                    .map_err(Self::Error::FloatConversion)?;

                duration.try_into().map_err(Self::Error::Zero)
            }
            Self::Bool(_)
            | Self::String(_)
            | Self::None
            | Self::List(_)
            | Self::Dict(_)
            | Self::Exception(_)
            | Self::Extension(_) => Err(Self::Error::UnsupportedValue),
        }
    }
}

impl<Flavor: crate::Flavor> waymark_vm_interpreter_extcallset::value::CaptureActionCallArgument
    for ReadyValue<Flavor>
where
    Flavor::Extension: Clone,
{
    type Error = core::convert::Infallible;
    type ActionCallArgument = ReadyValue<Flavor>;

    fn capture_action_call_argument(&self) -> Result<Self::ActionCallArgument, Self::Error> {
        Ok(self.clone())
    }
}
