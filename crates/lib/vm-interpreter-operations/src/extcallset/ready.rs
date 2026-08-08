//! The ready-level implementations.

use std::time::{Duration, TryFromFloatSecsError};

use waymark_nonzero_duration::{NonZeroDuration, ZeroDurationError};
use waymark_vm_value::ReadyValue;

use crate::Operations;

/// Errors returned while converting a value into a sleep duration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SleepDurationError {
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

impl<Variation> waymark_vm_interpreter_extcallset::operations::SleepDuration<ReadyValue>
    for Operations<Variation>
{
    type Error = SleepDurationError;

    fn to_sleep_duration(value: &ReadyValue) -> Result<NonZeroDuration, Self::Error> {
        match value {
            ReadyValue::Int(value) => {
                let seconds: u64 = (*value).try_into().map_err(|_| Self::Error::Negative)?;
                Duration::from_secs(seconds)
                    .try_into()
                    .map_err(Self::Error::Zero)
            }
            ReadyValue::Float(value) => {
                let duration = Duration::try_from_secs_f64(value.get())
                    .map_err(Self::Error::FloatConversion)?;

                duration.try_into().map_err(Self::Error::Zero)
            }
            ReadyValue::Bool(_)
            | ReadyValue::String(_)
            | ReadyValue::None
            | ReadyValue::List(_)
            | ReadyValue::Dict(_)
            | ReadyValue::Exception(_) => Err(Self::Error::UnsupportedValue),
        }
    }
}

impl<Variation> waymark_vm_interpreter_extcallset::operations::CaptureActionCallArgument<ReadyValue>
    for Operations<Variation>
{
    type Error = core::convert::Infallible;
    type ActionCallArgument = ReadyValue;

    fn capture_action_call_argument(
        value: &ReadyValue,
    ) -> Result<Self::ActionCallArgument, Self::Error> {
        Ok(value.clone())
    }
}
