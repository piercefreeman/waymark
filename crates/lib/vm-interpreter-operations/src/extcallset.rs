//! [`waymark_vm_interpreter_extcallset`] operations implementations.

use std::time::{Duration, TryFromFloatSecsError};

use waymark_nonzero_duration::{NonZeroDuration, ZeroDurationError};
use waymark_vm_runtime_promise_value::PromiseValue;
use waymark_vm_value::ReadyValue;

use crate::Operations;
use crate::promise::MaybeUnresolvedError;

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

impl<Variation, T> waymark_vm_interpreter_extcallset::operations::SleepDuration<PromiseValue<T>>
    for Operations<Variation>
where
    Operations<Variation>: waymark_vm_interpreter_extcallset::operations::SleepDuration<T>,
{
    type Error = MaybeUnresolvedError<
        <Operations<Variation> as waymark_vm_interpreter_extcallset::operations::SleepDuration<
            T,
        >>::Error,
    >;

    fn to_sleep_duration(value: &PromiseValue<T>) -> Result<NonZeroDuration, Self::Error> {
        let value = value
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;
        <Operations<Variation> as waymark_vm_interpreter_extcallset::operations::SleepDuration<
            T,
        >>::to_sleep_duration(value)
        .map_err(MaybeUnresolvedError::Ready)
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

impl<Variation, T>
    waymark_vm_interpreter_extcallset::operations::CaptureActionCallArgument<PromiseValue<T>>
    for Operations<Variation>
where
    Operations<Variation>:
        waymark_vm_interpreter_extcallset::operations::CaptureActionCallArgument<T>,
{
    type Error = MaybeUnresolvedError<
        <Operations<Variation> as waymark_vm_interpreter_extcallset::operations::CaptureActionCallArgument<
            T,
        >>::Error,
    >;
    type ActionCallArgument =
        <Operations<Variation> as waymark_vm_interpreter_extcallset::operations::CaptureActionCallArgument<
            T,
        >>::ActionCallArgument;

    fn capture_action_call_argument(
        value: &PromiseValue<T>,
    ) -> Result<Self::ActionCallArgument, Self::Error> {
        let value = value
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;
        <Operations<Variation> as waymark_vm_interpreter_extcallset::operations::CaptureActionCallArgument<
            T,
        >>::capture_action_call_argument(value)
        .map_err(MaybeUnresolvedError::Ready)
    }
}
