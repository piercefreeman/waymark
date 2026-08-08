//! Operations requirements.

use waymark_nonzero_duration::NonZeroDuration;

/// Convert a resolved VM value into a sleep duration.
pub trait SleepDuration<Value> {
    /// The implementation-specific error returned when conversion fails.
    type Error: core::fmt::Debug;

    /// Convert the value into a [`NonZeroDuration`].
    fn to_sleep_duration(value: &Value) -> Result<NonZeroDuration, Self::Error>;
}

/// Capture the value as an action call argument for executing
/// the corresponding extcall.
pub trait CaptureActionCallArgument<Value> {
    /// The implementation-specific error returned when capture fails.
    type Error: core::fmt::Debug;

    /// The owned argument type passed to the extcall.
    type ActionCallArgument;

    /// Convert the value into an owned action-call argument.
    fn capture_action_call_argument(value: &Value)
    -> Result<Self::ActionCallArgument, Self::Error>;
}

/// A unifying trait for all operations requirements.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Operations<Value>: SleepDuration<Value> + CaptureActionCallArgument<Value> {}

/// The error [`SleepDuration`] returns for `Value`.
pub type SleepDurationErrorFor<Operations, Value> = <Operations as SleepDuration<Value>>::Error;

/// The error [`CaptureActionCallArgument`] returns for `Value`.
pub type CaptureActionCallArgumentErrorFor<Operations, Value> =
    <Operations as CaptureActionCallArgument<Value>>::Error;

/// The owned action-call argument [`CaptureActionCallArgument`] captures
/// `Value` into.
pub type ActionCallArgumentFor<Operations, Value> =
    <Operations as CaptureActionCallArgument<Value>>::ActionCallArgument;

/// The exception model the operations have to satisfy for the errors of
/// the failing operations to be raised as runtime exceptions.
pub trait Exceptions<Value> {}

impl<T, Value> Exceptions<Value> for T {}
