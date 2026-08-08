//! The promise-level implementations.

use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_promise_value::PromiseValue;

use crate::Operations;
use crate::promise::MaybeUnresolvedError;

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
