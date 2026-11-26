//! [`waymark_vm_interpreter_extcallset`] trait implementations.

use waymark_nonzero_duration::NonZeroDuration;

use waymark_vm_runtime_core::PromisableExt as _;

use crate::PromiseValue;

/// The ready value of `value`, or the crate error carrying the id of the
/// promise state it waits for.
fn try_ready<T, ReadyValueError>(
    value: &PromiseValue<T>,
) -> Result<&T, crate::Error<ReadyValueError>> {
    value.require_ready_ref().map_err(|error| {
        crate::Error::UnresolvedPromise(error.map(|promise_state_handle| promise_state_handle.id()))
    })
}

impl<T> waymark_vm_interpreter_extcallset::value::SleepDuration for PromiseValue<T>
where
    T: waymark_vm_interpreter_extcallset::value::SleepDuration,
{
    type Error = crate::Error<T::Error>;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        let value = try_ready(self)?;
        value.to_sleep_duration().map_err(crate::Error::Ready)
    }
}

impl<T> waymark_vm_interpreter_extcallset::value::CaptureActionCallArgument for PromiseValue<T>
where
    T: waymark_vm_interpreter_extcallset::value::CaptureActionCallArgument,
{
    type Error = crate::Error<T::Error>;
    type ActionCallArgument = T::ActionCallArgument;

    fn capture_action_call_argument(&self) -> Result<Self::ActionCallArgument, Self::Error> {
        let value = try_ready(self)?;
        value
            .capture_action_call_argument()
            .map_err(crate::Error::Ready)
    }
}
