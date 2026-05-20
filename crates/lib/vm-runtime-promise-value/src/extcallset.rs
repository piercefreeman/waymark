//! [`waymark_vm_interpreter_extcallset`] trait implementations.

use waymark_nonzero_duration::NonZeroDuration;

use crate::PromiseValue;

impl<T> waymark_vm_interpreter_extcallset::value::SleepDuration for PromiseValue<T>
where
    T: waymark_vm_interpreter_extcallset::value::SleepDuration,
{
    type Error = crate::Error<T::Error>;

    fn to_sleep_duration(&self) -> Result<NonZeroDuration, Self::Error> {
        let value = self.require_ready_ref()?;
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
        let value = self.require_ready_ref()?;
        value
            .capture_action_call_argument()
            .map_err(crate::Error::Ready)
    }
}
