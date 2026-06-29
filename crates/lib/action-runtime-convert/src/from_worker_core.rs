use waymark_convert_core::TryConvert;

use crate::Converter;

impl TryConvert<waymark_runner_executor_core::ExecutionSuccess, waymark_vm_value::ReadyValue>
    for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        value: waymark_runner_executor_core::ExecutionSuccess,
    ) -> Result<waymark_vm_value::ReadyValue, Self::Error> {
        waymark_vm_value_convert_json::Converter::try_convert(value.0)
    }
}

impl
    TryConvert<
        waymark_runner_executor_core::ExecutionException,
        waymark_vm_runtime_exception::Exception<waymark_vm_value::ReadyValue>,
    > for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        value: waymark_runner_executor_core::ExecutionException,
    ) -> Result<waymark_vm_runtime_exception::Exception<waymark_vm_value::ReadyValue>, Self::Error>
    {
        waymark_vm_value_convert_json::Converter::try_convert(value.0)
    }
}

impl
    TryConvert<
        waymark_runner_executor_core::UncheckedExecutionResult,
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value::ReadyValue>,
    > for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        value: waymark_runner_executor_core::UncheckedExecutionResult,
    ) -> Result<
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value::ReadyValue>,
        Self::Error,
    > {
        match value.check() {
            Ok(value) => Ok(waymark_action_runtime_core::ActionCallOutcome::Value(
                Self::try_convert(value)?,
            )),
            Err(exception) => Ok(waymark_action_runtime_core::ActionCallOutcome::Exception(
                Self::try_convert(exception)?,
            )),
        }
    }
}
