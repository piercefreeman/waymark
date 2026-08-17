use waymark_convert_core::{Convert as _, TryConvert};

use crate::Converter;

/// Convert proto workflow arguments back into a VM ready value.
///
/// The [`proto::WorkflowArguments`] message is converted to a JSON object
/// via [`waymark_message_conversions::workflow_arguments_to_json`] and then
/// into a [`waymark_vm_value_python::ReadyValue::Dict`].
///
/// Fallible since the framing-level arguments carry their values as
/// opaque encoded documents that decode here.
impl TryConvert<&waymark_proto::messages::WorkflowArguments, waymark_vm_value_python::ReadyValue>
    for Converter
{
    type Error = waymark_message_conversions::DecodeArgumentError;

    fn try_convert(
        value: &waymark_proto::messages::WorkflowArguments,
    ) -> Result<waymark_vm_value_python::ReadyValue, Self::Error> {
        let json = waymark_message_conversions::workflow_arguments_to_json(value)?;
        let value = waymark_vm_value_convert_json::Converter::convert(json);
        Ok(value)
    }
}

/// Convert a single proto workflow argument value back into a VM ready
/// value.
///
/// This is the reverse of the
/// [`TryConvert<&ReadyValue, WorkflowArgumentValue>`] impl on this
/// converter.  The proto value is converted to JSON via
/// [`waymark_message_conversions::workflow_argument_value_to_json`] and
/// then into a [`waymark_vm_value_python::ReadyValue`].
impl
    TryConvert<&waymark_proto::messages::WorkflowArgumentValue, waymark_vm_value_python::ReadyValue>
    for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        value: &waymark_proto::messages::WorkflowArgumentValue,
    ) -> Result<waymark_vm_value_python::ReadyValue, Self::Error> {
        let json = waymark_message_conversions::workflow_argument_value_to_json(value);
        waymark_vm_value_convert_json::Converter::try_convert(json)
    }
}

/// Fallible since the framing-level arguments carry their values as
/// opaque encoded documents that decode here.
impl
    TryConvert<
        &waymark_proto::messages::ActionResult,
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
    > for Converter
{
    type Error = waymark_message_conversions::DecodeArgumentError;

    fn try_convert(
        result: &waymark_proto::messages::ActionResult,
    ) -> Result<
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
        Self::Error,
    > {
        if result.success {
            let result_arg = result
                .payload
                .as_ref()
                .and_then(|payload| payload.arguments.iter().find(|arg| arg.key == "result"));

            let result_value = result_arg
                .map(|arg| {
                    waymark_message_conversions::decode_workflow_argument_value(&arg.value).map_err(
                        |source| waymark_message_conversions::DecodeArgumentError {
                            key: arg.key.clone(),
                            source,
                        },
                    )
                })
                .transpose()?;

            let value = result_value.as_ref().map(Self::convert);
            let value = value.unwrap_or(waymark_vm_value_python::ReadyValue::None);
            return Ok(waymark_action_runtime_core::ActionCallOutcome::Value(value));
        }

        let error_type = result.error_type.as_deref().unwrap_or("ActionError");

        // TODO: we are effectively dropping the message here; consider what to
        // do with it.
        let details = result.payload.as_ref().map(Self::try_convert).transpose()?;
        let details = details.unwrap_or(waymark_vm_value_python::ReadyValue::None);
        let exception = waymark_vm_runtime_exception::Exception {
            type_id: error_type.to_owned(),
            details,
        };
        Ok(waymark_action_runtime_core::ActionCallOutcome::Exception(
            exception,
        ))
    }
}
