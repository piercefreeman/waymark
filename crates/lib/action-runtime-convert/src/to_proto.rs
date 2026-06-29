use waymark_convert_core::TryConvert;
use waymark_vm_value_convert_core::PendingPromiseError;

use crate::Converter;

impl TryConvert<&waymark_vm_value::ReadyValue, waymark_proto::messages::WorkflowArgumentValue>
    for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        value: &waymark_vm_value::ReadyValue,
    ) -> Result<waymark_proto::messages::WorkflowArgumentValue, PendingPromiseError> {
        let json = waymark_vm_value_convert_json::Converter::try_convert(value.clone())?;
        Ok(waymark_message_conversions::json_to_workflow_argument_value(&json))
    }
}

impl TryConvert<&waymark_vm_value::Value, waymark_proto::messages::WorkflowArgumentValue>
    for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        value: &waymark_vm_value::Value,
    ) -> Result<waymark_proto::messages::WorkflowArgumentValue, PendingPromiseError> {
        let json = waymark_vm_value_convert_json::Converter::try_convert(value.clone())?;
        Ok(waymark_message_conversions::json_to_workflow_argument_value(&json))
    }
}

/// Convert a pair of call-argument names and values into kwargs.
///
/// This is the typical shape of an action call: `call_args` from the
/// `ActionRef` paired with the argument values from the VM.
impl
    TryConvert<
        (&[String], &[waymark_vm_value::ReadyValue]),
        waymark_proto::messages::WorkflowArguments,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        (names, values): (&[String], &[waymark_vm_value::ReadyValue]),
    ) -> Result<waymark_proto::messages::WorkflowArguments, PendingPromiseError> {
        use waymark_proto::messages::{WorkflowArgument, WorkflowArguments};

        let mut arguments = Vec::with_capacity(names.len());
        for (name, value) in names.iter().zip(values.iter()) {
            // Skip `None`-valued parameters (dependency markers such as
            // `Annotated[T, Depend(…)]` are serialized as `None` by the
            // VM).  The Python side (`provide_dependencies`) will resolve
            // them from the function signature instead.
            if matches!(value, waymark_vm_value::ReadyValue::None) {
                continue;
            }
            arguments.push(WorkflowArgument {
                key: name.clone(),
                value: Some(Self::try_convert(value)?),
            });
        }
        Ok(WorkflowArguments { arguments })
    }
}

/// Convert a pair of call-argument names and values into an option-kwargs.
///
/// This is the typical shape of an action call: `call_args` from the
/// `ActionRef` paired with the argument values from the VM.
impl
    TryConvert<
        (&[String], &[waymark_vm_value::ReadyValue]),
        Option<waymark_proto::messages::WorkflowArguments>,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        (names, values): (&[String], &[waymark_vm_value::ReadyValue]),
    ) -> Result<Option<waymark_proto::messages::WorkflowArguments>, PendingPromiseError> {
        if names.is_empty() {
            return Ok(None);
        }

        let workflow_arguments = Self::try_convert((names, values))?;
        Ok(Some(workflow_arguments))
    }
}
