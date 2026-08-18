use waymark_convert_core::TryConvert;
use waymark_vm_value_convert_core::PendingPromiseError;

use crate::Converter;

impl TryConvert<&waymark_vm_value_python::ReadyValue, waymark_proto::python_value::Value>
    for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        value: &waymark_vm_value_python::ReadyValue,
    ) -> Result<waymark_proto::python_value::Value, PendingPromiseError> {
        waymark_vm_value_convert_proto::Converter::try_convert(value)
    }
}

impl TryConvert<&waymark_vm_value_python::Value, waymark_proto::python_value::Value> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(
        value: &waymark_vm_value_python::Value,
    ) -> Result<waymark_proto::python_value::Value, PendingPromiseError> {
        waymark_vm_value_convert_proto::Converter::try_convert(value)
    }
}

/// Convert a pair of call-argument names and values into kwargs.
///
/// This is the typical shape of an action call: `call_args` from the
/// `ActionRef` paired with the argument values from the VM.
impl
    TryConvert<
        (&[String], &[waymark_vm_value_python::ReadyValue]),
        waymark_proto::messages::WorkflowArguments,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        (names, values): (&[String], &[waymark_vm_value_python::ReadyValue]),
    ) -> Result<waymark_proto::messages::WorkflowArguments, PendingPromiseError> {
        use waymark_proto::messages::{WorkflowArgument, WorkflowArguments};

        let mut arguments = Vec::with_capacity(names.len());
        for (name, value) in names.iter().zip(values.iter()) {
            // Skip `None`-valued parameters (dependency markers such as
            // `Annotated[T, Depend(…)]` are serialized as `None` by the
            // VM).  The Python side (`provide_dependencies`) will resolve
            // them from the function signature instead.
            if matches!(value, waymark_vm_value_python::ReadyValue::None) {
                continue;
            }
            let encoded = Self::try_convert(value)?;
            arguments.push(WorkflowArgument {
                key: name.clone(),
                value: waymark_proto_python_value_conversions::encode_value(&encoded),
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
        (&[String], &[waymark_vm_value_python::ReadyValue]),
        Option<waymark_proto::messages::WorkflowArguments>,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        (names, values): (&[String], &[waymark_vm_value_python::ReadyValue]),
    ) -> Result<Option<waymark_proto::messages::WorkflowArguments>, PendingPromiseError> {
        if names.is_empty() {
            return Ok(None);
        }

        let workflow_arguments = Self::try_convert((names, values))?;
        Ok(Some(workflow_arguments))
    }
}
