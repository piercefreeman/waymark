//! [`Convert`](waymark_convert_core::Convert) implementations from VM
//! runtime values to protocol-buffer argument types.
//!
//! Delegates value-level error handling to
//! [`waymark_extcall_convert::PendingPromiseError`].

#![warn(missing_docs)]

use waymark_convert_core::TryConvert;

pub use waymark_vm_value_convert_core::PendingPromiseError;

/// Stateless converter with all VM-value-to-proto [`TryConvert`] impls.
pub struct Converter;

impl TryConvert<&waymark_vm_value::ReadyValue, waymark_proto::messages::WorkflowArgumentValue>
    for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        value: &waymark_vm_value::ReadyValue,
    ) -> Result<waymark_proto::messages::WorkflowArgumentValue, PendingPromiseError> {
        let json = waymark_extcall_convert::Converter::try_convert(value.clone())?;
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
        let json = waymark_extcall_convert::Converter::try_convert(value.clone())?;
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
                value: Some(Converter::try_convert(value)?),
            });
        }
        Ok(WorkflowArguments { arguments })
    }
}

/// Convert proto workflow arguments back into a VM ready value.
///
/// The [`proto::WorkflowArguments`] message is converted to a JSON object
/// via [`waymark_message_conversions::workflow_arguments_to_json`] and then
/// into a [`waymark_vm_value::ReadyValue::Dict`].
impl TryConvert<waymark_proto::messages::WorkflowArguments, waymark_vm_value::ReadyValue>
    for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        value: waymark_proto::messages::WorkflowArguments,
    ) -> Result<waymark_vm_value::ReadyValue, Self::Error> {
        let json = waymark_message_conversions::workflow_arguments_to_json(value);
        waymark_extcall_convert::Converter::try_convert(json)
    }
}

/// Convert a single proto workflow argument value back into a VM ready
/// value.
///
/// This is the reverse of the
/// [`TryConvert<&ReadyValue, WorkflowArgumentValue>`] impl on this
/// converter.  The proto value is converted to JSON via
/// [`waymark_message_conversions::workflow_argument_value_to_json`] and
/// then into a [`waymark_vm_value::ReadyValue`].
impl TryConvert<&waymark_proto::messages::WorkflowArgumentValue, waymark_vm_value::ReadyValue>
    for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        value: &waymark_proto::messages::WorkflowArgumentValue,
    ) -> Result<waymark_vm_value::ReadyValue, Self::Error> {
        let json = waymark_message_conversions::workflow_argument_value_to_json(value);
        waymark_extcall_convert::Converter::try_convert(json)
    }
}

// ---------------------------------------------------------------------------
// ActionResultConverter — extracts action return values from the Python
// serialisation envelope.
// ---------------------------------------------------------------------------

/// Stateless converter that extracts an action's return value from the
/// [`proto::WorkflowArguments`] payload produced by the Python
/// `serialize_result_payload` helper.
///
/// Python wraps every action result as `{"result": <value>}`.  This
/// converter finds the `"result"` entry and converts just that single
/// [`proto::WorkflowArgumentValue`] into a
/// [`waymark_vm_value::ReadyValue`] via [`Converter`].
///
/// The conversion is infallible — callers should use
/// [`Convert::convert`](waymark_convert_core::Convert::convert).
/// When the payload is empty or lacks a `"result"` entry,
/// [`waymark_vm_value::ReadyValue::None`] is returned.
pub struct ActionResultConverter;

impl TryConvert<waymark_proto::messages::WorkflowArguments, waymark_vm_value::ReadyValue>
    for ActionResultConverter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        value: waymark_proto::messages::WorkflowArguments,
    ) -> Result<waymark_vm_value::ReadyValue, Self::Error> {
        let result_value = value
            .arguments
            .iter()
            .find(|arg| arg.key == "result")
            .and_then(|arg| arg.value.as_ref());

        match result_value {
            Some(v) => Converter::try_convert(v),
            None => Ok(waymark_vm_value::ReadyValue::None),
        }
    }
}
