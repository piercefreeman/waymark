//! Workflow-completion result serialisation.
//!
//! Converts VM runtime values into the
//! [`WorkflowOutcome`](waymark_proto::python_value::WorkflowOutcome)
//! message that the Python client expects for workflow completion results.

#![warn(missing_docs)]

use waymark_convert_core::TryConvert;
use waymark_vm_value_convert_core::PendingPromiseError;
use waymark_workflow_completion_core::Outcome;

/// Converter for workflow-completion results.
pub struct Converter;

impl
    TryConvert<
        Outcome<waymark_vm_value_python::ReadyValue>,
        waymark_proto::python_value::WorkflowOutcome,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        outcome: Outcome<waymark_vm_value_python::ReadyValue>,
    ) -> Result<waymark_proto::python_value::WorkflowOutcome, PendingPromiseError> {
        match outcome {
            Outcome::Completion(value) => Converter::try_convert(value),
            Outcome::Exception(exception) => Converter::try_convert(exception),
        }
    }
}

impl TryConvert<waymark_vm_value_python::ReadyValue, waymark_proto::python_value::WorkflowOutcome>
    for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        value: waymark_vm_value_python::ReadyValue,
    ) -> Result<waymark_proto::python_value::WorkflowOutcome, PendingPromiseError> {
        let value = waymark_vm_value_python_convert_proto::Converter::try_convert(&value)?;
        Ok(waymark_proto::python_value::WorkflowOutcome {
            outcome: Some(waymark_proto::python_value::workflow_outcome::Outcome::Value(value)),
        })
    }
}

impl
    TryConvert<
        waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
        waymark_proto::python_value::WorkflowOutcome,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        exception: waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
    ) -> Result<waymark_proto::python_value::WorkflowOutcome, PendingPromiseError> {
        let exception = waymark_vm_value_python_convert_proto::Converter::try_convert(&exception)?;
        Ok(waymark_proto::python_value::WorkflowOutcome {
            outcome: Some(
                waymark_proto::python_value::workflow_outcome::Outcome::Exception(exception),
            ),
        })
    }
}
