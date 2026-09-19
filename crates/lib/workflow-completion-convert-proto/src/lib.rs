//! Workflow-completion result serialisation.
//!
//! Converts VM runtime values into the
//! [`WorkflowArguments`](waymark_proto::messages::WorkflowArguments)
//! format that the Python client expects for workflow completion results.

#![warn(missing_docs)]

use waymark_convert_core::TryConvert;
use waymark_vm_value_convert_core::PendingPromiseError;
use waymark_workflow_completion_core::Outcome;

/// Converter for workflow-completion results.
pub struct Converter;

impl
    TryConvert<
        Outcome<waymark_vm_value_python::ReadyValue>,
        waymark_proto::messages::WorkflowArguments,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        outcome: Outcome<waymark_vm_value_python::ReadyValue>,
    ) -> Result<waymark_proto::messages::WorkflowArguments, PendingPromiseError> {
        match outcome {
            Outcome::Completion(value) => Converter::try_convert(value),
            Outcome::Exception(exception) => Converter::try_convert(exception),
        }
    }
}

impl TryConvert<waymark_vm_value_python::ReadyValue, waymark_proto::messages::WorkflowArguments>
    for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        value: waymark_vm_value_python::ReadyValue,
    ) -> Result<waymark_proto::messages::WorkflowArguments, PendingPromiseError> {
        let value = waymark_vm_value_python_convert_proto::Converter::try_convert(&value)?;
        Ok(completion_workflow_arguments(value))
    }
}

impl
    TryConvert<
        waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
        waymark_proto::messages::WorkflowArguments,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        exception: waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
    ) -> Result<waymark_proto::messages::WorkflowArguments, PendingPromiseError> {
        let exception = waymark_vm_value_python_convert_proto::Converter::try_convert(&exception)?;
        Ok(exception_workflow_arguments(exception))
    }
}

fn exception_workflow_arguments(
    exception: waymark_proto::python_value::ExceptionValue,
) -> waymark_proto::messages::WorkflowArguments {
    use waymark_proto::messages::{WorkflowArgument, WorkflowArguments};

    let error_value = waymark_proto::python_value::Value {
        kind: Some(waymark_proto::python_value::value::Kind::Exception(
            Box::new(exception),
        )),
    };
    WorkflowArguments {
        arguments: vec![WorkflowArgument {
            key: "error".to_string(),
            value: waymark_proto_python_value_conversions::encode_value(&error_value),
        }],
    }
}

fn completion_workflow_arguments(
    value: waymark_proto::python_value::Value,
) -> waymark_proto::messages::WorkflowArguments {
    use waymark_proto::messages::{WorkflowArgument, WorkflowArguments};

    WorkflowArguments {
        arguments: vec![WorkflowArgument {
            key: "result".to_string(),
            value: waymark_proto_python_value_conversions::encode_value(&value),
        }],
    }
}
