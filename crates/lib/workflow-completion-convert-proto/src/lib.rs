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
///
/// Completion results are wrapped in a
/// [`BaseModelValue`](waymark_proto::python_value::BaseModelValue)
/// (`WorkflowNodeResult`) rather than plain primitive/dict/list values.
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
        let value = waymark_vm_value_convert_proto::Converter::try_convert(&value)?;
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
        let exception = waymark_vm_value_convert_proto::Converter::try_convert(&exception)?;
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
    use waymark_proto::python_value::{BaseModelValue, DictEntry, DictValue, value::Kind};

    // A dict is already a set of variables; anything else is the single
    // `result` variable.
    let variables_arg = match &value.kind {
        Some(Kind::DictValue(_)) => value,
        _ => waymark_proto::python_value::Value {
            kind: Some(Kind::DictValue(DictValue {
                entries: vec![DictEntry {
                    key: "result".to_string(),
                    value: Some(value),
                }],
            })),
        },
    };

    let dict = DictValue {
        entries: vec![DictEntry {
            key: "variables".to_string(),
            value: Some(variables_arg),
        }],
    };

    let result_value = waymark_proto::python_value::Value {
        kind: Some(Kind::Basemodel(BaseModelValue {
            module: "waymark.workflow_runtime".to_string(),
            name: "WorkflowNodeResult".to_string(),
            data: Some(dict),
        })),
    };

    WorkflowArguments {
        arguments: vec![WorkflowArgument {
            key: "result".to_string(),
            value: waymark_proto_python_value_conversions::encode_value(&result_value),
        }],
    }
}
