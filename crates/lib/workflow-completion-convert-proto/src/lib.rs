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
/// [`BaseModelWorkflowArgument`](waymark_proto::messages::BaseModelWorkflowArgument)
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
        let json = waymark_vm_value_convert_json::Converter::try_convert(value)?;
        Ok(completion_workflow_arguments(json))
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
        let error_json = waymark_vm_value_convert_json::Converter::try_convert(exception)?;
        Ok(exception_workflow_arguments(error_json))
    }
}

fn exception_workflow_arguments(
    error_json: serde_json::Value,
) -> waymark_proto::messages::WorkflowArguments {
    use waymark_proto::messages::{WorkflowArgument, WorkflowArguments};

    let error_value =
        waymark_proto_python_value_conversions::json_to_workflow_argument_value(&error_json);
    WorkflowArguments {
        arguments: vec![WorkflowArgument {
            key: "error".to_string(),
            value: waymark_proto_python_value_conversions::encode_workflow_argument_value(
                &error_value,
            ),
        }],
    }
}

fn completion_workflow_arguments(
    json: serde_json::Value,
) -> waymark_proto::messages::WorkflowArguments {
    use waymark_proto::messages::{WorkflowArgument, WorkflowArguments};
    use waymark_proto::python_value::{
        BaseModelWorkflowArgument, WorkflowDictArgument, WorkflowDictEntry,
        workflow_argument_value::Kind,
    };

    let variables_value = match &json {
        serde_json::Value::Object(_) => json.clone(),
        other => {
            let mut map = serde_json::Map::new();
            map.insert("result".to_string(), other.clone());
            serde_json::Value::Object(map)
        }
    };

    let variables_arg =
        waymark_proto_python_value_conversions::json_to_workflow_argument_value(&variables_value);
    let dict = WorkflowDictArgument {
        entries: vec![WorkflowDictEntry {
            key: "variables".to_string(),
            value: Some(variables_arg),
        }],
    };

    let result_value = waymark_proto::python_value::WorkflowArgumentValue {
        kind: Some(Kind::Basemodel(BaseModelWorkflowArgument {
            module: "waymark.workflow_runtime".to_string(),
            name: "WorkflowNodeResult".to_string(),
            data: Some(dict),
        })),
    };

    WorkflowArguments {
        arguments: vec![WorkflowArgument {
            key: "result".to_string(),
            value: waymark_proto_python_value_conversions::encode_workflow_argument_value(
                &result_value,
            ),
        }],
    }
}
