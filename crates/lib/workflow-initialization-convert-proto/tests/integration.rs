//! Happy-path coverage for the initial-context converter.

use waymark_convert_core::TryConvert;
use waymark_workflow_initialization_convert_proto::InitialContextConverter;

fn int_arg(key: &str, value: i64) -> waymark_proto::messages::WorkflowArgument {
    use waymark_proto::messages::{
        PrimitiveWorkflowArgument, WorkflowArgument, WorkflowArgumentValue,
        primitive_workflow_argument::Kind as PrimitiveKind, workflow_argument_value::Kind,
    };

    let value = WorkflowArgumentValue {
        kind: Some(Kind::Primitive(PrimitiveWorkflowArgument {
            kind: Some(PrimitiveKind::IntValue(value)),
        })),
    };

    WorkflowArgument {
        key: key.to_string(),
        value: waymark_message_conversions::encode_workflow_argument_value(&value),
    }
}

fn ready_int(value: i64) -> waymark_vm_value_python::Value {
    waymark_vm_value_python::Value::Ready(waymark_vm_value_python::ReadyValue::Int(value))
}

#[test]
fn positional_args_follow_input_name_order() {
    let arguments = waymark_proto::messages::WorkflowArguments {
        arguments: vec![int_arg("x", 1), int_arg("y", 2)],
    };
    let input_names = vec!["y".to_string(), "x".to_string()];

    let positional = InitialContextConverter::try_convert((&arguments, input_names.as_slice()))
        .expect("argument values decode");

    assert_eq!(positional, vec![ready_int(2), ready_int(1)]);
}

#[test]
fn missing_keys_default_to_none() {
    let arguments = waymark_proto::messages::WorkflowArguments {
        arguments: vec![int_arg("present", 7)],
    };
    let input_names = vec!["present".to_string(), "absent".to_string()];

    let positional = InitialContextConverter::try_convert((&arguments, input_names.as_slice()))
        .expect("argument values decode");

    assert_eq!(
        positional,
        vec![
            ready_int(7),
            waymark_vm_value_python::Value::Ready(waymark_vm_value_python::ReadyValue::None),
        ],
    );
}
