//! Happy-path coverage for the initial-context converter.

use waymark_convert_core::Convert;
use waymark_workflow_initialization_convert_proto::InitialContextConverter;

fn int_arg(key: &str, value: i64) -> waymark_proto::messages::WorkflowArgument {
    use waymark_proto::messages::{
        PrimitiveWorkflowArgument, WorkflowArgument, WorkflowArgumentValue,
        primitive_workflow_argument::Kind as PrimitiveKind, workflow_argument_value::Kind,
    };

    WorkflowArgument {
        key: key.to_string(),
        value: Some(WorkflowArgumentValue {
            kind: Some(Kind::Primitive(PrimitiveWorkflowArgument {
                kind: Some(PrimitiveKind::IntValue(value)),
            })),
        }),
    }
}

fn ready_int(value: i64) -> waymark_vm_value::Value {
    waymark_vm_value::Value::Ready(waymark_vm_value::ReadyValue::Int(value))
}

#[test]
fn positional_args_follow_input_name_order() {
    let arguments = waymark_proto::messages::WorkflowArguments {
        arguments: vec![int_arg("x", 1), int_arg("y", 2)],
    };
    let input_names = vec!["y".to_string(), "x".to_string()];

    let positional = InitialContextConverter::convert((&arguments, input_names.as_slice()));

    assert_eq!(positional, vec![ready_int(2), ready_int(1)]);
}

#[test]
fn missing_keys_default_to_none() {
    let arguments = waymark_proto::messages::WorkflowArguments {
        arguments: vec![int_arg("present", 7)],
    };
    let input_names = vec!["present".to_string(), "absent".to_string()];

    let positional = InitialContextConverter::convert((&arguments, input_names.as_slice()));

    assert_eq!(
        positional,
        vec![
            ready_int(7),
            waymark_vm_value::Value::Ready(waymark_vm_value::ReadyValue::None),
        ],
    );
}
