//! Happy-path coverage for the workflow-arguments converter, pinned to
//! the Python value converter.

use prost::Message as _;
use waymark_convert_core::TryConvert;

type Converter = waymark_workflow_initialization_convert_proto::Converter<
    waymark_vm_value_python_convert_proto::WorkflowArgumentsConverter,
>;

fn int_arg(key: &str, value: i64) -> waymark_proto::python_value::WorkflowArgument {
    let value: waymark_proto::python_value::Value =
        waymark_vm_value_python_convert_proto::Converter::try_convert(
            &waymark_vm_value_python::ReadyValue::Int(value),
        )
        .expect("an integer holds no pending promise");

    waymark_proto::python_value::WorkflowArgument {
        key: key.to_string(),
        value: Some(value),
    }
}

fn encoded(arguments: Vec<waymark_proto::python_value::WorkflowArgument>) -> Vec<u8> {
    waymark_proto::python_value::WorkflowArguments { arguments }.encode_to_vec()
}

fn ready_int(value: i64) -> waymark_vm_value_python::Value {
    waymark_vm_value_python::Value::Ready(waymark_vm_value_python::ReadyValue::Int(value))
}

#[test]
fn positional_args_follow_input_name_order() {
    let payload = encoded(vec![int_arg("x", 1), int_arg("y", 2)]);
    let input_names = vec!["y".to_string(), "x".to_string()];

    let call_spec: waymark_vm_runtime::CallSpec<usize, waymark_vm_value_python::Value> =
        Converter::try_convert((payload.as_slice(), input_names.as_slice()))
            .expect("argument values decode");

    assert_eq!(call_spec.args, vec![ready_int(2), ready_int(1)]);
}

#[test]
fn a_missing_input_is_refused() {
    let payload = encoded(vec![int_arg("present", 7)]);
    let input_names = vec!["present".to_string(), "absent".to_string()];

    let converted: Result<waymark_vm_runtime::CallSpec<usize, waymark_vm_value_python::Value>, _> =
        Converter::try_convert((payload.as_slice(), input_names.as_slice()));

    let error = converted.expect_err("every input needs an argument");
    let waymark_vm_value_python_convert_proto::WorkflowArgumentsError::MissingInput { name } =
        error
    else {
        panic!("a missing input, got {error:?}");
    };
    assert_eq!(name, "absent");
}

#[test]
fn an_argument_matching_no_input_is_refused() {
    let payload = encoded(vec![int_arg("present", 7), int_arg("stray", 8)]);
    let input_names = vec!["present".to_string()];

    let converted: Result<waymark_vm_runtime::CallSpec<usize, waymark_vm_value_python::Value>, _> =
        Converter::try_convert((payload.as_slice(), input_names.as_slice()));

    let error = converted.expect_err("every argument must match an input");
    let waymark_vm_value_python_convert_proto::WorkflowArgumentsError::UnknownArgument { name } =
        error
    else {
        panic!("an unknown argument, got {error:?}");
    };
    assert_eq!(name, "stray");
}

#[test]
fn an_explicit_none_reaches_its_input() {
    let none: waymark_proto::python_value::Value =
        waymark_vm_value_python_convert_proto::Converter::try_convert(
            &waymark_vm_value_python::ReadyValue::None,
        )
        .expect("None holds no pending promise");
    let payload = encoded(vec![waymark_proto::python_value::WorkflowArgument {
        key: "value".to_string(),
        value: Some(none),
    }]);
    let input_names = vec!["value".to_string()];

    let call_spec: waymark_vm_runtime::CallSpec<usize, waymark_vm_value_python::Value> =
        Converter::try_convert((payload.as_slice(), input_names.as_slice()))
            .expect("argument values decode");

    assert_eq!(
        call_spec.args,
        vec![waymark_vm_value_python::Value::Ready(
            waymark_vm_value_python::ReadyValue::None
        )],
    );
}
