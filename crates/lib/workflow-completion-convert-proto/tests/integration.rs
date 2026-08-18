//! Happy-path coverage for the workflow-completion result converter.

use waymark_convert_core::TryConvert;
use waymark_workflow_completion_convert_proto::Converter;
use waymark_workflow_completion_core::Outcome;

#[test]
fn completion_wraps_primitive_in_workflow_node_result() {
    use waymark_proto::python_value::{primitive_value::Kind as PrimitiveKind, value::Kind};

    let outcome = Outcome::Completion(waymark_vm_value_python::ReadyValue::Int(42));
    let args = Converter::try_convert(outcome).expect("conversion is infallible for ready ints");

    assert_eq!(args.arguments.len(), 1);
    let result_arg = &args.arguments[0];
    assert_eq!(result_arg.key, "result");

    let result_value: waymark_proto::python_value::Value =
        waymark_vm_value_python_convert_proto::Converter::try_convert(result_arg.value.as_slice())
            .expect("result argument value decodes");
    let Some(Kind::Basemodel(basemodel)) = &result_value.kind else {
        panic!("expected a BaseModel-wrapped result, got {result_value:?}");
    };
    assert_eq!(basemodel.module, "waymark.workflow_runtime");
    assert_eq!(basemodel.name, "WorkflowNodeResult");

    let variables = &basemodel
        .data
        .as_ref()
        .expect("basemodel carries a dict")
        .entries;
    assert_eq!(variables.len(), 1);
    assert_eq!(variables[0].key, "variables");

    // A non-object completion value is nested under a `result` key.
    let Some(Kind::DictValue(dict)) = variables[0]
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("expected variables to be a dict");
    };
    assert_eq!(dict.entries.len(), 1);
    assert_eq!(dict.entries[0].key, "result");

    let Some(Kind::Primitive(primitive)) = dict.entries[0]
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("expected the wrapped result to be a primitive");
    };
    assert_eq!(primitive.kind, Some(PrimitiveKind::IntValue(42)));
}

#[test]
fn exception_outcome_produces_single_error_argument() {
    use waymark_proto::python_value::primitive_value::Kind as PrimitiveKind;
    use waymark_proto::python_value::value::Kind;

    let exception = waymark_vm_runtime_exception::Exception {
        type_id: "ValueError".to_string(),
        details: waymark_vm_value_python::ReadyValue::String("boom".to_string()),
    };
    let args = Converter::try_convert(Outcome::Exception(exception))
        .expect("conversion is infallible for ready exceptions");

    assert_eq!(args.arguments.len(), 1);
    let error_arg = &args.arguments[0];
    assert_eq!(error_arg.key, "error");

    // The error payload is the exception itself, not a dict standing in
    // for one: the type identifying it and the details it carries.
    let error_value: waymark_proto::python_value::Value =
        waymark_vm_value_python_convert_proto::Converter::try_convert(error_arg.value.as_slice())
            .expect("error argument value decodes");
    let Some(Kind::Exception(exception)) = &error_value.kind else {
        panic!("expected the error payload to be an exception, got {error_value:?}");
    };
    assert_eq!(exception.type_id, "ValueError");

    let details = exception
        .details
        .as_deref()
        .expect("the exception carries its details");
    let Some(Kind::Primitive(primitive)) = &details.kind else {
        panic!("expected the details to be a primitive, got {details:?}");
    };
    assert_eq!(
        primitive.kind,
        Some(PrimitiveKind::StringValue("boom".to_string())),
    );
}
