//! Happy-path coverage for the workflow-completion result converter.

use waymark_convert_core::TryConvert;
use waymark_workflow_completion_convert_proto::Converter;
use waymark_workflow_completion_core::Outcome;

#[test]
fn completion_wraps_primitive_in_workflow_node_result() {
    use waymark_proto::messages::{
        primitive_workflow_argument::Kind as PrimitiveKind, workflow_argument_value::Kind,
    };

    let outcome = Outcome::Completion(waymark_vm_value_python::ReadyValue::Int(42));
    let args = Converter::try_convert(outcome).expect("conversion is infallible for ready ints");

    assert_eq!(args.arguments.len(), 1);
    let result_arg = &args.arguments[0];
    assert_eq!(result_arg.key, "result");

    let Some(Kind::Basemodel(basemodel)) = result_arg
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("expected a BaseModel-wrapped result, got {result_arg:?}");
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
    use waymark_proto::messages::workflow_argument_value::Kind;

    let exception = waymark_vm_runtime_exception::Exception {
        type_id: "ValueError".to_string(),
        details: waymark_vm_value_python::ReadyValue::String("boom".to_string()),
    };
    let args = Converter::try_convert(Outcome::Exception(exception))
        .expect("conversion is infallible for ready exceptions");

    assert_eq!(args.arguments.len(), 1);
    let error_arg = &args.arguments[0];
    assert_eq!(error_arg.key, "error");

    // The error payload is serialised as `{"type": ..., "message": ...}`.
    let Some(Kind::DictValue(dict)) = error_arg
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("expected the error payload to be a dict, got {error_arg:?}");
    };
    let keys: Vec<&str> = dict
        .entries
        .iter()
        .map(|entry| entry.key.as_str())
        .collect();
    assert!(keys.contains(&"type"), "missing type key in {keys:?}");
    assert!(keys.contains(&"message"), "missing message key in {keys:?}");
}
