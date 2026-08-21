//! Happy-path coverage for the workflow-completion result converter.

use waymark_convert_core::TryConvert;
use waymark_proto::python_value::workflow_outcome::Outcome as ProtoOutcome;
use waymark_workflow_completion_convert_proto::Converter;
use waymark_workflow_completion_core::Outcome;

#[test]
fn completion_produces_value_outcome() {
    use waymark_proto::python_value::{primitive_value::Kind as PrimitiveKind, value::Kind};

    let outcome = Outcome::Completion(waymark_vm_value_python::ReadyValue::Int(42));
    let result: waymark_proto::python_value::WorkflowOutcome =
        Converter::try_convert(outcome).expect("conversion is infallible for ready ints");

    // The completion value travels as-is in the `value` arm: no envelope
    // around it.
    let Some(ProtoOutcome::Value(value)) = &result.outcome else {
        panic!("expected a value outcome, got {result:?}");
    };
    let Some(Kind::Primitive(primitive)) = &value.kind else {
        panic!("expected the result to be a primitive, got {value:?}");
    };
    assert_eq!(primitive.kind, Some(PrimitiveKind::IntValue(42)));
}

#[test]
fn completion_dict_passes_through_verbatim() {
    use waymark_proto::python_value::{primitive_value::Kind as PrimitiveKind, value::Kind};

    // A dict completion is user data: no key it carries can collide with
    // the outcome structure, `result` included.
    let outcome = Outcome::Completion(waymark_vm_value_python::ReadyValue::Dict(
        [
            (
                "result".to_string(),
                waymark_vm_value_python::Value::Ready(waymark_vm_value_python::ReadyValue::String(
                    "inner".to_string(),
                )),
            ),
            (
                "other".to_string(),
                waymark_vm_value_python::Value::Ready(waymark_vm_value_python::ReadyValue::String(
                    "kept".to_string(),
                )),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    let result: waymark_proto::python_value::WorkflowOutcome =
        Converter::try_convert(outcome).expect("conversion is infallible for ready dicts");

    let Some(ProtoOutcome::Value(value)) = &result.outcome else {
        panic!("expected a value outcome, got {result:?}");
    };
    let Some(Kind::DictValue(dict)) = &value.kind else {
        panic!("expected the result to be a dict, got {value:?}");
    };
    let string_entry = |key: &str| {
        let entry = dict
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .unwrap_or_else(|| panic!("missing dict entry {key:?}"));
        let Some(Kind::Primitive(primitive)) =
            entry.value.as_ref().and_then(|value| value.kind.as_ref())
        else {
            panic!("expected the dict entry {key:?} to be a primitive");
        };
        primitive.kind.clone()
    };
    assert_eq!(dict.entries.len(), 2);
    assert_eq!(
        string_entry("result"),
        Some(PrimitiveKind::StringValue("inner".to_string())),
    );
    assert_eq!(
        string_entry("other"),
        Some(PrimitiveKind::StringValue("kept".to_string())),
    );
}

#[test]
fn exception_outcome_produces_exception_arm() {
    use waymark_proto::python_value::{primitive_value::Kind as PrimitiveKind, value::Kind};

    let exception = waymark_vm_runtime_exception::Exception {
        type_id: "ValueError".to_string(),
        details: waymark_vm_value_python::ReadyValue::String("boom".to_string()),
    };
    let result: waymark_proto::python_value::WorkflowOutcome =
        Converter::try_convert(Outcome::Exception(exception))
            .expect("conversion is infallible for ready exceptions");

    // The error payload is the exception itself, not a dict standing in
    // for one: the type identifying it and the details it carries.
    let Some(ProtoOutcome::Exception(exception)) = &result.outcome else {
        panic!("expected an exception outcome, got {result:?}");
    };
    assert_eq!(exception.type_id, "ValueError");

    let details = exception
        .details
        .as_ref()
        .expect("the exception carries its details");
    let Some(Kind::Primitive(primitive)) = &details.kind else {
        panic!("expected the details to be a primitive, got {details:?}");
    };
    assert_eq!(
        primitive.kind,
        Some(PrimitiveKind::StringValue("boom".to_string())),
    );
}
