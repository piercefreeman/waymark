//! Happy-path coverage for the workflow-completion result converter.

use waymark_convert_core::TryConvert;
use waymark_workflow_completion_convert_proto::Converter;
use waymark_workflow_completion_core::Outcome;

#[test]
fn completion_produces_single_result_argument() {
    use waymark_proto::messages::{
        primitive_workflow_argument::Kind as PrimitiveKind, workflow_argument_value::Kind,
    };

    let outcome = Outcome::Completion(waymark_vm_value::ReadyValue::Int(42));
    let args = Converter::try_convert(outcome).expect("conversion is infallible for ready ints");

    assert_eq!(args.arguments.len(), 1);
    let result_arg = &args.arguments[0];
    assert_eq!(result_arg.key, "result");

    // The completion value travels as-is: no envelope around it.
    let Some(Kind::Primitive(primitive)) = result_arg
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("expected the result to be a primitive, got {result_arg:?}");
    };
    assert_eq!(primitive.kind, Some(PrimitiveKind::IntValue(42)));
}

#[test]
fn completion_dict_passes_through_verbatim() {
    use waymark_proto::messages::{
        primitive_workflow_argument::Kind as PrimitiveKind, workflow_argument_value::Kind,
    };

    // A dict completion is user data, even when its keys collide with the
    // argument names of the completion plane (`result`).
    let outcome = Outcome::Completion(waymark_vm_value::ReadyValue::Dict(
        [
            (
                "result".to_string(),
                waymark_vm_value::Value::Ready(waymark_vm_value::ReadyValue::String(
                    "inner".to_string(),
                )),
            ),
            (
                "other".to_string(),
                waymark_vm_value::Value::Ready(waymark_vm_value::ReadyValue::String(
                    "kept".to_string(),
                )),
            ),
        ]
        .into_iter()
        .collect(),
    ));
    let args = Converter::try_convert(outcome).expect("conversion is infallible for ready dicts");

    assert_eq!(args.arguments.len(), 1);
    let result_arg = &args.arguments[0];
    assert_eq!(result_arg.key, "result");

    let Some(Kind::DictValue(dict)) = result_arg
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref())
    else {
        panic!("expected the result to be a dict, got {result_arg:?}");
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
fn exception_outcome_produces_single_error_argument() {
    use waymark_proto::messages::workflow_argument_value::Kind;

    let exception = waymark_vm_runtime_exception::Exception {
        type_id: "ValueError".to_string(),
        details: waymark_vm_value::ReadyValue::String("boom".to_string()),
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
