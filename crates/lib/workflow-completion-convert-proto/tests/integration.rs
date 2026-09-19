//! Happy-path coverage for the workflow-completion result converter.

use waymark_convert_core::TryConvert;
use waymark_workflow_completion_convert_proto::Converter;
use waymark_workflow_completion_core::Outcome;

#[test]
fn completion_produces_single_result_argument() {
    use waymark_proto::python_value::{primitive_value::Kind as PrimitiveKind, value::Kind};

    let outcome = Outcome::Completion(waymark_vm_value_python::ReadyValue::Int(42));
    let args = Converter::try_convert(outcome).expect("conversion is infallible for ready ints");

    assert_eq!(args.arguments.len(), 1);
    let result_arg = &args.arguments[0];
    assert_eq!(result_arg.key, "result");

    // The completion value travels as-is: no envelope around it.
    let result_value: waymark_proto::python_value::Value =
        waymark_vm_value_python_convert_proto::Converter::try_convert(result_arg.value.as_slice())
            .expect("result argument value decodes");
    let Some(Kind::Primitive(primitive)) = &result_value.kind else {
        panic!("expected the result to be a primitive, got {result_value:?}");
    };
    assert_eq!(primitive.kind, Some(PrimitiveKind::IntValue(42)));
}

#[test]
fn completion_dict_passes_through_verbatim() {
    use waymark_proto::python_value::{primitive_value::Kind as PrimitiveKind, value::Kind};

    // A dict completion is user data, even when its keys collide with the
    // argument names of the completion plane (`result`).
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
    let args = Converter::try_convert(outcome).expect("conversion is infallible for ready dicts");

    assert_eq!(args.arguments.len(), 1);
    let result_arg = &args.arguments[0];
    assert_eq!(result_arg.key, "result");

    let result_value: waymark_proto::python_value::Value =
        waymark_vm_value_python_convert_proto::Converter::try_convert(result_arg.value.as_slice())
            .expect("result argument value decodes");
    let Some(Kind::DictValue(dict)) = &result_value.kind else {
        panic!("expected the result to be a dict, got {result_value:?}");
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
