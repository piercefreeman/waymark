use super::*;
use crate::test_helpers::schema_of;

#[test]
fn deserializes_whole_seconds() {
    let seconds: NonZeroSeconds = serde_json::from_value(serde_json::json!(60)).expect("seconds");

    assert_eq!(seconds.0.as_secs_f64(), 60.0);
}

#[test]
fn rejects_zero() {
    let error = serde_json::from_value::<NonZeroSeconds>(serde_json::json!(0)).expect_err("zero");

    assert!(error.to_string().contains("at least 1"), "{error}");
}

#[test]
fn schema_is_an_inline_integer_from_one() {
    assert!(<NonZeroSeconds as schemars::JsonSchema>::inline_schema());

    let schema = schema_of::<NonZeroSeconds>();

    assert_eq!(schema["type"], "integer", "{schema}");
    assert_eq!(schema["minimum"], 1, "{schema}");
}

#[test]
fn serializes_as_whole_seconds() {
    let seconds: NonZeroSeconds = waymark_nonzero_duration::NonZeroDuration::from_secs(60)
        .expect("non-zero")
        .into();

    assert_eq!(
        serde_json::to_value(seconds).expect("json"),
        serde_json::json!(60)
    );
}
