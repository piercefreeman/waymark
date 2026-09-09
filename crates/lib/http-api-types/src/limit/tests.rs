use super::*;
use crate::test_helpers::schema_of;

#[test]
fn deserializes_within_the_cap() {
    let limit: Limit<10> = serde_json::from_value(serde_json::json!(10)).expect("limit");

    assert_eq!(limit.0.get(), 10);
}

#[test]
fn rejects_zero_and_above_the_cap() {
    let zero = serde_json::from_value::<Limit<10>>(serde_json::json!(0)).expect_err("zero");
    let above = serde_json::from_value::<Limit<10>>(serde_json::json!(11)).expect_err("above");

    assert!(zero.to_string().contains("1..=10"), "{zero}");
    assert!(above.to_string().contains("1..=10"), "{above}");
}

#[test]
fn schema_is_an_inline_integer_with_the_cap() {
    assert!(<Limit<10> as schemars::JsonSchema>::inline_schema());

    let schema = schema_of::<Limit<10>>();

    assert_eq!(schema["type"], "integer", "{schema}");
    assert_eq!(schema["minimum"], 1, "{schema}");
    assert_eq!(schema["maximum"], 10, "{schema}");
}

#[test]
fn serializes_as_the_number() {
    let limit: Limit<10> = waymark_query_limit::Limit::new(7).expect("limit").into();

    assert_eq!(
        serde_json::to_value(limit).expect("json"),
        serde_json::json!(7)
    );
}
