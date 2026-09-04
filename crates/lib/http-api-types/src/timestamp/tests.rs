use super::*;
use crate::test_helpers::schema_of;

fn read(text: &str) -> Result<Timestamp, serde_json::Error> {
    serde_json::from_value(serde_json::json!(text))
}

#[test]
fn deserializes_an_instant() {
    let timestamp = read("2024-01-01T00:00:00Z").expect("instant");

    assert_eq!(timestamp.0.get().timestamp(), 1_704_067_200);
}

#[test]
fn rejects_an_instant_outside_the_range() {
    let error = read("-262143-01-01T00:00:00Z").expect_err("chrono's minimum");

    assert!(error.to_string().contains("0000-01-01"), "{error}");
}

#[test]
fn schema_is_an_inline_date_time_string() {
    assert!(<Timestamp as schemars::JsonSchema>::inline_schema());

    let schema = schema_of::<Timestamp>();

    assert_eq!(schema["type"], "string", "{schema}");
    assert_eq!(schema["format"], "date-time", "{schema}");
}

#[test]
fn serializes_as_rfc_3339() {
    let timestamp: Timestamp = waymark_timestamp::Timestamp::new(
        chrono::DateTime::from_timestamp(1_704_067_200, 0).unwrap(),
    )
    .expect("within range")
    .into();

    assert_eq!(
        serde_json::to_value(timestamp).expect("json"),
        serde_json::json!("2024-01-01T00:00:00Z")
    );
}
