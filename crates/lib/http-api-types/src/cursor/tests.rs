use super::*;
use crate::test_helpers::{Position, schema_of};

#[test]
fn serializes_as_the_encoded_text() {
    let json = serde_json::to_value(Cursor(Position(3))).expect("json");

    assert_eq!(json, serde_json::json!("pos-3"));
}

#[test]
fn deserializes_through_the_codec() {
    let cursor: Cursor<Position> =
        serde_json::from_value(serde_json::json!("pos-3")).expect("cursor");

    assert_eq!(cursor.0, Position(3));
}

#[test]
fn rejects_what_the_codec_rejects() {
    let error = serde_json::from_value::<Cursor<Position>>(serde_json::json!("nope"))
        .expect_err("not a position");

    assert!(error.to_string().contains("invalid cursor"), "{error}");
}

#[test]
fn schema_is_an_inline_string() {
    assert!(<Cursor<Position> as schemars::JsonSchema>::inline_schema());

    let schema = schema_of::<Cursor<Position>>();

    assert_eq!(schema["type"], "string", "{schema}");
}
