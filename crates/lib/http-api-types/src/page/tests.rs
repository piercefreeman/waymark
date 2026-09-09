use super::*;
use crate::test_helpers::{Position, schema_of};

/// Another position type, as another read of the same items has.
#[derive(Debug)]
struct OtherPosition;

impl waymark_cursor_core::EncodeCursor for OtherPosition {
    fn encode(&self) -> String {
        String::new()
    }
}

/// An item with something to serialize.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
struct Thing {
    name: String,
}

#[test]
fn serializes_the_items_and_the_encoded_cursor() {
    let page = Page {
        items: vec![Thing {
            name: "a".to_owned(),
        }],
        next: Some(crate::cursor::Cursor(Position(7))),
    };

    let json = serde_json::to_value(&page).expect("json");

    assert_eq!(
        json,
        serde_json::json!({ "items": [{ "name": "a" }], "next": "pos-7" })
    );
}

#[test]
fn serializes_empty_without_a_cursor() {
    let page: Page<Thing, Position> = Page {
        items: Vec::new(),
        next: None,
    };

    let json = serde_json::to_value(&page).expect("json");

    assert_eq!(json, serde_json::json!({ "items": [], "next": null }));
}

#[test]
fn schema_is_named_after_the_item() {
    let name = <Page<Thing, Position> as schemars::JsonSchema>::schema_name();

    assert_eq!(name, "ThingPage");
}

#[test]
fn schema_documents_the_cursor_as_a_nullable_string() {
    let schema = schema_of::<Page<Thing, Position>>();
    let next = &schema["properties"]["next"];

    assert_eq!(next["type"], "string", "{next}");
    assert_eq!(next["nullable"], true, "{next}");
}

#[test]
fn schema_identity_does_not_depend_on_the_cursor() {
    /// Something holding a page per read.
    #[derive(schemars::JsonSchema)]
    struct Reads {
        _one: Page<Thing, Position>,
        _other: Page<Thing, OtherPosition>,
    }

    assert_eq!(
        <Page<Thing, Position> as schemars::JsonSchema>::schema_id(),
        <Page<Thing, OtherPosition> as schemars::JsonSchema>::schema_id(),
    );

    let schema = schema_of::<Reads>();
    let components = schema["components"]["schemas"]
        .as_object()
        .expect("components");
    let pages: Vec<_> = components
        .keys()
        .filter(|key| key.ends_with("Page"))
        .collect();

    assert_eq!(pages, ["ThingPage"], "{schema}");
}
