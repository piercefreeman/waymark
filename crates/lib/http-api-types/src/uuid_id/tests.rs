use super::*;
use crate::test_helpers::schema_of;

const TEXT: &str = "00000000-0000-0000-0000-000000000001";

fn node_id() -> waymark_ids::NodeId {
    waymark_ids::NodeId::try_new(uuid::Uuid::from_u128(1)).expect("non-nil")
}

#[test]
fn serializes_as_the_uuid_text() {
    let json = serde_json::to_value(UuidId(node_id())).expect("json");

    assert_eq!(json, serde_json::json!(TEXT));
}

#[test]
fn deserializes_from_the_uuid_text() {
    let id: UuidId<waymark_ids::NodeId> =
        serde_json::from_value(serde_json::json!(TEXT)).expect("id");

    assert_eq!(id.0, node_id());
}

#[test]
fn rejects_the_nil_uuid_as_the_id_does() {
    let nil = "00000000-0000-0000-0000-000000000000";
    let error = serde_json::from_value::<UuidId<waymark_ids::NodeId>>(serde_json::json!(nil))
        .expect_err("nil");

    assert!(error.to_string().contains("nil uuid"), "{error}");
}

#[test]
fn rejects_what_is_not_a_uuid() {
    serde_json::from_value::<UuidId<waymark_ids::NodeId>>(serde_json::json!("not-a-uuid"))
        .expect_err("not a uuid");
}

#[test]
fn schema_is_a_component_named_after_the_id() {
    /// Something holding an id.
    #[derive(schemars::JsonSchema)]
    struct Holder {
        /// The node's id.
        _node_id: UuidId<waymark_ids::NodeId>,
    }

    assert_eq!(
        <UuidId<waymark_ids::NodeId> as schemars::JsonSchema>::schema_name(),
        "NodeId"
    );

    let schema = schema_of::<Holder>();

    assert_eq!(
        schema["components"]["schemas"]["NodeId"],
        serde_json::json!({ "type": "string", "format": "uuid" }),
        "{schema}"
    );

    // OpenAPI 3.0 allows no siblings on a reference, so the field's own
    // description rides beside the reference wrapped in `allOf`.
    assert_eq!(
        schema["properties"]["_node_id"],
        serde_json::json!({
            "allOf": [{ "$ref": "#/components/schemas/NodeId" }],
            "description": "The node's id.",
        }),
        "{schema}"
    );
}
