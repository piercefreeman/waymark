//! The schemas of the fields whose types describe none of their own.

/// The schema of an id serialized as its UUID's text: the id types carry
/// no schema of their own, and this crate describes the payload it
/// serializes.
pub(crate) fn uuid(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string",
        "format": "uuid",
    })
}
