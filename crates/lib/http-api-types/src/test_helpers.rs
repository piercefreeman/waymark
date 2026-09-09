//! Fixtures shared by the modules' tests.

/// A position with the codec and nothing else: no schema, no serde, as
/// a backend's cursor has neither.
#[derive(Debug, PartialEq)]
pub(crate) struct Position(pub(crate) u32);

/// A wire form that is not a position.
#[derive(Debug)]
pub(crate) struct NotAPosition;

impl waymark_cursor_core::EncodeCursor for Position {
    fn encode(&self) -> String {
        format!("pos-{}", self.0)
    }
}

impl waymark_cursor_core::DecodeCursor for Position {
    type Error = NotAPosition;

    fn decode(text: &str) -> Result<Self, Self::Error> {
        let Some(number) = text.strip_prefix("pos-") else {
            return Err(NotAPosition);
        };
        match number.parse() {
            Ok(number) => Ok(Self(number)),
            Err(_) => Err(NotAPosition),
        }
    }
}

/// The schema of `T`, as JSON, under the OpenAPI 3 settings the
/// document is generated with.
pub(crate) fn schema_of<T: schemars::JsonSchema>() -> serde_json::Value {
    let mut generator = schemars::generate::SchemaSettings::openapi3().into_generator();
    let schema = generator.root_schema_for::<T>();
    serde_json::to_value(schema).expect("schema json")
}
