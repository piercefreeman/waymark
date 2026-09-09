//! The cursor: a position in a read's order, on the wire.

/// A position in a read's order, as opaque text: a page's `next`, handed
/// back as the following read's `after`.
//
// A proxy over the backend's own position type: the codec is the one
// cursor-core demands of every cursor, so the position never learns the
// wire, and the wire never learns the position's shape.
#[derive(Debug)]
pub struct Cursor<Position>(pub Position);

impl<Position> From<Position> for Cursor<Position> {
    fn from(position: Position) -> Self {
        Self(position)
    }
}

impl<Position> serde::Serialize for Cursor<Position>
where
    Position: waymark_cursor_core::EncodeCursor,
{
    fn serialize<Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error>
    where
        Serializer: serde::Serializer,
    {
        let encoded = waymark_cursor_core::EncodeCursor::encode(&self.0);
        serializer.serialize_str(&encoded)
    }
}

impl<'de, Position> serde::Deserialize<'de> for Cursor<Position>
where
    Position: waymark_cursor_core::DecodeCursor,
{
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        let position = match waymark_cursor_core::DecodeCursor::decode(&text) {
            Ok(position) => position,
            Err(error) => {
                return Err(serde::de::Error::custom(format_args!(
                    "invalid cursor: {error:?}"
                )));
            }
        };
        Ok(Self(position))
    }
}

/// The schema of an optional cursor, for a container's field: the same
/// string whatever the position, so the container's own schema identity
/// stays independent of the position type — a page of one item type is
/// one component however it is read.
pub(crate) fn optional_schema<Position>(
    generator: &mut schemars::SchemaGenerator,
) -> schemars::Schema {
    generator.subschema_for::<Option<Cursor<Position>>>()
}

impl<Position> schemars::JsonSchema for Cursor<Position> {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Cursor")
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
        })
    }
}

#[cfg(test)]
mod tests;
