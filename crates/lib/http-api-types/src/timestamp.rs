//! The instant, on the wire as RFC 3339 text.

/// An instant, as RFC 3339 text, from 0000-01-01T00:00:00Z through
/// 9999-12-31T23:59:59.999999Z.
//
// A proxy over timestamp's `Timestamp`, whose construction is the range
// check and whose serde is the RFC 3339 text; this only puts the format
// in the schema.
#[derive(Debug)]
pub struct Timestamp(pub waymark_timestamp::Timestamp);

impl From<waymark_timestamp::Timestamp> for Timestamp {
    fn from(timestamp: waymark_timestamp::Timestamp) -> Self {
        Self(timestamp)
    }
}

impl From<Timestamp> for waymark_timestamp::Timestamp {
    fn from(timestamp: Timestamp) -> Self {
        timestamp.0
    }
}

impl From<Timestamp> for chrono::DateTime<chrono::Utc> {
    fn from(timestamp: Timestamp) -> Self {
        timestamp.0.get()
    }
}

impl serde::Serialize for Timestamp {
    fn serialize<Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error>
    where
        Serializer: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        <waymark_timestamp::Timestamp as serde::Deserialize>::deserialize(deserializer).map(Self)
    }
}

impl schemars::JsonSchema for Timestamp {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Timestamp")
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "date-time",
        })
    }
}

#[cfg(test)]
mod tests;
