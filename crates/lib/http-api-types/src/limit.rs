//! The page size of a read, on the wire.

/// At most this many items in a page: one to the read's cap, inclusive.
//
// A proxy over query-limit's `Limit`, whose construction is the cap check;
// this only reads the number and puts the cap in the schema.
#[derive(Debug)]
pub struct Limit<const MAX: usize>(pub waymark_query_limit::Limit<MAX>);

impl<const MAX: usize> From<waymark_query_limit::Limit<MAX>> for Limit<MAX> {
    fn from(limit: waymark_query_limit::Limit<MAX>) -> Self {
        Self(limit)
    }
}

impl<const MAX: usize> From<Limit<MAX>> for waymark_query_limit::Limit<MAX> {
    fn from(limit: Limit<MAX>) -> Self {
        limit.0
    }
}

impl<const MAX: usize> serde::Serialize for Limit<MAX> {
    fn serialize<Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error>
    where
        Serializer: serde::Serializer,
    {
        serde::Serialize::serialize(&self.0.get(), serializer)
    }
}

impl<'de, const MAX: usize> serde::Deserialize<'de> for Limit<MAX> {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let value = <usize as serde::Deserialize>::deserialize(deserializer)?;
        let limit = match waymark_query_limit::Limit::new(value) {
            Ok(limit) => limit,
            Err(error) => return Err(serde::de::Error::custom(error)),
        };
        Ok(Self(limit))
    }
}

impl<const MAX: usize> schemars::JsonSchema for Limit<MAX> {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(format!("Limit{MAX}"))
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": 1,
            "maximum": MAX,
        })
    }
}

#[cfg(test)]
mod tests;
