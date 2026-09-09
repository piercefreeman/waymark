//! The non-zero duration, on the wire as whole seconds.

/// A duration as whole seconds, at least one.
//
// A proxy over nonzero-duration's type: the construction is the zero
// check, this only reads the number and puts the bound in the schema.
#[derive(Debug)]
pub struct NonZeroSeconds(pub waymark_nonzero_duration::NonZeroDuration);

impl From<waymark_nonzero_duration::NonZeroDuration> for NonZeroSeconds {
    fn from(duration: waymark_nonzero_duration::NonZeroDuration) -> Self {
        Self(duration)
    }
}

impl From<NonZeroSeconds> for waymark_nonzero_duration::NonZeroDuration {
    fn from(seconds: NonZeroSeconds) -> Self {
        seconds.0
    }
}

impl<'de> serde::Deserialize<'de> for NonZeroSeconds {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let seconds = <u64 as serde::Deserialize>::deserialize(deserializer)?;
        match waymark_nonzero_duration::NonZeroDuration::from_secs(seconds) {
            Some(duration) => Ok(Self(duration)),
            None => Err(serde::de::Error::custom("seconds must be at least 1")),
        }
    }
}

impl schemars::JsonSchema for NonZeroSeconds {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("NonZeroSeconds")
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": 1,
        })
    }
}

#[cfg(test)]
mod tests;
