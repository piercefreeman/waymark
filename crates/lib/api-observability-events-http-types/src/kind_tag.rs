//! The kind, on the wire as its tag.

use std::marker::PhantomData;

/// A kind, as its stable tag: `vm_driver.vm_stopped.cancelled`.
//
// A proxy over any subset of a kind family: the value is the subset's
// own, the codec is the root kind's (`Tagged` out, `FromTag` back in,
// through the subset's conversions to and from the root), the values a
// field may hold are the subset's and are listed and checked as such, and
// the component's name is the `Name` marker's, chosen by the transport
// that serves the field.
#[derive(Debug)]
pub struct KindTag<Kind, Name> {
    /// The kind.
    pub kind: Kind,

    name: PhantomData<Name>,
}

impl<Kind, Name> From<Kind> for KindTag<Kind, Name> {
    fn from(kind: Kind) -> Self {
        Self {
            kind,
            name: PhantomData,
        }
    }
}

impl<Kind, Name> serde::Serialize for KindTag<Kind, Name>
where
    Kind: waymark_observability_events_core::kind::SubsetExt,
{
    fn serialize<Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error>
    where
        Serializer: serde::Serializer,
    {
        serializer.serialize_str(self.kind.root_tag())
    }
}

impl<'de, Kind, Name> serde::Deserialize<'de> for KindTag<Kind, Name>
where
    Kind: waymark_observability_events_core::kind::SubsetExt,
{
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        let Some(kind) = Kind::try_from_root_tag(&text) else {
            return Err(serde::de::Error::custom(format_args!(
                "not a kind of this set: {text}"
            )));
        };
        Ok(Self::from(kind))
    }
}

impl<Kind, Name> schemars::JsonSchema for KindTag<Kind, Name>
where
    Kind: waymark_observability_events_core::kind::SubsetExt,
    Name: waymark_http_api_types::TypeName,
{
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(Name::NAME)
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(format!("{}::KindTag<{}>", module_path!(), Name::NAME))
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let tags: Vec<&'static str> = Kind::subset().map(|kind| kind.root_tag()).collect();
        schemars::json_schema!({
            "type": "string",
            "enum": tags,
        })
    }
}

#[cfg(test)]
mod tests;
