//! The UUID-shaped id, on the wire.

/// An id the API serves: its schema is a component called by its
/// [`TypeName`], so every field holding the id refers to the one
/// definition.
///
/// Implemented for every id waymark-ids defines.
///
/// [`TypeName`]: crate::TypeName
pub trait WellKnownId: crate::TypeName {}

impl crate::TypeName for waymark_ids::NodeId {
    const NAME: &'static str = "NodeId";
}

impl WellKnownId for waymark_ids::NodeId {}

// The way back out of the proxy is per id: a foreign trait can not be
// implemented for a bare type parameter, so each admitted id gets its own.
impl From<UuidId<waymark_ids::NodeId>> for waymark_ids::NodeId {
    fn from(id: UuidId<Self>) -> Self {
        id.0
    }
}

impl crate::TypeName for waymark_ids::InstanceId {
    const NAME: &'static str = "InstanceId";
}

impl WellKnownId for waymark_ids::InstanceId {}

impl From<UuidId<waymark_ids::InstanceId>> for waymark_ids::InstanceId {
    fn from(id: UuidId<Self>) -> Self {
        id.0
    }
}

impl crate::TypeName for waymark_ids::ExecutionId {
    const NAME: &'static str = "ExecutionId";
}

impl WellKnownId for waymark_ids::ExecutionId {}

impl From<UuidId<waymark_ids::ExecutionId>> for waymark_ids::ExecutionId {
    fn from(id: UuidId<Self>) -> Self {
        id.0
    }
}

impl crate::TypeName for waymark_ids::WorkflowVersionId {
    const NAME: &'static str = "WorkflowVersionId";
}

impl WellKnownId for waymark_ids::WorkflowVersionId {}

impl From<UuidId<waymark_ids::WorkflowVersionId>> for waymark_ids::WorkflowVersionId {
    fn from(id: UuidId<Self>) -> Self {
        id.0
    }
}

/// An id, as the text of its UUID.
//
// A proxy over the id type: the id crate keeps its conversions to and from
// the UUID and knows nothing of the wire; each impl asks for exactly the
// conversion it uses. The schema says `uuid`, which a bare string can not,
// and is a named component, so that one identity reads as one type
// wherever it appears.
#[derive(Debug)]
pub struct UuidId<Id>(pub Id);

impl<Id> From<Id> for UuidId<Id> {
    fn from(id: Id) -> Self {
        Self(id)
    }
}

impl<Id> serde::Serialize for UuidId<Id>
where
    Id: Into<uuid::Uuid> + Copy,
{
    fn serialize<Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error>
    where
        Serializer: serde::Serializer,
    {
        let uuid: uuid::Uuid = self.0.into();
        serializer.collect_str(&uuid)
    }
}

impl<'de, Id> serde::Deserialize<'de> for UuidId<Id>
where
    Id: TryFrom<uuid::Uuid, Error: std::fmt::Display>,
{
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        let uuid = match uuid::Uuid::parse_str(&text) {
            Ok(uuid) => uuid,
            Err(error) => return Err(serde::de::Error::custom(error)),
        };
        let id = match Id::try_from(uuid) {
            Ok(id) => id,
            Err(error) => return Err(serde::de::Error::custom(error)),
        };
        Ok(Self(id))
    }
}

impl<Id> schemars::JsonSchema for UuidId<Id>
where
    Id: WellKnownId,
{
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(Id::NAME)
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(format!("{}::UuidId<{}>", module_path!(), Id::NAME))
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "uuid",
        })
    }
}

#[cfg(test)]
mod tests;
