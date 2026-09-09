//! The UUID-shaped id, on the wire.

/// An id the API serves: its schema is a component called by its
/// [`TypeName`], so every field holding the id refers to the one
/// definition.
///
/// [`TypeName`]: crate::TypeName
pub trait WellKnownId: crate::TypeName {}

/// Admit waymark ids: each one's [`TypeName`](crate::TypeName) is its own
/// name, it is a [`WellKnownId`], and it comes back out of the proxy.
///
/// The way back out is per id: a foreign trait can not be implemented for
/// a bare type parameter, so each admitted id gets its own.
macro_rules! well_known_ids {
    [$($id:ident),+ $(,)?] => {
        $(
            impl crate::TypeName for ::waymark_ids::$id {
                const NAME: &'static str = ::core::stringify!($id);
            }

            impl crate::WellKnownId for ::waymark_ids::$id {}

            impl ::core::convert::From<crate::UuidId<::waymark_ids::$id>> for ::waymark_ids::$id {
                fn from(id: crate::UuidId<Self>) -> Self {
                    id.0
                }
            }
        )+
    };
}

well_known_ids! {
    NodeId,
    InstanceId,
    ExecutionId,
    WorkflowVersionId,
}

/// An id on the wire: serialized as the id itself is, described as a
/// UUID.
//
// A proxy over the id type: the id's own serde carries it on the wire, and
// its schema is set here. The schema says `uuid`, which a bare string can
// not, and is a component, so that one identity reads as one type wherever
// it appears.
#[derive(Debug)]
pub struct UuidId<Id>(pub Id);

impl<Id> From<Id> for UuidId<Id> {
    fn from(id: Id) -> Self {
        Self(id)
    }
}

impl<Id> serde::Serialize for UuidId<Id>
where
    Id: WellKnownId,
    Id: serde::Serialize,
{
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

impl<'de, Id> serde::Deserialize<'de> for UuidId<Id>
where
    Id: WellKnownId,
    Id: serde::Deserialize<'de>,
{
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let id = Id::deserialize(deserializer)?;

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
