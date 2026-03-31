#[derive(Debug, thiserror::Error)]
#[error("nil uuid is not allowed in IDs")]
pub struct NilUuidError;

macro_rules! uuid_type {
    ($name:ident) => {
        #[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "uuid::Uuid", into = "uuid::Uuid"))]
        #[repr(transparent)]
        pub struct $name(uuid::NonNilUuid);

        impl $name {
            pub const fn try_new(val: uuid::Uuid) -> Result<Self, NilUuidError> {
                let Some(val) = uuid::NonNilUuid::new(val) else {
                    return Err(NilUuidError);
                };
                Ok(Self(val))
            }

            #[cfg(feature = "uuid-v4")]
            pub fn new_uuid_v4() -> Self {
                // SAFETY: `uuid::Uuid::new_v4` is guaranteed to be non-nil.
                let val = unsafe {
                    let val = uuid::Uuid::new_v4();
                    uuid::NonNilUuid::new_unchecked(val)
                };
                Self(val)
            }

            #[cfg(feature = "uuid-v4")]
            #[deprecated = "use `new_uuid_v4` instead"]
            pub fn new_v4() -> Self {
                Self::new_uuid_v4()
            }
        }

        impl AsRef<uuid::NonNilUuid> for $name {
            fn as_ref(&self) -> &uuid::NonNilUuid {
                &self.0
            }
        }

        impl TryFrom<uuid::Uuid> for $name {
            type Error = NilUuidError;

            fn try_from(value: uuid::Uuid) -> Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }

        impl From<uuid::NonNilUuid> for $name {
            fn from(value: uuid::NonNilUuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for uuid::NonNilUuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl From<$name> for uuid::Uuid {
            fn from(value: $name) -> Self {
                value.0.get()
            }
        }

        impl core::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl core::str::FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let value = <uuid::Uuid as core::str::FromStr>::from_str(s)?;
                let value = value.try_into()?;
                Ok(Self(value))
            }
        }
    };
}

#[cfg(feature = "sqlx-postgres")]
macro_rules! uuid_type_sqlx_postgres {
    ($name:ident) => {
        impl sqlx::types::Type<sqlx::Postgres> for $name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::types::Type<sqlx::Postgres>>::type_info()
            }
        }

        impl sqlx::postgres::PgHasArrayType for $name {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }

        impl sqlx::Encode<'_, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                <uuid::Uuid as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.0.get(), buf)
            }
        }

        impl sqlx::Decode<'_, sqlx::Postgres> for $name {
            fn decode(
                value: sqlx::postgres::PgValueRef<'_>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let uuid = <uuid::Uuid as sqlx::Decode<'_, sqlx::Postgres>>::decode(value)?;
                Self::try_new(uuid).map_err(Into::into)
            }
        }
    };
}

macro_rules! uuid_types {
    ($($name:ident),+) => {
        $(
            uuid_type!($name);

            #[cfg(feature = "sqlx-postgres")]
            uuid_type_sqlx_postgres!($name);
        )*
    };
}

uuid_types![InstanceId, LockId, DispatchToken, ExecutionId];

#[deprecated = "use InstanceId instead"]
pub type ExecutorId = InstanceId;
