//! The timestamp for our own operations: the lowest common denominator
//! of every database backend we aim to use.
//!
//! An instant here is one every backend can hold, so it can be written
//! to, read from and compared against any of them without a per-backend
//! check. The clock's instant type reads far more than a store can keep,
//! and a store handed an instant outside its range fails the whole
//! request as its own error. The range is checked once, on construction,
//! so a backend never sees an instant it cannot hold, and a transport
//! turns the construction error into its own rejection with nothing to
//! know.
//!
//! The range is the one every backend shares at microsecond resolution:
//! Postgres's `timestamptz` runs from 4713 BC to 294276 AD; ClickHouse's
//! `DateTime64` runs from 0000-01-01 to 9999-12-31 at precisions up to
//! seven digits. Their intersection is 0000-01-01T00:00:00Z through
//! 9999-12-31T23:59:59.999999Z. Resolution is not checked here: a finer
//! fraction is the store's to keep or round.

#![warn(missing_docs)]

/// An instant within 0000-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z.
///
/// Instants compare and order as instants do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(chrono::DateTime<chrono::Utc>);

/// An instant outside the range every store holds.
#[derive(Debug, thiserror::Error)]
#[error(
    "the instant must be within 0000-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z, got {instant}"
)]
pub struct TimestampRangeError {
    /// The instant asked for.
    pub instant: chrono::DateTime<chrono::Utc>,
}

impl Timestamp {
    /// The instant `instant`, when it is within the range.
    pub fn new(instant: chrono::DateTime<chrono::Utc>) -> Result<Self, TimestampRangeError> {
        if instant < floor() || instant > ceiling() {
            return Err(TimestampRangeError { instant });
        }
        Ok(Self(instant))
    }

    /// The instant.
    pub fn get(self) -> chrono::DateTime<chrono::Utc> {
        self.0
    }

    /// The present instant.
    ///
    /// # Panics
    ///
    /// Panics when the clock reads past the year 9999, which no clock
    /// this runs on does.
    #[cfg(feature = "now")]
    pub fn now() -> Self {
        Self::new(chrono::Utc::now()).expect("the clock is within the year 9999")
    }
}

/// The earliest instant in the range: 0000-01-01T00:00:00Z.
fn floor() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(-62_167_219_200, 0)
        .expect("0000-01-01 is within chrono's range")
}

/// The latest instant in the range: 9999-12-31T23:59:59.999999Z.
fn ceiling() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(253_402_300_799, 999_999_000)
        .expect("9999-12-31 is within chrono's range")
}

#[cfg(feature = "serde")]
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

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<Deserializer>(deserializer: Deserializer) -> Result<Self, Deserializer::Error>
    where
        Deserializer: serde::Deserializer<'de>,
    {
        let instant =
            <chrono::DateTime<chrono::Utc> as serde::Deserialize>::deserialize(deserializer)?;
        match Self::new(instant) {
            Ok(timestamp) => Ok(timestamp),
            Err(error) => Err(serde::de::Error::custom(error)),
        }
    }
}

#[cfg(feature = "sqlx-postgres")]
impl sqlx::types::Type<sqlx::Postgres> for Timestamp {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <chrono::DateTime<chrono::Utc> as sqlx::types::Type<sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "sqlx-postgres")]
impl sqlx::postgres::PgHasArrayType for Timestamp {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <chrono::DateTime<chrono::Utc> as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

#[cfg(feature = "sqlx-postgres")]
impl sqlx::Encode<'_, sqlx::Postgres> for Timestamp {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <chrono::DateTime<chrono::Utc> as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(
            &self.0, buf,
        )
    }
}

#[cfg(feature = "sqlx-postgres")]
impl sqlx::Decode<'_, sqlx::Postgres> for Timestamp {
    fn decode(value: sqlx::postgres::PgValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
        let instant =
            <chrono::DateTime<chrono::Utc> as sqlx::Decode<'_, sqlx::Postgres>>::decode(value)?;
        Self::new(instant).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;
