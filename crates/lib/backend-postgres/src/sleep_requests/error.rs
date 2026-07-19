//! Error types for the sleep-requests postgres backend.

use waymark_ids::InstanceId;
use waymark_sleep_reconciler_backend::SleepKey;

/// Error returned by [`super::super::PostgresBackend`] when recording
/// sleep requests.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A promise state id or effect number does not fit the database
    /// column type.
    #[error("value out of range for the database column: {0}")]
    OutOfRange(#[source] std::num::TryFromIntError),

    /// Records for these keys already exist with a different effect
    /// number — the "same effect ⇒ same pair" invariant is broken.
    #[error("diverging effect numbers for already-recorded sleeps: {0:?}")]
    DivergentEffectNumber(nonempty_collections::NEVec<SleepKey<InstanceId>>),
}

impl waymark_sleep_reconciler_backend::record_sleeps::Error for RecordError {
    fn kind(&self) -> waymark_sleep_reconciler_backend::record_sleeps::ErrorKind {
        use waymark_sleep_reconciler_backend::record_sleeps::ErrorKind;
        match self {
            Self::DivergentEffectNumber(_) => ErrorKind::DivergentEffectNumber,
            Self::Sqlx(_) | Self::OutOfRange(_) => ErrorKind::Internal,
        }
    }
}

/// Error returned by [`super::super::PostgresBackend`] when polling due
/// sleeps by demand.
#[derive(Debug, thiserror::Error)]
pub enum PollError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A stored value does not fit the in-memory representation.
    #[error("stored value out of range: {0}")]
    OutOfRange(#[source] std::num::TryFromIntError),
}

/// Error returned by [`super::super::PostgresBackend`] when acking
/// sleeps.
#[derive(Debug, thiserror::Error)]
pub enum AckError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A promise state id does not fit the database column type.
    #[error("value out of range for the database column: {0}")]
    OutOfRange(#[source] std::num::TryFromIntError),
}

/// Error returned by [`super::super::PostgresBackend`] when purging a
/// VM's sleeps.
#[derive(Debug, thiserror::Error)]
pub enum PurgeError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}
