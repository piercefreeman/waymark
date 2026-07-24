//! Error types for the action-call-completions postgres backend.

use waymark_action_completions_reconciler_backend::CompletionKey;
use waymark_ids::InstanceId;

/// Error returned by [`super::super::PostgresBackend`] when recording
/// completions.
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
    #[error("diverging effect numbers for already-recorded completions: {0:?}")]
    DivergentEffectNumber(nonempty_collections::NEVec<CompletionKey<InstanceId>>),
}

impl waymark_action_completions_reconciler_backend::record_completions::Error for RecordError {
    fn kind(&self) -> waymark_action_completions_reconciler_backend::record_completions::ErrorKind {
        use waymark_action_completions_reconciler_backend::record_completions::ErrorKind;
        match self {
            Self::DivergentEffectNumber(_) => ErrorKind::DivergentEffectNumber,
            Self::Sqlx(_) | Self::OutOfRange(_) => ErrorKind::Internal,
        }
    }
}

/// Error returned by [`super::super::PostgresBackend`] when polling
/// completions by demand.
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
/// completions.
#[derive(Debug, thiserror::Error)]
pub enum AckError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A promise state id does not fit the database column type.
    #[error("value out of range for the database column: {0}")]
    OutOfRange(#[source] std::num::TryFromIntError),
}

/// Error returned by [`super::super::PostgresBackend`] when purging a VM's
/// completions.
#[derive(Debug, thiserror::Error)]
pub enum PurgeError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}
