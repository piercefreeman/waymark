//! Error types for the action-call-requests postgres backend.

use waymark_action_effect_reconciler_backend::ActionCallRequestKey;
use waymark_ids::InstanceId;

/// Error returned by [`super::super::PostgresBackend`] when recording
/// requests.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A promise state id or effect number does not fit the database
    /// column type.
    #[error("value out of range for the database column: {0}")]
    OutOfRange(#[source] std::num::TryFromIntError),

    /// Requests for these keys already exist with a different payload —
    /// replay determinism is broken.
    #[error("diverging payloads for already-recorded requests: {0:?}")]
    DivergentPayload(nonempty_collections::NEVec<ActionCallRequestKey<InstanceId>>),
}

impl waymark_action_effect_reconciler_backend::record_action_call_requests::Error for RecordError {
    fn kind(
        &self,
    ) -> waymark_action_effect_reconciler_backend::record_action_call_requests::ErrorKind {
        use waymark_action_effect_reconciler_backend::record_action_call_requests::ErrorKind;
        match self {
            Self::DivergentPayload(_) => ErrorKind::DivergentPayload,
            Self::Sqlx(_) | Self::OutOfRange(_) => ErrorKind::Internal,
        }
    }
}

/// Error returned by [`super::super::PostgresBackend`] when locking a VM's
/// requests for delivery.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A stored value does not fit the in-memory representation.
    #[error("stored value out of range: {0}")]
    OutOfRange(#[source] std::num::TryFromIntError),
}

/// Error returned by [`super::super::PostgresBackend`] when renewing
/// request locks.
#[derive(Debug, thiserror::Error)]
pub enum RenewError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A promise state id does not fit the database column type.
    #[error("value out of range for the database column: {0}")]
    OutOfRange(#[source] std::num::TryFromIntError),
}

/// Error returned by [`super::super::PostgresBackend`] when unlocking
/// requests.
#[derive(Debug, thiserror::Error)]
pub enum UnlockError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A promise state id does not fit the database column type.
    #[error("value out of range for the database column: {0}")]
    OutOfRange(#[source] std::num::TryFromIntError),
}
