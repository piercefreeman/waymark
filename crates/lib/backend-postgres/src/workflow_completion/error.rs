//! Error types for workflow completion persistence.

/// Error returned when recording a workflow completion or exception fails.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// A different outcome was already recorded for this VM.
    #[error("conflicting outcome already recorded for vm {0}")]
    Conflict(waymark_ids::InstanceId),
}

/// Error returned when polling for a workflow outcome fails.
#[derive(Debug, thiserror::Error)]
pub enum PollError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// The stored row violates the result/error XOR invariant.
    #[error("corrupt outcome row for vm {0}: both result and error present, or neither")]
    CorruptRow(waymark_ids::InstanceId),
}
