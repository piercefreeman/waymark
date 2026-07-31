//! Error types for workflow completion persistence.

/// Error returned when recording workflow terminal outcomes fails.
///
/// Per-row conflicts are not errors — they are reported via
/// [`waymark_workflow_completion_backend::RecordingSuccess::SomeConflicted`].
#[derive(Debug, thiserror::Error)]
pub enum RecordOutcomesError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}

/// Error returned when polling for a workflow outcome fails.
#[derive(Debug, thiserror::Error)]
pub enum PollError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// The stored row violates the result/error XOR invariant.
    #[error("corrupt outcome row for vm {0}: both result and error present, or neither")]
    CorruptRow(waymark_ids::InstanceId),
}
