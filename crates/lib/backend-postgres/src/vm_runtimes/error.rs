//! Error types for the state-vm-runtimes postgres backend.

/// Error returned by [`super::PostgresBackend`] when storing VM snapshots.
#[derive(Debug, thiserror::Error)]
pub enum StoreSnapshotsError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}

/// Error returned by [`super::PostgresBackend`] when loading a VM snapshot
/// for revival.
#[derive(Debug, thiserror::Error)]
pub enum LoadForReviveError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// No snapshot exists for the given VM.
    #[error("vm runtime not found: {0}")]
    NotFound(waymark_ids::InstanceId),
}
