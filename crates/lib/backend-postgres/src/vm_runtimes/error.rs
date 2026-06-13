//! Error types for the state-vm-runtimes postgres backend.

/// Error returned by [`super::PostgresBackend`] when storing a VM snapshot.
#[derive(Debug, thiserror::Error)]
pub enum StoreSnapshotError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// The VM has not been registered yet. Call
    /// [`super::PostgresBackend::register_vm_runtime`] first.
    #[error("vm runtime not registered: {0}")]
    NotRegistered(waymark_ids::InstanceId),
}

/// Error returned by [`super::PostgresBackend`] when loading a VM snapshot
/// for revival.
#[derive(Debug, thiserror::Error)]
pub enum LoadForReviveError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// No snapshot exists for the given VM.
    #[error("vm runtime not found: {0}")]
    NotFound(waymark_ids::InstanceId),
}
