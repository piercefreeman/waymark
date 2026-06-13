//! Error types for the workload-pinning postgres backend.

/// Error returned by [`super::PostgresBackend`] when polling for unpinned instances.
#[derive(Debug, thiserror::Error)]
pub enum PollError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}

/// Error returned by [`super::PostgresBackend`] when refreshing pinnings.
#[derive(Debug, thiserror::Error)]
pub enum RefreshError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}

/// Error returned by [`super::PostgresBackend`] when releasing pinnings.
#[derive(Debug, thiserror::Error)]
pub enum ReleaseError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}
