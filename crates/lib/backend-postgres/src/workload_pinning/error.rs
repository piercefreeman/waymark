//! Error types for the workload-pinning postgres backend.

/// Error returned by [`super::PostgresBackend`] when polling for unpinned workloads.
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

/// Error returned by [`super::PostgresBackend`] when unpinning workloads.
#[derive(Debug, thiserror::Error)]
pub enum UnpinError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}
