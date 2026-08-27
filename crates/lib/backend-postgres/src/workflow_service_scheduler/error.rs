//! Error types for the workflow-service scheduler postgres backend.

use waymark_scheduler_core::ParseScheduleStatusError;

/// Error returned by [`crate::PostgresBackend`] when upserting a
/// schedule.
#[derive(Debug, thiserror::Error)]
pub enum UpsertScheduleError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}

/// Error returned by [`crate::PostgresBackend`] when reading a schedule.
#[derive(Debug, thiserror::Error)]
pub enum GetScheduleError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// The stored status text does not name a schedule status.
    #[error("invalid status in schedule row: {0}")]
    InvalidStatus(#[source] ParseScheduleStatusError),
}

/// Error returned by [`crate::PostgresBackend`] when listing schedules.
#[derive(Debug, thiserror::Error)]
pub enum ListSchedulesError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A stored status text does not name a schedule status.
    #[error("invalid status in schedule row: {0}")]
    InvalidStatus(#[source] ParseScheduleStatusError),
}

/// Error returned by [`crate::PostgresBackend`] when updating a
/// schedule's status.
#[derive(Debug, thiserror::Error)]
pub enum UpdateScheduleStatusError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}

/// Error returned by [`crate::PostgresBackend`] when deleting a
/// schedule.
#[derive(Debug, thiserror::Error)]
pub enum DeleteScheduleError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}
