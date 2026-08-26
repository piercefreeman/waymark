//! Error types for the scheduler postgres backend.

/// Error returned by [`crate::PostgresBackend`] when polling for due
/// schedules.
#[derive(Debug, thiserror::Error)]
pub enum PollDueSchedulesError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}

/// Error returned by [`crate::PostgresBackend`] when registering
/// scheduled VM runtimes.
///
/// Fence mismatches and held overlap gates are not errors — they are
/// reported per row via
/// [`waymark_scheduler_backend::register_scheduled_vm_runtimes::Outcome`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterScheduledVmRuntimesError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}
