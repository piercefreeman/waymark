//! Backend trait for the observability-events retention sweeper.

#![warn(missing_docs)]

/// Delete events older than a cutoff.
pub trait ApplyRetention {
    /// Error type for the sweep.
    type Error: std::fmt::Debug;

    /// Delete every event stamped before `cutoff`; returns how many rows
    /// were deleted.
    fn apply_retention(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send + '_;
}
