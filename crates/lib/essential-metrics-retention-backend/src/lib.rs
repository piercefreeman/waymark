//! Backend trait for the essential-metrics retention sweeper.

#![warn(missing_docs)]

/// Delete samples older than a cutoff.
pub trait ApplyRetention {
    /// Error type for the sweep.
    type Error: std::fmt::Debug;

    /// Delete every sample taken before `cutoff`; returns how many rows
    /// were deleted.
    fn apply_retention(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send + '_;
}
