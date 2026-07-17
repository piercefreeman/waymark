//! Error types for the maintenance loop.

/// Errors that can occur in the maintenance loop.
#[derive(Debug, thiserror::Error)]
pub enum MaintenanceError<KeepaliveError, UnpinError> {
    /// Refreshing pinnings failed.
    #[error("refresh pinnings: {0}")]
    Refresh(#[source] KeepaliveError),

    /// Unpinning workloads failed.
    #[error("unpin workloads: {0}")]
    Unpin(#[source] UnpinError),

    /// The loop was force-shutdown.
    #[error("maintenance loop force shutdown")]
    ForceShutdown,
}

/// Convenience alias for [`MaintenanceError`] parameterized on a backend.
pub type MaintenanceErrorFor<Backend> = MaintenanceError<
    <Backend as waymark_workload_pinning_backend::KeepalivePinnings>::Error,
    <Backend as waymark_workload_pinning_backend::UnpinWorkloads>::Error,
>;
