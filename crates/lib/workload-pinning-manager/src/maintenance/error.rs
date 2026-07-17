//! Error types for the maintenance loop.

/// Errors that can occur in the maintenance loop.
#[derive(Debug, thiserror::Error)]
pub enum MaintenanceError<KeepaliveError, ReleaseError> {
    /// Refreshing pinnings failed.
    #[error("refresh pinnings: {0}")]
    Refresh(#[source] KeepaliveError),

    /// Releasing pinnings failed.
    #[error("release pinnings: {0}")]
    Release(#[source] ReleaseError),

    /// The loop was force-shutdown.
    #[error("maintenance loop force shutdown")]
    ForceShutdown,
}

/// Convenience alias for [`MaintenanceError`] parameterized on a backend.
pub type MaintenanceErrorFor<Backend> = MaintenanceError<
    <Backend as waymark_workload_pinning_backend::KeepalivePinnings>::Error,
    <Backend as waymark_workload_pinning_backend::ReleasePinnings>::Error,
>;
