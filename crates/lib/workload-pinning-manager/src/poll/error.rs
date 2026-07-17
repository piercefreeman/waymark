//! Error types for the poll loop.

/// Errors that can occur in the poll loop.
#[derive(Debug, thiserror::Error)]
pub enum PollLoopError<PollError> {
    /// Polling from the backend failed.
    #[error("poll: {0}")]
    Poll(#[source] PollError),

    /// The maintenance loop channel closed — the poll loop cannot register
    /// new work.
    #[error("maintenance loop closed")]
    MaintenanceClosed,

    /// The maintenance loop did not acknowledge a batch registration.
    #[error("maintenance loop did not acknowledge batch")]
    MaintenanceUnresponsive,

    /// The pinned-handle receiver closed — the consumer is gone.
    #[error("pinned handle receiver closed")]
    PinnedReceiverClosed,
}

/// Convenience alias for [`PollLoopError`] parameterized on a backend.
pub type PollLoopErrorFor<Backend> =
    PollLoopError<<Backend as waymark_workload_pinning_backend::PollUnpinnedWorkloads>::Error>;
