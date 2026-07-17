//! The outcome of a workload pinning manager run.

use crate::{MaintenanceError, PollLoopError};

/// The outcome of a workload pinning manager run.
///
/// Each sub-system result is preserved independently so callers can
/// inspect exactly what happened rather than receiving a collapsed error.
///
/// Use [`RunOutcomeFor`] to construct this type from a backend.
#[derive(Debug)]
#[must_use = "the run outcome should be inspected for errors"]
pub struct RunOutcome<PollError, KeepaliveError, ReleaseError> {
    /// Error from the poll loop, if any.
    pub poll_error: Option<PollLoopError<PollError>>,

    /// Error from the maintenance loop, if any.
    pub maintenance_error: Option<MaintenanceError<KeepaliveError, ReleaseError>>,

    /// If cleanup failed, the error from releasing remaining pinnings.
    /// `None` means either there were no remaining pinnings or they were
    /// released successfully.
    pub cleanup_error: Option<ReleaseError>,
}

/// Convenience alias for [`RunOutcome`] parameterized on a backend.
pub type RunOutcomeFor<Backend> = RunOutcome<
    <Backend as waymark_workload_pinning_backend::PollUnpinnedWorkloads>::Error,
    <Backend as waymark_workload_pinning_backend::KeepalivePinnings>::Error,
    <Backend as waymark_workload_pinning_backend::ReleasePinnings>::Error,
>;

impl<PollError, KeepaliveError, ReleaseError> RunOutcome<PollError, KeepaliveError, ReleaseError> {
    /// Returns `true` if every error field is `None`.
    pub fn is_ok(&self) -> bool {
        matches!(
            self,
            Self {
                poll_error: None,
                maintenance_error: None,
                cleanup_error: None
            }
        )
    }

    /// Returns `true` if any error field is `Some`.
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }
}
