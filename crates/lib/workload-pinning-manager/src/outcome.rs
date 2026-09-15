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
pub struct RunOutcome<PollError, KeepaliveError, UnpinError> {
    /// Error from the poll loop, if any.
    pub poll_error: Option<PollLoopError<PollError>>,

    /// Error from the maintenance loop, if any.
    pub maintenance_error: Option<MaintenanceError<KeepaliveError>>,

    /// Error from the unpin loop, if any.
    ///
    /// The loop gave up on unpins it could not apply — including the
    /// pinnings routed to it for cleanup when the other loops exited.
    /// Those pinnings are left to lapse on their own.
    pub unpin_error: Option<UnpinError>,
}

/// Convenience alias for [`RunOutcome`] parameterized on a backend.
pub type RunOutcomeFor<Backend> = RunOutcome<
    <Backend as waymark_workload_pinning_backend::PollUnpinnedWorkloads>::Error,
    <Backend as waymark_workload_pinning_backend::KeepalivePinnings>::Error,
    <Backend as waymark_workload_pinning_backend::UnpinWorkloads>::Error,
>;

impl<PollError, KeepaliveError, UnpinError> RunOutcome<PollError, KeepaliveError, UnpinError> {
    /// Returns `true` if every error field is `None`.
    pub fn is_ok(&self) -> bool {
        matches!(
            self,
            Self {
                poll_error: None,
                maintenance_error: None,
                unpin_error: None
            }
        )
    }

    /// Returns `true` if any error field is `Some`.
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }

    /// The run as a result: `Ok` when no loop failed, the outcome itself
    /// otherwise.
    pub fn into_result(self) -> Result<(), Self> {
        if self.is_ok() { Ok(()) } else { Err(self) }
    }
}

impl<PollError, KeepaliveError, UnpinError> std::fmt::Display
    for RunOutcome<PollError, KeepaliveError, UnpinError>
where
    PollError: std::fmt::Display,
    KeepaliveError: std::fmt::Display,
    UnpinError: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "workload pinning manager")?;
        let mut separator = ": ";
        if let Some(error) = &self.poll_error {
            write!(f, "{separator}poll loop: {error}")?;
            separator = "; ";
        }
        if let Some(error) = &self.maintenance_error {
            write!(f, "{separator}maintenance loop: {error}")?;
            separator = "; ";
        }
        if let Some(error) = &self.unpin_error {
            write!(f, "{separator}unpin loop: {error}")?;
        }
        Ok(())
    }
}

impl<PollError, KeepaliveError, UnpinError> std::error::Error
    for RunOutcome<PollError, KeepaliveError, UnpinError>
where
    PollError: std::fmt::Display + std::fmt::Debug,
    KeepaliveError: std::fmt::Display + std::fmt::Debug,
    UnpinError: std::fmt::Display + std::fmt::Debug,
{
}
