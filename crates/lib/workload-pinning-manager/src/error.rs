//! Error types for the workload pinning manager.

use waymark_workload_pinning_backend::poll::ErrorKind as BackendErrorKind;

/// Errors that can occur during the workload pinning management loop.
#[derive(Debug, thiserror::Error)]
pub enum Error<PollError, KeepaliveError, ReleaseError> {
    /// Polling from the backend failed.
    #[error("poll: {0}")]
    Poll(#[source] PollError),

    /// Refreshing pinnings failed.
    #[error("refresh pinnings: {0}")]
    Refresh(#[source] KeepaliveError),

    /// Releasing pinnings failed.
    #[error("release pinnings: {0}")]
    Release(#[source] ReleaseError),

    /// The pinned-handle receiver was closed.
    #[error("pinned handle receiver closed")]
    PinnedReceiverClosed,

    /// Releasing pinnings at cleanup failed.
    #[error("release pinnings at cleanup: {0}")]
    Cleanup(#[source] ReleaseError),
}

impl<PollError, KeepaliveError, ReleaseError> Error<PollError, KeepaliveError, ReleaseError>
where
    PollError: waymark_workload_pinning_backend::poll::Error,
{
    /// Returns the kind of the error for categorization.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Poll(error) => match error.kind() {
                BackendErrorKind::NoInstances => ErrorKind::NoWork,
                BackendErrorKind::Internal => ErrorKind::Internal,
            },
            Self::Refresh(_) | Self::Release(_) | Self::PinnedReceiverClosed | Self::Cleanup(_) => {
                ErrorKind::Internal
            }
        }
    }
}

/// Stable categories for workload pinning management errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// No VM workloads were available for polling.
    NoWork,

    /// An internal error occurred.
    Internal,
}
