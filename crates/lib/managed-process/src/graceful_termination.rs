use crate::Child;

/// Error returned when graceful termination cannot be initiated.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct GracefulTerminationError {
    #[cfg(not(unix))]
    inner: core::convert::Infallible,

    #[cfg(unix)]
    inner: crate::SendSignalError,
}

impl Child {
    /// Whether this platform has a graceful termination step, i.e. whether
    /// [`Child::shutdown`] can end with [`crate::ShutdownOutcome::Exited`].
    pub const CAN_GRACEFULLY_TERMINATE: bool = cfg!(unix);

    pub(crate) async fn trigger_graceful_termination(
        &self,
    ) -> Result<(), GracefulTerminationError> {
        #[cfg(unix)]
        self.send_signal(nix::sys::signal::Signal::SIGTERM)
            .map_err(|error| GracefulTerminationError { inner: error })?;

        Ok(())
    }
}
