//! What the crate's tests share: the task error, the startup error, and
//! the task futures that observe the shutdown.

/// The tests' task error and startup step error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct MessageError(pub &'static str);

/// The startup error the tests run with: its own error, as any crate's
/// would be, knowing which of its variants is the
/// [`StoppedError`](crate::startup::StoppedError), with the `Error` form
/// being itself.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error(transparent)]
    Stopped(#[from] crate::startup::StoppedError),

    #[error(transparent)]
    Message(#[from] MessageError),
}

impl crate::startup::Error for StartupError {
    fn as_stopped(&self) -> Option<&crate::startup::StoppedError> {
        match self {
            Self::Stopped(stopped) => Some(stopped),
            Self::Message(_) => None,
        }
    }
}

impl waymark_error_coercion::IntoError for StartupError {
    type Error = Self;

    fn into_error(self) -> Self {
        self
    }
}

/// The error a task observing the shutdown token returns with.
#[derive(Debug, thiserror::Error)]
#[error("shutdown observed")]
pub struct ShutdownError;

impl From<ShutdownError> for MessageError {
    fn from(_: ShutdownError) -> Self {
        MessageError("shutdown observed")
    }
}

/// A supervised task that runs until the shutdown token is cancelled.
pub async fn until_shutdown(
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), ShutdownError> {
    shutdown_token.cancelled().await;
    Err(ShutdownError)
}
