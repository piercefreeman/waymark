#[cfg(unix)]
mod platform_unix;

mod graceful_termination;

pub use self::graceful_termination::*;

#[cfg(unix)]
pub use self::platform_unix::*;

use std::process::Stdio;

/// A single managed child process handle.
///
/// Will kill the child process on drop.
pub struct Child {
    /// The child process.
    child: tokio::process::Child,
}

pub fn spawn(mut command: tokio::process::Command) -> Result<Child, std::io::Error> {
    command.stderr(Stdio::inherit()).kill_on_drop(true);

    let child = command.spawn()?;

    tracing::debug!(pid = child.id(), "spawned child process");

    Ok(Child { child })
}

#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    #[error("graceful termination: {0}")]
    GracefulTermination(#[source] GracefulTerminationError),

    #[error("graceful termination wait: {0}")]
    GracefulTerminationWait(#[source] std::io::Error),

    #[error("kill: {0}")]
    Kill(#[source] KillAndWaitError),
}

#[derive(Debug, thiserror::Error)]
pub enum KillAndWaitError {
    #[error("kill: {0}")]
    Kill(#[source] std::io::Error),

    #[error("wait: {0}")]
    Wait(#[source] WaitWithTimeoutError),
}

#[derive(Debug, thiserror::Error)]
pub enum WaitWithTimeoutError {
    #[error("wait: {0}")]
    Wait(#[source] std::io::Error),

    #[error("process didn't exit in time")]
    Timeout { elapsed: std::time::Duration },
}

impl Child {
    pub async fn wait(&mut self) -> Result<std::process::ExitStatus, std::io::Error> {
        self.child.wait().await
    }

    pub async fn wait_with_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<std::process::ExitStatus, WaitWithTimeoutError> {
        let timeout_result = tokio::time::timeout(timeout, self.wait()).await;

        match timeout_result {
            // Process exited.
            Ok(Ok(exit_code)) => Ok(exit_code),

            // `wait()` errored.
            Ok(Err(error)) => Err(WaitWithTimeoutError::Wait(error)),

            // Timeout.
            Err(tokio::time::error::Elapsed { .. }) => {
                Err(WaitWithTimeoutError::Timeout { elapsed: timeout })
            }
        }
    }

    pub async fn shutdown(
        mut self,
        graceful_termination_timeout: impl Into<Option<std::time::Duration>>,
        kill_timeout: impl Into<Option<std::time::Duration>>,
    ) -> Result<std::process::ExitStatus, ShutdownError> {
        self.trigger_graceful_termination()
            .await
            .map_err(ShutdownError::GracefulTermination)?;

        if Self::CAN_GRACEFULLY_TERMINATE
            && let Some(graceful_termination_timeout) = graceful_termination_timeout.into()
        {
            let result = self.wait_with_timeout(graceful_termination_timeout).await;

            match result {
                // Process exited.
                Ok(exit_code) => return Ok(exit_code),

                // `wait()` errored.
                Err(WaitWithTimeoutError::Wait(error)) => {
                    return Err(ShutdownError::GracefulTerminationWait(error));
                }

                // Timeout.
                Err(WaitWithTimeoutError::Timeout { .. }) => {
                    // continue to kill
                }
            }
        }

        self.kill_and_wait(kill_timeout)
            .await
            .map_err(ShutdownError::Kill)
    }

    pub fn send_kill(&mut self) -> Result<(), std::io::Error> {
        self.child.start_kill()
    }

    pub async fn kill_and_wait(
        mut self,
        timeout: impl Into<Option<std::time::Duration>>,
    ) -> Result<std::process::ExitStatus, KillAndWaitError> {
        self.send_kill().map_err(KillAndWaitError::Kill)?;

        let exit_code = if let Some(timeout) = timeout.into() {
            self.wait_with_timeout(timeout)
                .await
                .map_err(KillAndWaitError::Wait)?
        } else {
            self.wait()
                .await
                .map_err(|err| KillAndWaitError::Wait(WaitWithTimeoutError::Wait(err)))?
        };

        Ok(exit_code)
    }

    pub async fn unmanage(self) -> tokio::process::Child {
        self.child
    }
}

impl AsRef<tokio::process::Child> for Child {
    fn as_ref(&self) -> &tokio::process::Child {
        &self.child
    }
}
