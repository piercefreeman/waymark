//! Managed wrapper around `tokio::process::Child` with graceful shutdown helpers.

#![warn(missing_docs)]

#[cfg(unix)]
mod platform_unix;

#[cfg(target_os = "linux")]
mod platform_linux;

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

/// Spawns a managed child process.
///
/// The child inherits `stderr` and is configured with `kill_on_drop(true)`.
/// On Unix, the child is also configured with a parent-death signal so it
/// receives `SIGTERM` when the parent process exits.
pub fn spawn(command: impl Into<tokio::process::Command>) -> Result<Child, std::io::Error> {
    let mut command = command.into();

    command.stderr(Stdio::inherit()).kill_on_drop(true);

    #[cfg(target_os = "linux")]
    platform_linux::inject_sigterm_pdeathsig(&mut command);

    let child = command.spawn()?;

    tracing::debug!(pid = child.id(), "spawned child process");

    Ok(Child { child })
}

/// Errors that can occur while shutting down a managed child.
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    /// Triggering graceful termination failed.
    #[error("graceful termination: {0}")]
    GracefulTermination(#[source] GracefulTerminationError),

    /// Waiting after graceful termination failed.
    #[error("graceful termination wait: {0}")]
    GracefulTerminationWait(#[source] std::io::Error),

    /// Force-kill fallback failed.
    #[error("kill: {0}")]
    Kill(#[source] KillAndWaitError),
}

/// Errors that can occur when force-killing a child and waiting for exit.
#[derive(Debug, thiserror::Error)]
pub enum KillAndWaitError {
    /// Sending kill failed.
    #[error("kill: {0}")]
    Kill(#[source] std::io::Error),

    /// Waiting for exit failed.
    #[error("wait: {0}")]
    Wait(#[source] WaitWithTimeoutError),
}

/// Errors returned by [`Child::wait_with_timeout`].
#[derive(Debug, thiserror::Error)]
pub enum WaitWithTimeoutError {
    /// Waiting for process exit failed.
    #[error("wait: {0}")]
    Wait(#[source] std::io::Error),

    /// The timeout elapsed before the process exited.
    #[error("process didn't exit in time")]
    Timeout {
        /// Elapsed timeout duration.
        elapsed: std::time::Duration,
    },
}

impl Child {
    /// Waits for the child process to exit.
    pub async fn wait(&mut self) -> Result<std::process::ExitStatus, std::io::Error> {
        self.child.wait().await
    }

    /// Waits for the child process to exit up to `timeout`.
    ///
    /// Returns [`WaitWithTimeoutError::Timeout`] when the timeout expires.
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

    /// Attempts graceful shutdown first, then force-kills as fallback.
    ///
    /// If `graceful_termination_timeout` is set and graceful termination is
    /// supported on this platform, waits up to that duration before falling
    /// back to kill.
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

    /// Sends a kill signal to the child process.
    pub fn send_kill(&mut self) -> Result<(), std::io::Error> {
        self.child.start_kill()
    }

    /// Force-kills the child process and waits for it to exit.
    ///
    /// When `timeout` is `Some`, waiting is bounded by that duration.
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

    /// Returns the underlying unmanaged `tokio` child process handle.
    pub async fn unmanage(self) -> tokio::process::Child {
        self.child
    }
}

impl AsRef<tokio::process::Child> for Child {
    fn as_ref(&self) -> &tokio::process::Child {
        &self.child
    }
}
