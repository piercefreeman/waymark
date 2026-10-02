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

/// A single managed child process handle.
///
/// Will kill the child process on drop.
#[derive(Debug)]
#[must_use = "dropping a `Child` kills its process"]
pub struct Child {
    /// The child process.
    child: tokio::process::Child,
}

/// Spawns a managed child process.
///
/// The child is configured with `kill_on_drop(true)`. On Linux, the child
/// is also configured with a parent-death signal so it receives `SIGTERM`
/// when the parent process exits.
pub fn spawn(command: impl Into<tokio::process::Command>) -> Result<Child, std::io::Error> {
    let mut command = command.into();

    command.kill_on_drop(true);

    #[cfg(target_os = "linux")]
    platform_linux::inject_sigterm_pdeathsig(&mut command);

    let child = command.spawn()?;

    tracing::debug!(pid = child.id(), "spawned child process");

    Ok(Child { child })
}

/// How [`Child::shutdown`] ended, and the exit status it collected.
///
/// The outcome records what the wrapper did, not what ended the process:
/// the process can exit on its own at any moment, including right before
/// the kill reaches it.
#[derive(Debug)]
pub enum ShutdownOutcome {
    /// The process exited before any kill was sent.
    Exited(std::process::ExitStatus),

    /// A kill was sent, because the graceful wait ran out, because none
    /// was asked for, or because the platform has no graceful termination.
    /// The status is whatever the process exited with; it is the process's
    /// own exit when the process exited before the kill reached it.
    KillSent(std::process::ExitStatus),
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

    /// The process was still running when the kill timeout ran out.
    #[error("process still running {}s after the kill", .elapsed.as_secs_f64())]
    KillTimeout {
        /// The kill timeout that ran out.
        elapsed: std::time::Duration,
    },
}

/// Why [`Child::wait_until`] did not observe the exit.
#[derive(Debug, thiserror::Error)]
pub enum WaitUntilError<Until> {
    /// Waiting for the exit failed.
    #[error("wait: {0}")]
    Wait(#[source] std::io::Error),

    /// `until` resolved first, with this output; the exit was not
    /// observed. The process may have exited at the same moment: the
    /// select between the two is unbiased.
    #[error("the wait was cut short")]
    Until(Until),
}

/// Errors that can occur when force-killing a child and waiting for exit.
#[derive(Debug, thiserror::Error)]
pub enum KillAndWaitError {
    /// Sending kill failed.
    #[error("kill: {0}")]
    Kill(#[source] std::io::Error),

    /// Waiting for exit failed.
    #[error("wait: {0}")]
    Wait(#[source] std::io::Error),
}

/// An error that did not observe the child's exit, with the `Child` it
/// happened to.
///
/// Nothing about the process follows from such an error: it may have
/// exited and been reaped elsewhere, or it may be running with the wait
/// refused, as under a seccomp policy. So the `Child` comes back with the
/// error, as it was, for the caller to decide what to do with it. The
/// carrier says nothing of its own: it prints and sources as `error`.
#[derive(Debug)]
pub struct ErrorWithChild<T> {
    /// The `Child` the failed step ran on.
    pub child: Child,

    /// What failed.
    pub error: T,
}

impl<T> std::fmt::Display for ErrorWithChild<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<T> std::error::Error for ErrorWithChild<T>
where
    T: std::error::Error,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}

impl<T> ErrorWithChild<T> {
    /// The same `Child` with the error mapped through `map_fn`.
    pub fn map<OtherError>(
        self,
        map_fn: impl FnOnce(T) -> OtherError,
    ) -> ErrorWithChild<OtherError> {
        ErrorWithChild {
            child: self.child,
            error: map_fn(self.error),
        }
    }
}

/// What an observation of the child found: the handle back while no exit
/// was observed, or the exit status once it was.
///
/// A handle is a process whose exit has not been observed; the process
/// may have exited already. Observing the exit consumes the handle, so a
/// handle whose exit was observed cannot exist, and the methods that end
/// the process ([`Child::shutdown`], [`Child::kill_and_wait`],
/// [`Child::kill_and_wait_with_timeout`]) never signal a collected one.
#[derive(Debug)]
#[must_use = "a discarded `Observed::Running` drops its `Child`, which kills the process"]
pub enum Observed {
    /// The process was still running when observed; here is its handle.
    Running(Child),

    /// The process has exited with this status.
    Exited(std::process::ExitStatus),
}

impl Child {
    /// Waits for the child process to exit.
    ///
    /// Consumes the handle with the exit; a failed wait hands it back in
    /// the error.
    pub async fn wait(
        mut self,
    ) -> Result<std::process::ExitStatus, ErrorWithChild<std::io::Error>> {
        match self.child.wait().await {
            Ok(exit_status) => Ok(exit_status),
            Err(error) => Err(ErrorWithChild { child: self, error }),
        }
    }

    /// Waits for the child process to exit, or for `until` to resolve
    /// first.
    ///
    /// Consumes the handle with the exit; `until` resolving first, with
    /// its output, or a failed wait hands it back in the error.
    pub async fn wait_until<Until>(
        mut self,
        until: Until,
    ) -> Result<std::process::ExitStatus, ErrorWithChild<WaitUntilError<Until::Output>>>
    where
        Until: Future,
    {
        // Cancel safe on tokio's side.
        tokio::select! {
            waited = self.child.wait() => match waited {
                Ok(exit_status) => Ok(exit_status),
                Err(error) => Err(ErrorWithChild {
                    child: self,
                    error: WaitUntilError::Wait(error),
                }),
            },
            output = until => Err(ErrorWithChild {
                child: self,
                error: WaitUntilError::Until(output),
            }),
        }
    }

    /// Checks whether the child process has exited, without waiting.
    ///
    /// Hands the handle back while the process is still running, and the
    /// exit status once it has exited; a failed wait hands it back in the
    /// error.
    //
    // `allow`, not `expect`: the lint fires only when tokio's `rt` feature is
    // enabled, which this crate does not ask for but any build alongside a
    // crate that does unifies in (tokio's `Child` is larger with it), so an
    // `expect` is unfulfilled in the builds without it.
    #[allow(
        clippy::result_large_err,
        reason = "the error carries the handle back by design"
    )]
    pub fn try_wait(mut self) -> Result<Observed, ErrorWithChild<std::io::Error>> {
        match self.child.try_wait() {
            Ok(Some(exit_status)) => Ok(Observed::Exited(exit_status)),
            Ok(None) => Ok(Observed::Running(self)),
            Err(error) => Err(ErrorWithChild { child: self, error }),
        }
    }

    /// Waits for the child process to exit up to `timeout`.
    ///
    /// Hands the handle back when the timeout expires with the process
    /// still running, and the exit status once it has exited; a failed
    /// wait hands it back in the error.
    pub async fn wait_with_timeout(
        mut self,
        timeout: std::time::Duration,
    ) -> Result<Observed, ErrorWithChild<std::io::Error>> {
        // Cancel safe on tokio's side.
        match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(Ok(exit_status)) => Ok(Observed::Exited(exit_status)),
            Ok(Err(error)) => Err(ErrorWithChild { child: self, error }),
            Err(tokio::time::error::Elapsed { .. }) => Ok(Observed::Running(self)),
        }
    }

    /// Observes the `Child` in `slot`, if any, through `observe_fn`, such
    /// as [`try_wait`](Self::try_wait).
    ///
    /// The exit status once the exit is observed, the slot then empty;
    /// `None` while the process is still running, or when the slot is
    /// empty. A failed observation leaves the `Child` in the slot.
    pub fn try_observe_in<Error>(
        slot: &mut Option<Self>,
        observe_fn: impl FnOnce(Self) -> Result<Observed, ErrorWithChild<Error>>,
    ) -> Result<Option<std::process::ExitStatus>, Error> {
        let Some(child) = slot.take() else {
            return Ok(None);
        };

        match observe_fn(child) {
            Ok(Observed::Running(child)) => {
                *slot = Some(child);
                Ok(None)
            }
            Ok(Observed::Exited(exit_status)) => Ok(Some(exit_status)),
            Err(ErrorWithChild { child, error }) => {
                *slot = Some(child);
                Err(error)
            }
        }
    }

    /// Attempts graceful shutdown first, then force-kills as fallback.
    ///
    /// If `graceful_termination_timeout` is set and graceful termination is
    /// supported on this platform, waits up to that duration before falling
    /// back to kill. The returned [`ShutdownOutcome`] says which of the two
    /// happened.
    pub async fn shutdown(
        self,
        graceful_termination_timeout: impl Into<Option<std::time::Duration>>,
        kill_timeout: impl Into<Option<std::time::Duration>>,
    ) -> Result<ShutdownOutcome, ErrorWithChild<ShutdownError>> {
        if let Err(error) = self.trigger_graceful_termination().await {
            return Err(ErrorWithChild {
                child: self,
                error: ShutdownError::GracefulTermination(error),
            });
        }

        let child = if Self::CAN_GRACEFULLY_TERMINATE
            && let Some(graceful_termination_timeout) = graceful_termination_timeout.into()
        {
            let observed = self
                .wait_with_timeout(graceful_termination_timeout)
                .await
                .map_err(|error| error.map(ShutdownError::GracefulTerminationWait))?;
            match observed {
                Observed::Exited(exit_status) => return Ok(ShutdownOutcome::Exited(exit_status)),
                // The timeout ran out: continue to kill.
                Observed::Running(child) => child,
            }
        } else {
            self
        };

        let exit_status = match kill_timeout.into() {
            None => child
                .kill_and_wait()
                .await
                .map_err(|error| error.map(ShutdownError::Kill))?,
            Some(kill_timeout) => {
                let observed = child
                    .kill_and_wait_with_timeout(kill_timeout)
                    .await
                    .map_err(|error| error.map(ShutdownError::Kill))?;
                match observed {
                    Observed::Exited(exit_status) => exit_status,
                    Observed::Running(child) => {
                        return Err(ErrorWithChild {
                            child,
                            error: ShutdownError::KillTimeout {
                                elapsed: kill_timeout,
                            },
                        });
                    }
                }
            }
        };

        Ok(ShutdownOutcome::KillSent(exit_status))
    }

    /// Sends a kill signal to the child process.
    pub fn send_kill(&mut self) -> Result<(), std::io::Error> {
        self.child.start_kill()
    }

    /// Force-kills the child process and waits for it to exit.
    pub async fn kill_and_wait(
        mut self,
    ) -> Result<std::process::ExitStatus, ErrorWithChild<KillAndWaitError>> {
        if let Err(error) = self.send_kill() {
            return Err(ErrorWithChild {
                child: self,
                error: KillAndWaitError::Kill(error),
            });
        }

        self.wait()
            .await
            .map_err(|error| error.map(KillAndWaitError::Wait))
    }

    /// Force-kills the child process and waits for it to exit up to
    /// `timeout`.
    ///
    /// Hands the handle back when the timeout expires with the process
    /// still running, the kill sent, and the exit status once it has
    /// exited.
    pub async fn kill_and_wait_with_timeout(
        mut self,
        timeout: std::time::Duration,
    ) -> Result<Observed, ErrorWithChild<KillAndWaitError>> {
        if let Err(error) = self.send_kill() {
            return Err(ErrorWithChild {
                child: self,
                error: KillAndWaitError::Kill(error),
            });
        }

        self.wait_with_timeout(timeout)
            .await
            .map_err(|error| error.map(KillAndWaitError::Wait))
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
