use crate::Child;

use nix::{sys::signal::Signal, unistd::Pid};

/// Errors that can occur when sending a Unix signal to a child process.
#[derive(Debug, thiserror::Error)]
pub enum SendSignalError {
    /// The child no longer has a process id.
    #[error("child process is gone")]
    ChildGone,

    /// The process id does not fit into `i32`.
    #[error("pid overflows i32: {0}")]
    PidOverflow(std::num::TryFromIntError),

    /// The process id was zero, which is invalid for this operation.
    #[error("pid is zero: {0}")]
    PidZero(std::num::TryFromIntError),

    /// System call to send the signal failed.
    #[error("kill: {0}")]
    Kill(nix::errno::Errno),
}

impl Child {
    /// Sends `signal` to the child process.
    ///
    /// Passing `None` uses platform-default behavior for `kill(2)`.
    pub fn send_signal(&self, signal: impl Into<Option<Signal>>) -> Result<(), SendSignalError> {
        let pid = self.child.id().ok_or(SendSignalError::ChildGone)?;
        let pid = i32::try_from(pid).map_err(SendSignalError::PidOverflow)?;
        let pid = std::num::NonZeroI32::try_from(pid).map_err(SendSignalError::PidZero)?;
        let pid = Pid::from_raw(pid.get());
        nix::sys::signal::kill(pid, signal).map_err(SendSignalError::Kill)?;
        Ok(())
    }
}

/// Set the parent-death signal of the child process we're going to spawn.
/// This is the signal that the calling process will get when its parent dies.
///
/// Effectively we request that the child processes we spawn get SIGTERM if we
/// die.
pub(crate) fn inject_sigterm_pdeathsig(command: &mut tokio::process::Command) {
    use std::os::unix::process::CommandExt as _;

    let our_pid = std::process::id();

    unsafe {
        command.as_std_mut().pre_exec(move || {
            if our_pid != std::os::unix::process::parent_id() {
                // If the parent has exited - we abort the launch because it
                // won't be terminated when the parent exists.
                return Err(std::io::Error::other(
                    "parent process already exited, won't start child with a set pdeathsig",
                ));
            }

            nix::sys::prctl::set_pdeathsig(Signal::SIGTERM).map_err(std::io::Error::other)
        });
    }
}
