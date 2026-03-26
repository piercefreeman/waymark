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
