use crate::Child;

use nix::{sys::signal::Signal, unistd::Pid};

#[derive(Debug, thiserror::Error)]
pub enum SendSignalError {
    #[error("child process is gone")]
    ChildGone,

    #[error("pid overflows i32: {0}")]
    PidOverflow(std::num::TryFromIntError),

    #[error("pid is zero: {0}")]
    PidZero(std::num::TryFromIntError),

    #[error("kill: {0}")]
    Kill(nix::errno::Errno),
}

impl Child {
    pub fn send_signal(&self, signal: impl Into<Option<Signal>>) -> Result<(), SendSignalError> {
        let pid = self.child.id().ok_or(SendSignalError::ChildGone)?;
        let pid = i32::try_from(pid).map_err(SendSignalError::PidOverflow)?;
        let pid = std::num::NonZeroI32::try_from(pid).map_err(SendSignalError::PidZero)?;
        let pid = Pid::from_raw(pid.get());
        nix::sys::signal::kill(pid, signal).map_err(SendSignalError::Kill)?;
        Ok(())
    }
}
