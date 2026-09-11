//! Termination: the stop an orchestrator or the system sends.
//!
//! SIGTERM on unix; on Windows the console close, logoff, and shutdown
//! events, which mean the same thing to the process and are received as
//! one kind. Which of them a shutdown delivers depends on the session:
//! logoff to a console process in an interactive session, shutdown to one
//! in session 0, a service or a container.

/// A held termination listener.
///
/// Obtained from [`install`]. Each receiver is an independent
/// subscription: several can coexist and each observes every request
/// landing after its own installation, subject to coalescing.
#[derive(Debug)]
pub struct Receiver {
    #[cfg(unix)]
    inner: tokio::signal::unix::Signal,

    #[cfg(windows)]
    close: tokio::signal::windows::CtrlClose,

    #[cfg(windows)]
    logoff: tokio::signal::windows::CtrlLogoff,

    #[cfg(windows)]
    shutdown: tokio::signal::windows::CtrlShutdown,
}

impl Receiver {
    /// Wait for the next request.
    ///
    /// Completes for a request that landed at any point after the previous
    /// completion, including while no `recv` was pending. Requests that
    /// land before this observes one of them are coalesced into it.
    pub async fn recv(&mut self) {
        #[cfg(unix)]
        self.inner
            .recv()
            .await
            .expect("tokio's signal listener never closes");

        #[cfg(windows)]
        tokio::select! {
            closed = self.close.recv() => {
                closed.expect("tokio's signal listener never closes");
            }
            logged_off = self.logoff.recv() => {
                logged_off.expect("tokio's signal listener never closes");
            }
            shut_down = self.shutdown.recv() => {
                shut_down.expect("tokio's signal listener never closes");
            }
        }
    }
}

/// Register a termination listener with the current tokio runtime's
/// signal driver and return its receiver.
///
/// # Panics
///
/// On unix, panics when called outside a tokio runtime, as every tokio
/// signal constructor does. A runtime without the I/O driver is reported
/// as an error instead.
pub fn install() -> Result<Receiver, crate::InstallError> {
    #[cfg(unix)]
    let receiver = Receiver {
        inner: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|source| crate::InstallError { source })?,
    };

    #[cfg(windows)]
    let receiver = Receiver {
        close: tokio::signal::windows::ctrl_close()
            .map_err(|source| crate::InstallError { source })?,
        logoff: tokio::signal::windows::ctrl_logoff()
            .map_err(|source| crate::InstallError { source })?,
        shutdown: tokio::signal::windows::ctrl_shutdown()
            .map_err(|source| crate::InstallError { source })?,
    };

    Ok(receiver)
}
