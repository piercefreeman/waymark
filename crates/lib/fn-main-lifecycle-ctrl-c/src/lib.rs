//! The OS's shutdown requests, turned into the lifecycle's shutdown.

#![warn(missing_docs)]

/// The OS's shutdown requests, held for [`listen`].
#[derive(Debug)]
pub struct Receivers {
    /// The Ctrl+C requests.
    pub ctrl_c: waymark_os_shutdown_requests::ctrl_c::Receiver,

    /// The termination requests.
    pub termination: waymark_os_shutdown_requests::termination::Receiver,
}

/// Error from [`install`].
#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    /// The Ctrl+C listener could not be installed.
    #[error("install the Ctrl+C listener: {0}")]
    CtrlC(#[source] waymark_os_shutdown_requests::InstallError),

    /// The termination listener could not be installed.
    #[error("install the termination listener: {0}")]
    Termination(#[source] waymark_os_shutdown_requests::InstallError),
}

/// Install the listeners for the OS's shutdown requests [`listen`] waits on.
pub fn install() -> Result<Receivers, InstallError> {
    let ctrl_c = waymark_os_shutdown_requests::ctrl_c::install().map_err(InstallError::CtrlC)?;
    let termination =
        waymark_os_shutdown_requests::termination::install().map_err(InstallError::Termination)?;

    Ok(Receivers {
        ctrl_c,
        termination,
    })
}

/// Wait for the OS's first shutdown request of either kind, then cancel
/// the stop-startup-then-shutdown token and wait for the shutdown token
/// to follow, so this task never returns before the shutdown request;
/// returns without a request of its own when the shutdown is requested
/// some other way first.
pub async fn listen(
    receivers: Receivers,
    stop_startup_then_shutdown_token: tokio_util::sync::CancellationToken,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), core::convert::Infallible> {
    let Receivers {
        mut ctrl_c,
        mut termination,
    } = receivers;

    tokio::select! {
        () = ctrl_c.recv() => {
            tracing::info!("Ctrl+C received; shutting down");
        }
        () = termination.recv() => {
            tracing::info!("termination requested; shutting down");
        }
        () = shutdown_token.cancelled() => {
            return Ok(());
        }
    }
    stop_startup_then_shutdown_token.cancel();

    // The lifecycle cancels the shutdown token at the startup's next
    // checkpoint, when the startup returns, or right away once the startup
    // is over; returning only then keeps this task from returning before
    // the shutdown request.
    shutdown_token.cancelled().await;

    Ok(())
}

#[cfg(test)]
mod tests;
