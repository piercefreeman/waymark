//! Ctrl+C: the interrupt a person sends from the keyboard.

/// A held Ctrl+C listener.
///
/// Obtained from [`install`]. Each receiver is an independent
/// subscription: several can coexist and each observes every press
/// landing after its own installation, subject to coalescing.
#[derive(Debug)]
pub struct Receiver {
    #[cfg(unix)]
    inner: tokio::signal::unix::Signal,

    #[cfg(windows)]
    inner: tokio::signal::windows::CtrlC,
}

impl Receiver {
    /// Wait for the next press.
    ///
    /// Completes for a press that landed at any point after the previous
    /// completion, including while no `recv` was pending. Presses that
    /// land before this observes one of them are coalesced into it.
    pub async fn recv(&mut self) {
        self.inner
            .recv()
            .await
            .expect("tokio's signal listener never closes");
    }
}

/// Register a Ctrl+C listener with the current tokio runtime's signal
/// driver and return its receiver.
///
/// # Panics
///
/// On unix, panics when called outside a tokio runtime, as every tokio
/// signal constructor does. A runtime without the I/O driver is reported
/// as an error instead.
pub fn install() -> Result<Receiver, crate::InstallError> {
    #[cfg(unix)]
    let inner = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|source| crate::InstallError { source })?;

    #[cfg(windows)]
    let inner =
        tokio::signal::windows::ctrl_c().map_err(|source| crate::InstallError { source })?;

    Ok(Receiver { inner })
}
