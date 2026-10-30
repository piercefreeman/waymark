//! The recheck request from a VM's [`EffectHandler`](crate::EffectHandler)
//! to its [`Adopter`](super::Adopter): a fresh emission came back already
//! recorded.
//!
//! A recording whose statement committed but reported failure leaves the
//! row locked here under a lock nobody tracks, and its retry finds the
//! row already recorded — the same answer a replayed effect gets.  The
//! handler cannot tell the two apart, so it requests a recheck, and the
//! adopter runs a pass whatever its state: a replayed row is tracked here
//! or held elsewhere and the pass settles as before, an untracked row
//! held here is watched until its lock lapses.
//!
//! The request is a level: requests between two looks of the adopter
//! collapse into one, and a request wakes a pending wait.

use tokio::sync::watch;

/// Open a recheck channel: the handler's [`Tx`] and the adopter's [`Rx`].
pub fn new() -> (Tx, Rx) {
    let (sender, receiver) = watch::channel(());
    (Tx(sender), Rx(receiver))
}

/// The effect handler's half of the recheck channel.
#[derive(Debug)]
pub struct Tx(watch::Sender<()>);

impl Tx {
    /// Request a recheck.
    pub fn request_recheck(&self) {
        self.0.send_replace(());
    }
}

/// The [`Tx`] is gone: no recheck is ever requested again.
#[derive(Debug, thiserror::Error)]
#[error("the recheck tx is gone")]
pub struct TxGoneError;

/// The adopter's half of the recheck channel.
#[derive(Debug)]
pub struct Rx(watch::Receiver<()>);

impl Rx {
    /// Resolves once a recheck has been requested since the last
    /// [`clear_recheck_request`](Self::clear_recheck_request); fails once
    /// the [`Tx`] is gone.
    pub async fn recheck_requested(&mut self) -> Result<(), TxGoneError> {
        self.0
            .changed()
            .await
            .map_err(|_: watch::error::RecvError| TxGoneError)
    }

    /// Clear the requests so far.
    pub fn clear_recheck_request(&mut self) {
        self.0.mark_unchanged();
    }
}
