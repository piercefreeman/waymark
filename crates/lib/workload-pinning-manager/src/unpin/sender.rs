//! The sending half of the unpin request channel.
//!
//! See [`UnpinSender`].

use waymark_workload_pinning_core::UnpinMode;

/// The sending half of the unpin request channel: a thin wrapper that
/// lets a caller ask for the durable unpin of an evicted workload
/// without handling the channel itself.
///
/// Cheap to clone, and every holder feeds the same [unpin
/// loop](super::run_unpin_loop): requests are coalesced into batches,
/// applied with retries, and a later request for a workload supersedes
/// one still pending for it.
pub struct UnpinSender<Id> {
    tx: tokio::sync::mpsc::UnboundedSender<(Id, UnpinMode)>,
}

impl<Id> UnpinSender<Id> {
    /// Request the durable unpin of a workload.
    ///
    /// Returns nothing and cannot fail: the unpin loop applies the
    /// request on its own schedule. If the loop has already exited the
    /// request is dropped, leaving that pinning to lapse — the same
    /// outcome as the loop giving up on it.
    pub fn request(&self, id: Id, mode: UnpinMode) {
        let _ = self.tx.send((id, mode));
    }
}

// Cloning the sender never depends on the id being cloneable, so this
// cannot be derived.
impl<Id> Clone for UnpinSender<Id> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

/// Wrap the sending half of an unpin request channel.
///
/// The channel itself is created by the caller, alongside the others it
/// wires up; the receiving half goes to
/// [`UnpinParams::unpin_rx`](super::UnpinParams::unpin_rx).
pub fn wrap_tx<Id>(tx: tokio::sync::mpsc::UnboundedSender<(Id, UnpinMode)>) -> UnpinSender<Id> {
    UnpinSender { tx }
}
