//! Handle for a pinned workload.
//!
//! See [`PinnedHandle`].

use tokio_util::sync::CancellationToken;
use waymark_workload_pinning_core::UnpinMode;

/// A handle to a pinned workload.
///
/// Unpin explicitly via [`PinnedHandle::unpin`]. When dropped, the
/// workload is unpinned with [`UnpinMode::Release`] — not because
/// releasing is unconditionally valid (see [`UnpinMode`]: it is not),
/// but because a drop carries no decision, and of the two ways to be
/// wrong, a spurious release is the recoverable one: it is loud and ends
/// as soon as some attempt reaches a park decision, while a spurious
/// park is silent and permanent. Parking is reachable only through the
/// explicit call.
#[must_use]
pub struct PinnedHandle<WorkloadId> {
    id: Option<WorkloadId>,
    evict_tx: tokio::sync::mpsc::UnboundedSender<(WorkloadId, UnpinMode)>,
    fence: CancellationToken,
}

impl<WorkloadId> PinnedHandle<WorkloadId> {
    /// Create a new pinned handle.
    pub(crate) fn new(
        id: WorkloadId,
        evict_tx: tokio::sync::mpsc::UnboundedSender<(WorkloadId, UnpinMode)>,
        fence: CancellationToken,
    ) -> Self {
        Self {
            id: Some(id),
            evict_tx,
            fence,
        }
    }

    /// Return a reference to the wrapped workload ID.
    pub fn id(&self) -> &WorkloadId {
        // SAFETY: `id` is emptied only by `unpin` and by `Drop`, and
        // neither uses the handle after taking it — so any other caller
        // observes the `id` present.
        unsafe { self.id.as_ref().unwrap_unchecked() }
    }

    /// Resolve when the workload is fenced.
    ///
    /// Fencing means this node can no longer prove it holds the
    /// pinning: the pinning **lapsed** (its local deadline passed
    /// without a confirmed refresh) or was **lost** (a refresh reported
    /// it held by another node).  The workload may already be executing
    /// elsewhere.  What to do about that is the consumer's concern; this
    /// signal only reports the fact.
    ///
    /// Cancellation-safe: fencing is a latched state, so the future can
    /// be dropped and re-created freely.
    pub async fn fenced(&self) {
        self.fence.cancelled().await;
    }

    /// Unpin the workload with the given mode.
    ///
    /// See [`UnpinMode`] for what each mode obliges the caller to have
    /// established: neither of them is the safe default, and passing the
    /// wrong one is a spinloop or a stuck workload, not a no-op.
    pub fn unpin(mut self, mode: UnpinMode) {
        if let Some(id) = self.id.take() {
            let _ = self.evict_tx.send((id, mode));
        }
    }
}

impl<WorkloadId> Drop for PinnedHandle<WorkloadId> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.evict_tx.send((id, UnpinMode::Release));
        }
    }
}

impl<WorkloadId: std::fmt::Debug> std::fmt::Debug for PinnedHandle<WorkloadId> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedHandle")
            .field("id", self.id())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
