//! Handle for a pinned workload.
//!
//! See [`PinnedHandle`].

use waymark_workload_pinning_core::UnpinMode;

/// A handle to a pinned workload.
///
/// Unpin explicitly via [`PinnedHandle::unpin`]. When dropped, the
/// workload is unpinned with [`UnpinMode::Release`]: releasing is valid
/// unless parking is justified, and parking — which requires a
/// justification — is only available through the explicit call.
#[must_use]
pub struct PinnedHandle<WorkloadId> {
    id: Option<WorkloadId>,
    evict_tx: tokio::sync::mpsc::UnboundedSender<(WorkloadId, UnpinMode)>,
}

impl<WorkloadId> PinnedHandle<WorkloadId> {
    /// Create a new pinned handle.
    pub(crate) fn new(
        id: WorkloadId,
        evict_tx: tokio::sync::mpsc::UnboundedSender<(WorkloadId, UnpinMode)>,
    ) -> Self {
        Self {
            id: Some(id),
            evict_tx,
        }
    }

    /// Return a reference to the wrapped workload ID.
    pub fn id(&self) -> &WorkloadId {
        // SAFETY: `id` is emptied only by `unpin` and by `Drop`, and
        // neither uses the handle after taking it — so any other caller
        // observes the `id` present.
        unsafe { self.id.as_ref().unwrap_unchecked() }
    }

    /// Unpin the workload with the given mode.
    ///
    /// Pass [`UnpinMode::Park`] only when there is a liveness guarantee
    /// for the workload to be unparked in the future when needed — or
    /// when it never needs to be.
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
