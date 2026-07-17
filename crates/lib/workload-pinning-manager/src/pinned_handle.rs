//! Handle for a pinned workload.
//!
//! See [`PinnedHandle`].

/// A handle to a pinned workload.
///
/// When dropped, the workload ID is sent back to the workload manager
/// for unpinning.
#[must_use]
pub struct PinnedHandle<WorkloadId> {
    id: Option<WorkloadId>,
    evict_tx: tokio::sync::mpsc::UnboundedSender<WorkloadId>,
}

impl<WorkloadId> PinnedHandle<WorkloadId> {
    /// Create a new pinned handle.
    pub(crate) fn new(
        id: WorkloadId,
        evict_tx: tokio::sync::mpsc::UnboundedSender<WorkloadId>,
    ) -> Self {
        Self {
            id: Some(id),
            evict_tx,
        }
    }

    /// Return a reference to the wrapped workload ID.
    pub fn id(&self) -> &WorkloadId {
        // SAFETY: `PinnedHandle` is not dropped, so the `id` must be present.
        unsafe { self.id.as_ref().unwrap_unchecked() }
    }
}

impl<WorkloadId> Drop for PinnedHandle<WorkloadId> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.evict_tx.send(id);
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
