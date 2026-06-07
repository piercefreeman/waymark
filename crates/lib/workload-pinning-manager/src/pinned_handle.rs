//! Handle for a pinned instance.
//!
//! See [`PinnedHandle`].

/// A handle to a pinned instance.
///
/// When dropped, the instance ID is sent back to the workload manager
/// for unpinning.
#[must_use]
pub struct PinnedHandle<InstanceId> {
    id: Option<InstanceId>,
    evict_tx: tokio::sync::mpsc::UnboundedSender<InstanceId>,
}

impl<InstanceId> PinnedHandle<InstanceId> {
    /// Create a new pinned handle.
    pub(crate) fn new(
        id: InstanceId,
        evict_tx: tokio::sync::mpsc::UnboundedSender<InstanceId>,
    ) -> Self {
        Self {
            id: Some(id),
            evict_tx,
        }
    }

    /// Return a reference to the wrapped instance ID.
    pub fn id(&self) -> &InstanceId {
        // SAFETY: `PinnedHandle` is not dropped, so the `id` must be present.
        unsafe { self.id.as_ref().unwrap_unchecked() }
    }
}

impl<InstanceId> Drop for PinnedHandle<InstanceId> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.evict_tx.send(id);
        }
    }
}

impl<InstanceId: std::fmt::Debug> std::fmt::Debug for PinnedHandle<InstanceId> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedHandle")
            .field("id", self.id())
            .finish_non_exhaustive()
    }
}
