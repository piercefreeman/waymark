//! The per-VM adapter feeding the shared snapshot batcher.

use crate::snapshot_batcher::{SnapshotBatchError, SnapshotBatcherHandle};

/// Adapter that submits a VM's snapshots to the shared snapshot batcher.
pub struct SnapshotAdapter<VmId> {
    /// The VM whose snapshots this adapter persists.
    pub vm_id: VmId,

    /// Handle to the shared snapshot batcher.
    pub batcher: SnapshotBatcherHandle<VmId>,
}

impl<VmId> waymark_vm_driver_core::SnapshotPersister for SnapshotAdapter<VmId>
where
    VmId: Clone + Send + Sync + 'static,
{
    type Error = SnapshotBatchError;

    async fn persist_snapshot<'a>(&'a self, data: &'a [u8]) -> Result<(), Self::Error> {
        // The batcher owns the bytes until flush, so they must be copied here.
        // Each driver still awaits its own submission, preserving
        // persist-before-continue.
        match self
            .batcher
            .submit((self.vm_id.clone(), data.to_vec()))
            .await
        {
            Ok(outcome) => outcome,
            Err(waymark_batcher::Closed) => Err(SnapshotBatchError::Closed),
        }
    }
}
