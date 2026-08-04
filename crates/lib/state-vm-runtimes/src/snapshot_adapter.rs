use waymark_batcher::BatcherHandle;

/// The batcher item: a VM id and its owned snapshot bytes. The bytes must be
/// owned because the batcher holds them until the batch flushes.
pub type SnapshotJob<VmId> = (VmId, Vec<u8>);

/// The per-submission result the snapshot batcher hands back.
pub type SnapshotOutcome = Result<(), SnapshotBatchError>;

/// Error from persisting a snapshot through the shared batcher.
///
/// The concrete backend error is logged in full at the flush — the one
/// place that has it — and the waiters receive the category: their drive
/// loops fail and the workloads re-pin either way.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum SnapshotBatchError {
    /// The batched store failed; nothing of the batch was persisted.
    #[error("batched snapshot store failed")]
    Store,

    /// The snapshot batcher has shut down and can no longer persist.
    #[error("snapshot batcher is closed")]
    Closed,
}

/// Adapter that submits a VM's snapshots to the shared snapshot batcher.
pub struct SnapshotAdapter<VmId> {
    /// The VM whose snapshots this adapter persists.
    pub vm_id: VmId,

    /// Handle to the shared snapshot batcher.
    pub batcher: BatcherHandle<SnapshotJob<VmId>, SnapshotOutcome>,
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
