use std::sync::Arc;

/// Adapter that binds a VM id to a shared backend.
pub(crate) struct SnapshotAdapter<VmId, Backend> {
    pub vm_id: VmId,
    pub backend: Arc<Backend>,
}

impl<VmId, Backend> waymark_vm_driver_core::SnapshotPersister for SnapshotAdapter<VmId, Backend>
where
    Backend: waymark_state_vm_runtimes_backend::StoreSnapshot<VmId = VmId> + Send + Sync,
    <Backend as waymark_state_vm_runtimes_backend::StoreSnapshot>::Error: std::fmt::Debug,
    VmId: Sync,
{
    type Error = <Backend as waymark_state_vm_runtimes_backend::StoreSnapshot>::Error;

    async fn persist_snapshot<'a>(&'a self, data: &'a [u8]) -> Result<(), Self::Error> {
        self.backend.store_snapshot(&self.vm_id, data).await
    }
}
