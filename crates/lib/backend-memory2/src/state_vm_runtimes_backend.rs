//! In-memory backend for VM runtime state persistence.

use waymark_ids::{InstanceId, WorkflowVersionId};

// ---------------------------------------------------------------------------
// HasVmId / HasExecutableId
// ---------------------------------------------------------------------------

impl waymark_state_vm_runtimes_backend::HasVmId for crate::MemoryBackend {
    type VmId = InstanceId;
}

impl waymark_state_vm_runtimes_backend::HasExecutableId for crate::MemoryBackend {
    type ExecutableId = WorkflowVersionId;
}

// ---------------------------------------------------------------------------
// StoreSnapshot
// ---------------------------------------------------------------------------

impl waymark_state_vm_runtimes_backend::StoreSnapshot for crate::MemoryBackend {
    type Error = StoreSnapshotError;

    async fn store_snapshot<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        data: &'a [u8],
    ) -> Result<(), Self::Error> {
        let mut guard = self.vm_runtime_snapshots.lock().unwrap();
        let Some(entry) = guard.get_mut(vm_id) else {
            return Err(StoreSnapshotError::NotRegistered(*vm_id));
        };
        entry.snapshot = data.to_vec();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// LoadForRevive
// ---------------------------------------------------------------------------

impl waymark_state_vm_runtimes_backend::LoadForRevive for crate::MemoryBackend {
    type Error = LoadForReviveError;

    async fn load_for_revive<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
    ) -> Result<waymark_state_vm_runtimes_backend::RevivePayload<Self::ExecutableId>, Self::Error>
    {
        let guard = self.vm_runtime_snapshots.lock().unwrap();
        let entry = guard
            .get(vm_id)
            .ok_or(LoadForReviveError::NotFound(*vm_id))?;
        Ok(waymark_state_vm_runtimes_backend::RevivePayload {
            snapshot: entry.snapshot.clone(),
            executable_id: entry.executable_id,
        })
    }
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum StoreSnapshotError {
    #[error("vm runtime not registered: {0}")]
    NotRegistered(InstanceId),
}

#[derive(Debug, thiserror::Error)]
pub enum LoadForReviveError {
    #[error("vm runtime not found: {0}")]
    NotFound(InstanceId),
}
