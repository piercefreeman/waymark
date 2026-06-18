//! In-memory backend for the workflow service.

use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_workflow_service_vm_runtimes_backend::register_vm_runtime;

// ---------------------------------------------------------------------------
// HasVmId / HasExecutableId
// ---------------------------------------------------------------------------

impl waymark_workflow_service_vm_runtimes_backend::HasVmId for crate::MemoryBackend {
    type VmId = InstanceId;
}

impl waymark_workflow_service_vm_runtimes_backend::HasExecutableId for crate::MemoryBackend {
    type ExecutableId = WorkflowVersionId;
}

// ---------------------------------------------------------------------------
// RegisterVmRuntime
// ---------------------------------------------------------------------------

impl waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntime for crate::MemoryBackend {
    type Error = RegisterVmRuntimeError;

    async fn register_vm_runtime(
        &self,
        vm_id: &Self::VmId,
        executable_id: &Self::ExecutableId,
        snapshot: &[u8],
    ) -> Result<(), Self::Error> {
        let mut snapshots = self.vm_runtime_snapshots.lock().unwrap();
        if snapshots.contains_key(vm_id) {
            return Err(RegisterVmRuntimeError::AlreadyExists(*vm_id));
        }
        snapshots.insert(
            *vm_id,
            crate::SnapshotEntry {
                executable_id: *executable_id,
                snapshot: snapshot.to_vec(),
            },
        );

        let mut pinnings = self.workload_pinnings.lock().unwrap();
        pinnings.insert(*vm_id, None);

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum RegisterVmRuntimeError {
    #[error("vm runtime already registered: {0}")]
    AlreadyExists(InstanceId),
}

impl register_vm_runtime::Error for RegisterVmRuntimeError {
    fn kind(&self) -> register_vm_runtime::ErrorKind {
        match self {
            Self::AlreadyExists(_) => register_vm_runtime::ErrorKind::AlreadyRegistered,
        }
    }
}
