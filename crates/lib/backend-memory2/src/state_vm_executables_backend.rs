//! In-memory backend for VM executable loading.

use waymark_ids::WorkflowVersionId;

impl waymark_state_vm_executables_backend::HasExecutableId for crate::MemoryBackend {
    type ExecutableId = WorkflowVersionId;
}

impl waymark_state_vm_executables_backend::LoadExecutable for crate::MemoryBackend {
    type Error = LoadError;

    async fn load_executable<'a>(
        &'a self,
        id: &'a Self::ExecutableId,
    ) -> Result<Vec<u8>, Self::Error> {
        let guard = self.executables_by_id.lock().unwrap();
        guard.get(id).cloned().ok_or(LoadError::NotFound(*id))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("executable not found: {0}")]
    NotFound(WorkflowVersionId),
}
