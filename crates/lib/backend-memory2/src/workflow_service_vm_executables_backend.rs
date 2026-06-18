//! In-memory backend for the workflow service VM executables.

use waymark_ids::WorkflowVersionId;

impl waymark_workflow_service_vm_executables_backend::HasExecutableId for crate::MemoryBackend {
    type ExecutableId = WorkflowVersionId;
}

impl waymark_workflow_service_vm_executables_backend::UpsertExecutable for crate::MemoryBackend {
    type Error = UpsertError;

    async fn upsert_executable<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        bytes: &'a [u8],
    ) -> Result<Self::ExecutableId, Self::Error> {
        let mut guard = self.executables.lock().unwrap();
        let key = (name.to_owned(), version.to_owned());

        if let Some((existing_id, existing_bytes)) = guard.get(&key) {
            if existing_bytes == bytes {
                return Ok(*existing_id);
            }
            return Err(UpsertError::Conflict);
        }

        let id = WorkflowVersionId::new_uuid_v4();
        guard.insert(key, (id, bytes.to_vec()));

        // Also store by ID for LoadExecutable.
        self.executables_by_id
            .lock()
            .unwrap()
            .insert(id, bytes.to_vec());

        Ok(id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UpsertError {
    #[error("executable conflict")]
    Conflict,
}

impl waymark_workflow_service_vm_executables_backend::Error for UpsertError {
    fn kind(&self) -> waymark_workflow_service_vm_executables_backend::ErrorKind {
        match self {
            Self::Conflict => waymark_workflow_service_vm_executables_backend::ErrorKind::Conflict,
        }
    }
}
