use uuid::Uuid;
use waymark_workflow_registry_backend::{
    BackendError, BackendResult, WorkflowRegistration, WorkflowRegistryBackend, WorkflowVersion,
};

#[async_trait::async_trait]
impl WorkflowRegistryBackend for crate::MemoryBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        let mut guard = self
            .workflow_versions
            .lock()
            .expect("workflow versions poisoned");
        let key = (
            registration.workflow_name.clone(),
            registration.workflow_version.clone(),
        );
        if let Some((id, existing)) = guard.get(&key) {
            if existing.ir_hash != registration.ir_hash {
                return Err(BackendError::Message(format!(
                    "workflow version already exists with different IR hash: {}@{}",
                    registration.workflow_name, registration.workflow_version
                )));
            }
            return Ok(*id);
        }

        let id = Uuid::new_v4();
        guard.insert(key, (id, registration.clone()));
        Ok(id)
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let guard = self
            .workflow_versions
            .lock()
            .expect("workflow versions poisoned");
        let mut versions = Vec::new();
        for (id, registration) in guard.values() {
            if ids.contains(id) {
                versions.push(WorkflowVersion {
                    id: *id,
                    workflow_name: registration.workflow_name.clone(),
                    workflow_version: registration.workflow_version.clone(),
                    ir_hash: registration.ir_hash.clone(),
                    program_proto: registration.program_proto.clone(),
                    concurrent: registration.concurrent,
                });
            }
        }
        Ok(versions)
    }
}
