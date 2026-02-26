use uuid::Uuid;

pub use waymark_backends_core::{BackendError, BackendResult};

/// Registration payload for storing workflow DAG metadata.
#[derive(Clone, Debug)]
pub struct WorkflowRegistration {
    pub workflow_name: String,
    pub workflow_version: String,
    pub ir_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
}

#[derive(Clone, Debug)]
/// Stored workflow version metadata and IR payload.
pub struct WorkflowVersion {
    pub id: Uuid,
    pub workflow_name: String,
    pub workflow_version: String,
    pub ir_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
}

/// Backend capability for registering workflow DAGs.
#[async_trait::async_trait]
pub trait WorkflowRegistryBackend: Send + Sync {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid>;

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>>;
}
