pub use waymark_backends_core::{BackendError, BackendResult};
use waymark_ids::WorkflowVersionId;

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
    pub id: WorkflowVersionId,
    pub workflow_name: String,
    pub workflow_version: String,
    pub ir_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
}

/// Backend capability for registering workflow DAGs.
pub trait WorkflowRegistryBackend {
    fn upsert_workflow_version<'a>(
        &'a self,
        registration: &'a WorkflowRegistration,
    ) -> impl Future<Output = BackendResult<WorkflowVersionId>> + Send + 'a;

    fn get_workflow_versions<'a>(
        &'a self,
        ids: &'a [WorkflowVersionId],
    ) -> impl Future<Output = BackendResult<Vec<WorkflowVersion>>> + Send + 'a;
}
