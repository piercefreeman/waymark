use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use uuid::Uuid;
use waymark_proto::ast as ir;

#[derive(Debug, Default)]
pub struct WorkflowDagCache {
    map: HashMap<Uuid, Arc<waymark_dag::DAG>>,
}

#[derive(Debug, thiserror::Error)]
pub enum PopulateError {
    #[error("get workflow versions: {0}")]
    GetWorkflowVersions(#[source] waymark_backends_core::BackendError),

    #[error("invalid workflow IR: {0}")]
    IrProgramDecode(#[source] prost::DecodeError),

    #[error("invalid workflow DAG: {0}")]
    ConvertToDag(#[source] waymark_dag_builder::DagConversionError),
}

impl WorkflowDagCache {
    /// Populate the DAG cache for `workflow_version_ids` from
    /// the `registry_backend`.
    pub async fn populate<WorkflowRegistryBackend>(
        &mut self,
        registry_backend: &WorkflowRegistryBackend,
        workflow_version_ids: impl IntoIterator<Item = uuid::Uuid>,
    ) -> Result<(), PopulateError>
    where
        WorkflowRegistryBackend:
            ?Sized + waymark_workflow_registry_backend::WorkflowRegistryBackend,
    {
        let mut missing = HashSet::new();

        for workflow_version_id in workflow_version_ids {
            if !self.map.contains_key(&workflow_version_id) {
                missing.insert(workflow_version_id);
            }
        }

        if missing.is_empty() {
            return Ok(());
        }

        let missing: Vec<_> = missing.into_iter().collect();

        let versions = registry_backend
            .get_workflow_versions(&missing)
            .await
            .map_err(PopulateError::GetWorkflowVersions)?;
        for version in versions {
            let program = <ir::Program as prost::Message>::decode(&version.program_proto[..])
                .map_err(PopulateError::IrProgramDecode)?;
            let dag = waymark_dag_builder::convert_to_dag(&program)
                .map_err(PopulateError::ConvertToDag)?;
            self.map.insert(version.id, Arc::new(dag));
        }

        Ok(())
    }

    pub fn get(&self, workflow_version_id: &uuid::Uuid) -> Option<&Arc<waymark_dag::DAG>> {
        self.map.get(workflow_version_id)
    }
}
