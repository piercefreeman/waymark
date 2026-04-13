use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use uuid::Uuid;
use waymark_core_backend::QueuedInstance;
use waymark_proto::ast as ir;

use crate::hydrated_instance::HydratedInstance;

pub struct Params<'a, WorkflowRegistryBackend: ?Sized> {
    /// Cache of workflow DAGs keyed by workflow version ID to avoid repeated hydration work.
    pub workflow_cache: &'a mut HashMap<Uuid, Arc<waymark_dag::DAG>>,
    /// Backend used to fetch workflow definitions that are missing from the local cache.
    pub registry_backend: &'a WorkflowRegistryBackend,
    /// Claimed instances that need DAG references attached before shard execution.
    pub instances: NEVec<QueuedInstance>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("caching missing DAGs: {0}")]
    CacheMissingDags(#[source] CacheMissingDagsError),

    #[error("workflow version not found: {workflow_version_id}")]
    WorkflowCacheGetNone { workflow_version_id: uuid::Uuid },
}

/// Loads and caches workflow DAG definitions for instances.
///
/// Before instances can be executed, their workflow definitions must be fetched and
/// converted to executable DAGs. This operation:
/// - Identifies instances missing their DAG from the cache
/// - Batch-fetches missing workflow versions from the backend
/// - Parses IR protobuf and converts to executable DAG representation
/// - Caches DAGs for reuse across instances with the same workflow version
/// - Attaches DAG references to instances for shard execution
///
/// Caching avoids repeated fetches for workflows used by multiple instances.
pub async fn run<WorkflowRegistryBackend>(
    params: Params<'_, WorkflowRegistryBackend>,
) -> Result<NEVec<HydratedInstance>, Error>
where
    WorkflowRegistryBackend: ?Sized + waymark_workflow_registry_backend::WorkflowRegistryBackend,
{
    let Params {
        workflow_cache,
        registry_backend,
        instances,
    } = params;

    cache_missing_dags(
        registry_backend,
        workflow_cache,
        instances
            .iter()
            .map(|instance| instance.workflow_version_id),
    )
    .await
    .map_err(Error::CacheMissingDags)?;

    let hydrate_instance = |instance: QueuedInstance| -> Result<HydratedInstance, _> {
        let dag = workflow_cache
            .get(&instance.workflow_version_id)
            .ok_or_else(|| Error::WorkflowCacheGetNone {
                workflow_version_id: instance.workflow_version_id,
            })?;
        let dag = Arc::clone(dag);
        Ok(HydratedInstance { dag, instance })
    };

    let hydrated_instances = instances
        .into_nonempty_iter()
        .map(hydrate_instance)
        .collect::<Result<NEVec<HydratedInstance>, Error>>()?;

    Ok(hydrated_instances)
}

#[derive(Debug, thiserror::Error)]
pub enum CacheMissingDagsError {
    #[error("get workflow versions: {0}")]
    GetWorkflowVersions(#[source] waymark_backends_core::BackendError),

    #[error("invalid workflow IR: {0}")]
    IrProgramDecode(#[source] prost::DecodeError),

    #[error("invalid workflow DAG: {0}")]
    ConvertToDag(#[source] waymark_dag_builder::DagConversionError),
}

async fn cache_missing_dags<WorkflowRegistryBackend>(
    registry_backend: &WorkflowRegistryBackend,
    workflow_cache: &mut HashMap<Uuid, Arc<waymark_dag::DAG>>,
    workflow_version_ids: impl IntoIterator<Item = uuid::Uuid>,
) -> Result<(), CacheMissingDagsError>
where
    WorkflowRegistryBackend: ?Sized + waymark_workflow_registry_backend::WorkflowRegistryBackend,
{
    let mut missing = HashSet::new();

    for workflow_version_id in workflow_version_ids {
        if !workflow_cache.contains_key(&workflow_version_id) {
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
        .map_err(CacheMissingDagsError::GetWorkflowVersions)?;
    for version in versions {
        let program = <ir::Program as prost::Message>::decode(&version.program_proto[..])
            .map_err(CacheMissingDagsError::IrProgramDecode)?;
        let dag = waymark_dag_builder::convert_to_dag(&program)
            .map_err(CacheMissingDagsError::ConvertToDag)?;
        workflow_cache.insert(version.id, Arc::new(dag));
    }

    Ok(())
}
