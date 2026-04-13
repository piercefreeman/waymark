use std::sync::Arc;

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use waymark_core_backend::QueuedInstance;

use crate::{hydrated_instance::HydratedInstance, runloop::WorkflowDagCache};

pub struct Params<'a, WorkflowRegistryBackend: ?Sized> {
    /// Cache of workflow DAGs keyed by workflow version ID to avoid repeated hydration work.
    pub workflow_cache: &'a mut WorkflowDagCache,
    /// Backend used to fetch workflow definitions that are missing from the local cache.
    pub registry_backend: &'a WorkflowRegistryBackend,
    /// Claimed instances that need DAG references attached before shard execution.
    pub instances: NEVec<QueuedInstance>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("caching missing DAGs: {0}")]
    WorkflowDagCachePopulate(#[source] crate::runloop::workflow_dag_cache::PopulateError),

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

    workflow_cache
        .populate(
            registry_backend,
            instances
                .iter()
                .map(|instance| instance.workflow_version_id),
        )
        .await
        .map_err(Error::WorkflowDagCachePopulate)?;

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
