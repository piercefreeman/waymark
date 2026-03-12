use std::{collections::HashMap, sync::Arc};

use uuid::Uuid;
use waymark_core_backend::QueuedInstance;
use waymark_proto::ast as ir;

use crate::runloop::RunLoopError;

pub struct Params<'a, WorkflowRegistryBackend: ?Sized> {
    pub workflow_cache: &'a mut HashMap<Uuid, Arc<waymark_dag::DAG>>,
    pub registry_backend: &'a WorkflowRegistryBackend,
    pub instances: &'a mut [QueuedInstance],
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
) -> Result<(), RunLoopError>
where
    WorkflowRegistryBackend: ?Sized + waymark_workflow_registry_backend::WorkflowRegistryBackend,
{
    let Params {
        workflow_cache,
        registry_backend,
        instances,
    } = params;

    let mut missing = Vec::new();
    for instance in instances.iter() {
        if !workflow_cache.contains_key(&instance.workflow_version_id) {
            missing.push(instance.workflow_version_id);
        }
    }
    missing.sort();
    missing.dedup();

    if !missing.is_empty() {
        let versions = registry_backend
            .get_workflow_versions(&missing)
            .await
            .map_err(RunLoopError::Backend)?;
        for version in versions {
            let program = <ir::Program as prost::Message>::decode(&version.program_proto[..])
                .map_err(|err| RunLoopError::Message(format!("invalid workflow IR: {err}")))?;
            let dag = waymark_dag_builder::convert_to_dag(&program)
                .map_err(|err| RunLoopError::Message(format!("invalid workflow DAG: {err}")))?;
            workflow_cache.insert(version.id, Arc::new(dag));
        }
    }

    for instance in instances.iter_mut() {
        let dag = workflow_cache
            .get(&instance.workflow_version_id)
            .ok_or_else(|| {
                RunLoopError::Message(format!(
                    "workflow version not found: {}",
                    instance.workflow_version_id
                ))
            })?;
        instance.dag = Some(Arc::clone(dag));
    }

    Ok(())
}
