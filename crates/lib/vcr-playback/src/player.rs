use std::collections::HashMap;

use waymark_ids::WorkflowVersionId;
use waymark_vcr_core::CorrelationId;
pub use waymark_vcr_file::LogItem;

pub type Sender = tokio::sync::mpsc::Sender<LogItem>;
pub type Receiver = tokio::sync::mpsc::Receiver<LogItem>;

pub struct Params<Backend> {
    pub execution_correlator_prep: crate::execution_correlator::PrepHandle,
    pub backend: Backend,
    pub player_rx: Receiver,
}

type RemappedWorkflowVersionIdsMap = HashMap<WorkflowVersionId, WorkflowVersionId>;

pub async fn run<Backend>(
    params: Params<Backend>,
) -> Result<(), waymark_backends_core::BackendError>
where
    Backend: waymark_core_backend::CoreBackend,
    Backend: waymark_workflow_registry_backend::WorkflowRegistryBackend,
{
    let Params {
        mut execution_correlator_prep,
        mut backend,
        mut player_rx,
    } = params;

    let mut remapped_workflow_version_ids = Default::default();

    loop {
        let Some(log_item) = player_rx.recv().await else {
            break;
        };

        match log_item {
            waymark_vcr_file::LogItem::Instance(log_item) => {
                handle_instance(
                    &mut backend,
                    &remapped_workflow_version_ids,
                    &mut execution_correlator_prep,
                    log_item,
                )
                .await?;
            }
            waymark_vcr_file::LogItem::WorkflowVersion(log_item) => {
                handle_workflow_version(&mut backend, &mut remapped_workflow_version_ids, log_item)
                    .await?;
            }
        };
    }

    Ok(())
}

async fn handle_instance<Backend>(
    backend: &mut Backend,
    remapped_workflow_version_ids: &RemappedWorkflowVersionIdsMap,
    execution_correlator_prep: &mut crate::execution_correlator::PrepHandle,
    log_item: waymark_vcr_file::instance::LogItem,
) -> Result<(), waymark_backends_core::BackendError>
where
    Backend: waymark_core_backend::CoreBackend,
{
    let waymark_vcr_file::instance::LogItem {
        workflow_version_id,
        actions,
    } = log_item;

    let Some(workflow_version_id) = remapped_workflow_version_ids.get(&workflow_version_id) else {
        tracing::warn!(
            %workflow_version_id,
            "unable to locate remapped workflow version id for the queued instance"
        );
        return Ok(());
    };

    let executor_id = waymark_ids::InstanceId::new_uuid_v4();

    let first_action_execution_id = waymark_ids::ExecutionId::new_uuid_v4();

    let queued_instance = waymark_core_backend::QueuedInstance {
        workflow_version_id: *workflow_version_id,
        schedule_id: None,
        entry_node: first_action_execution_id,
        state: waymark_runner_state::RunnerState::dummy(),
        action_results: Default::default(),
        instance_id: executor_id,
        scheduled_at: None,
    };

    backend.queue_instances(&[queued_instance]).await?;

    for log_item in actions {
        let correlation_id = CorrelationId {
            executor_id,
            execution_id: first_action_execution_id, // TODO: figure out how to predict the subsequent execution IDs.
            attempt_number: 0,                       // TODO: keep as part of log item
        };
        execution_correlator_prep.prepare_correlation(correlation_id, log_item);
    }

    Ok(())
}

async fn handle_workflow_version<Backend>(
    backend: &mut Backend,
    remapped_workflow_version_ids: &mut RemappedWorkflowVersionIdsMap,
    log_item: waymark_vcr_file::workflow_version::LogItem,
) -> Result<(), waymark_backends_core::BackendError>
where
    Backend: waymark_workflow_registry_backend::WorkflowRegistryBackend,
{
    let waymark_vcr_file::workflow_version::LogItem {
        id,
        workflow_name,
        workflow_version,
        ir_hash,
        program_proto,
        concurrent,
    } = log_item;

    let std::collections::hash_map::Entry::Vacant(vacant) = remapped_workflow_version_ids.entry(id)
    else {
        return Ok(());
    };

    let remapped_id = backend
        .upsert_workflow_version(&waymark_workflow_registry_backend::WorkflowRegistration {
            workflow_name,
            workflow_version,
            ir_hash,
            program_proto,
            concurrent,
        })
        .await?;

    vacant.insert(remapped_id);

    Ok(())
}
