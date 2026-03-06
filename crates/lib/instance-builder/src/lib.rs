use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;
use waymark_core_backend::QueuedInstance;
use waymark_dag::DAG;
use waymark_ir_conversions::literal_from_json_value;
use waymark_runner_state::{RunnerState, RunnerStateError};

#[derive(Debug, Error)]
pub enum InstanceBuilderError {
    #[error("{0}")]
    RunnerState(String),
    #[error("DAG entry node not found")]
    MissingEntryNode,
}

impl From<RunnerStateError> for InstanceBuilderError {
    fn from(value: RunnerStateError) -> Self {
        Self::RunnerState(value.0)
    }
}

pub fn seed_inputs_from_json_iter<'a, I>(
    state: &mut RunnerState,
    inputs: I,
) -> Result<(), InstanceBuilderError>
where
    I: IntoIterator<Item = (&'a String, &'a Value)>,
{
    for (name, value) in inputs {
        let expr = literal_from_json_value(value);
        let label = format!("input {name} = {value}");
        state.record_assignment(vec![name.clone()], &expr, None, Some(label))?;
    }
    Ok(())
}

pub fn build_queued_instance_with_seed<F>(
    dag: Arc<DAG>,
    workflow_version_id: Uuid,
    instance_id: Uuid,
    schedule_id: Option<Uuid>,
    scheduled_at: Option<DateTime<Utc>>,
    seed_fn: F,
) -> Result<QueuedInstance, InstanceBuilderError>
where
    F: FnOnce(&mut RunnerState) -> Result<(), InstanceBuilderError>,
{
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
    seed_fn(&mut state)?;

    let entry_node = dag
        .entry_node
        .as_ref()
        .ok_or(InstanceBuilderError::MissingEntryNode)?;

    let entry_exec = state.queue_template_node(entry_node, None)?;

    Ok(QueuedInstance {
        workflow_version_id,
        schedule_id,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: std::collections::HashMap::new(),
        instance_id,
        scheduled_at,
    })
}
