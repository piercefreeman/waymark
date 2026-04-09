use std::collections::HashSet;

use tracing::debug;
use uuid::Uuid;
use waymark_core_backend::InstanceDone;
use waymark_ids::{ExecutionId, InstanceId};
use waymark_runner::{RunnerExecutor, replay_variables};
use waymark_runner_executor_core::{ExecutionException, ExecutionSuccess};
use waymark_worker_core::{ActionCompletion, ActionRequest};

use crate::error_value;

pub(super) struct Executor {
    pub executor_id: InstanceId,
    pub executor: RunnerExecutor<true>,
    pub entry_node: ExecutionId,
    pub inflight: HashSet<ExecutionId>,
    pub completed: bool,
}

impl Executor {
    pub fn new(
        executor_id: InstanceId,
        executor: RunnerExecutor<true>,
        entry_node: ExecutionId,
    ) -> Self {
        Self {
            executor_id,
            executor,
            entry_node,
            inflight: HashSet::new(),
            completed: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("increment: {0}")]
    Increment(#[source] waymark_runner::RunnerExecutorError),

    #[error("apply step: {0}")]
    ApplyStep(#[source] ApplyStepError),
}

impl Executor {
    pub fn start(&mut self) -> Result<super::Step, StartError> {
        let step = self
            .executor
            .increment(&[self.entry_node])
            .map_err(StartError::Increment)?;
        self.apply_step(step).map_err(StartError::ApplyStep)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HandleCompletionsError {
    #[error("increment: {0}")]
    Increment(#[source] waymark_runner::RunnerExecutorError),

    #[error("apply step: {0}")]
    ApplyStep(#[source] ApplyStepError),
}

impl Executor {
    pub fn handle_completions(
        &mut self,
        completions: Vec<ActionCompletion>,
    ) -> Result<Option<super::Step>, HandleCompletionsError> {
        let mut finished_nodes = Vec::new();
        for completion in completions {
            self.executor
                .set_action_result(completion.execution_id, completion.result);
            self.inflight.remove(&completion.execution_id);
            finished_nodes.push(completion.execution_id);
        }
        if finished_nodes.is_empty() {
            return Ok(None);
        }
        let step = self
            .executor
            .increment(&finished_nodes)
            .map_err(HandleCompletionsError::Increment)?;
        let step = self
            .apply_step(step)
            .map_err(HandleCompletionsError::ApplyStep)?;
        Ok(Some(step))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HandleWakeError {
    #[error("increment: {0}")]
    Increment(#[source] waymark_runner::RunnerExecutorError),

    #[error("apply step: {0}")]
    ApplyStep(#[source] ApplyStepError),
}

impl Executor {
    pub fn handle_wake(
        &mut self,
        node_ids: Vec<ExecutionId>,
    ) -> Result<Option<super::Step>, HandleWakeError> {
        let mut finished_nodes = Vec::new();
        for node_id in node_ids {
            if self.executor.state().nodes.contains_key(&node_id) {
                self.inflight.remove(&node_id);
                finished_nodes.push(node_id);
            }
        }
        if finished_nodes.is_empty() {
            return Ok(None);
        }
        let step = self
            .executor
            .increment(&finished_nodes)
            .map_err(HandleWakeError::Increment)?;
        let step = self.apply_step(step).map_err(HandleWakeError::ApplyStep)?;
        Ok(Some(step))
    }
}
#[derive(Debug, thiserror::Error)]
pub enum ApplyStepError {
    #[error("action node missing action spec")]
    NoActionSpec,

    #[error("resolve action kwargs: {0}")]
    ResolveActionKwargs(#[source] waymark_runner::RunnerExecutorError),

    #[error("action timeout seconds: {0}")]
    ActionTimeoutSeconds(#[source] waymark_runner::RunnerExecutorError),

    #[error("invalid negative action attempt for node {node_id}: {action_attempt}")]
    NegativeAction {
        node_id: ExecutionId,
        action_attempt: i32,
    },
}

impl Executor {
    fn apply_step(
        &mut self,
        step: waymark_runner::ExecutorStep,
    ) -> Result<super::Step, ApplyStepError> {
        let mut actions = Vec::new();
        let mut sleep_requests = Vec::new();
        for action in &step.actions {
            let action_spec = action.action.clone().ok_or(ApplyStepError::NoActionSpec)?;
            if self.inflight.contains(&action.node_id) {
                continue;
            }
            let kwargs = self
                .executor
                .resolve_action_kwargs(action.node_id, &action_spec)
                .map_err(ApplyStepError::ResolveActionKwargs)?;
            let timeout_seconds = self
                .executor
                .action_timeout_seconds(action.node_id)
                .map_err(ApplyStepError::ActionTimeoutSeconds)?;
            let attempt_number = u32::try_from(action.action_attempt).map_err(|_| {
                ApplyStepError::NegativeAction {
                    node_id: action.node_id,
                    action_attempt: action.action_attempt,
                }
            })?;
            actions.push(ActionRequest {
                executor_id: self.executor_id,
                execution_id: action.node_id,
                action_name: action_spec.action_name,
                module_name: action_spec.module_name.clone(),
                kwargs,
                timeout_seconds,
                attempt_number,
                dispatch_token: Uuid::new_v4(),
            });
            self.inflight.insert(action.node_id);
        }
        for sleep_request in step.sleep_requests {
            if self.inflight.contains(&sleep_request.node_id) {
                continue;
            }
            self.inflight.insert(sleep_request.node_id);
            sleep_requests.push(sleep_request);
        }

        debug!(
            executor_id = %self.executor_id,
            actions = actions.len(),
            sleep_requests = sleep_requests.len(),
            inflight = self.inflight.len(),
            "executor step"
        );

        let instance_done = if !self.completed && actions.is_empty() && self.inflight.is_empty() {
            self.completed = true;
            Some(build_instance_done(
                self.executor_id,
                self.entry_node,
                &self.executor,
            ))
        } else {
            None
        };

        Ok(super::Step {
            executor_id: self.executor_id,
            actions,
            sleep_requests,
            updates: step.updates,
            instance_done,
        })
    }
}

fn build_instance_done<const SHOULD_COLLECT_UPDATES: bool>(
    executor_id: InstanceId,
    entry_node: ExecutionId,
    executor: &RunnerExecutor<SHOULD_COLLECT_UPDATES>,
) -> InstanceDone {
    if let Some(error_payload) = executor.terminal_error().cloned() {
        return InstanceDone {
            executor_id,
            entry_node,
            result: None,
            error: Some(error_payload),
        };
    }
    let (result_payload, error_payload) = compute_instance_payload(executor);
    InstanceDone {
        executor_id,
        entry_node,
        result: result_payload,
        error: error_payload,
    }
}

fn compute_instance_payload<const SHOULD_COLLECT_UPDATES: bool>(
    executor: &RunnerExecutor<SHOULD_COLLECT_UPDATES>,
) -> (Option<ExecutionSuccess>, Option<ExecutionException>) {
    let outputs = output_vars(executor.dag());
    match replay_variables(executor.state(), executor.action_results()) {
        Ok(replayed) => {
            if outputs.is_empty() {
                let mut map = serde_json::Map::new();
                for (key, value) in replayed.variables {
                    map.insert(key, value);
                }
                return (Some(ExecutionSuccess(serde_json::Value::Object(map))), None);
            }
            let mut map = serde_json::Map::new();
            for name in outputs {
                let value = replayed
                    .variables
                    .get(&name)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                map.insert(name, value);
            }
            (Some(ExecutionSuccess(serde_json::Value::Object(map))), None)
        }
        Err(err) => {
            let error_value = error_value("ReplayError", &err.to_string());
            (None, Some(ExecutionException(error_value)))
        }
    }
}

fn output_vars(dag: &waymark_dag::DAG) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for node in dag.nodes.values() {
        match node {
            waymark_dag::DAGNode::Output(waymark_dag::OutputNode { io_vars, .. }) => {
                for name in io_vars {
                    if seen.insert(name.clone()) {
                        names.push(name.clone());
                    }
                }
            }
            waymark_dag::DAGNode::Return(waymark_dag::ReturnNode {
                targets, target, ..
            }) => {
                if let Some(targets) = targets {
                    for name in targets {
                        if seen.insert(name.clone()) {
                            names.push(name.clone());
                        }
                    }
                } else if let Some(target) = target
                    && seen.insert(target.clone())
                {
                    names.push(target.clone());
                }
            }
            _ => {}
        }
    }
    names
}
