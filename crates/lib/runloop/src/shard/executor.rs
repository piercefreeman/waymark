use std::collections::HashSet;

use tracing::debug;
use uuid::Uuid;
use waymark_core_backend::InstanceDone;
use waymark_runner::{RunnerExecutor, replay_variables};
use waymark_worker_core::{ActionCompletion, ActionRequest};

use crate::{RunLoopError, error_value};

pub struct Executor {
    pub executor_id: Uuid,
    pub executor: RunnerExecutor,
    pub entry_node: Uuid,
    pub inflight: HashSet<Uuid>,
    pub completed: bool,
}

impl Executor {
    pub fn new(executor_id: Uuid, executor: RunnerExecutor, entry_node: Uuid) -> Self {
        Self {
            executor_id,
            executor,
            entry_node,
            inflight: HashSet::new(),
            completed: false,
        }
    }

    pub fn start(&mut self) -> Result<super::Step, RunLoopError> {
        let step = self.executor.increment(&[self.entry_node])?;
        self.apply_step(step)
    }

    pub fn handle_completions(
        &mut self,
        completions: Vec<ActionCompletion>,
    ) -> Result<Option<super::Step>, RunLoopError> {
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
        let step = self.executor.increment(&finished_nodes)?;
        Ok(Some(self.apply_step(step)?))
    }

    pub fn handle_wake(
        &mut self,
        node_ids: Vec<Uuid>,
    ) -> Result<Option<super::Step>, RunLoopError> {
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
        let step = self.executor.increment(&finished_nodes)?;
        Ok(Some(self.apply_step(step)?))
    }

    fn apply_step(
        &mut self,
        step: waymark_runner::ExecutorStep,
    ) -> Result<super::Step, RunLoopError> {
        let mut actions = Vec::new();
        let mut sleep_requests = Vec::new();
        for action in &step.actions {
            let action_spec = action.action.clone().ok_or_else(|| {
                RunLoopError::Message("action node missing action spec".to_string())
            })?;
            if self.inflight.contains(&action.node_id) {
                continue;
            }
            let kwargs = self
                .executor
                .resolve_action_kwargs(action.node_id, &action_spec)?;
            let timeout_seconds = self.executor.action_timeout_seconds(action.node_id)?;
            let attempt_number = u32::try_from(action.action_attempt).map_err(|_| {
                RunLoopError::Message(format!(
                    "invalid negative action attempt for node {}",
                    action.node_id
                ))
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

fn build_instance_done(
    executor_id: Uuid,
    entry_node: Uuid,
    executor: &RunnerExecutor,
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

fn compute_instance_payload(
    executor: &RunnerExecutor,
) -> (Option<serde_json::Value>, Option<serde_json::Value>) {
    let outputs = output_vars(executor.dag());
    match replay_variables(executor.state(), executor.action_results()) {
        Ok(replayed) => {
            if outputs.is_empty() {
                let mut map = serde_json::Map::new();
                for (key, value) in replayed.variables {
                    map.insert(key, value);
                }
                return (Some(serde_json::Value::Object(map)), None);
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
            (Some(serde_json::Value::Object(map)), None)
        }
        Err(err) => {
            let error_value = error_value("ReplayError", &err.to_string());
            (None, Some(error_value))
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
