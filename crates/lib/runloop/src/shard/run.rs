use std::{collections::HashMap, sync::mpsc as std_mpsc};

use tokio::sync::mpsc;
use tracing::{debug, warn};
use waymark_ids::{ExecutionId, InstanceId};
use waymark_worker_core::ActionCompletion;

use crate::shard;

#[derive(Debug, thiserror::Error)]
pub enum AssignInstancesError {
    #[error("queued instance missing runner state")]
    QueuedInstanceMissingRunnerState,

    #[error("queued instance missing workflow DAG")]
    QueuedInstanceMissingWorkflowDag,

    #[error("start: {0}")]
    Start(#[source] super::executor::StartError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("assign instances: {0}")]
    AssignInstances(AssignInstancesError),

    #[error("action completions: {0}")]
    ActionCompletions(super::executor::HandleCompletionsError),

    #[error("wake: {0}")]
    Wake(super::executor::HandleWakeError),
}

pub fn run_executor_shard(
    shard_id: usize,
    receiver: std_mpsc::Receiver<shard::Command>,
    sender: mpsc::UnboundedSender<shard::Event>,
) {
    let mut executors: HashMap<InstanceId, shard::Executor> = HashMap::new();

    let send_instance_failed =
        |executor_id: InstanceId,
         entry_node: ExecutionId,
         err: Error,
         sender: &mpsc::UnboundedSender<shard::Event>| {
            let _ = sender.send(shard::Event::InstanceFailed {
                executor_id,
                entry_node,
                error: err.to_string(),
            });
        };

    while let Ok(command) = receiver.recv() {
        match command {
            shard::Command::AssignInstances(instances) => {
                debug!(
                    shard_id,
                    count = instances.len(),
                    "assigning instances to shard"
                );
                for instance in instances {
                    // If the same instance id was reclaimed from the DB, we treat
                    // the prior in-memory executor as stale (e.g. stalled) and
                    // replace it with the freshly claimed state.
                    if executors.remove(&instance.instance_id).is_some() {
                        warn!(
                            shard_id,
                            instance_id = %instance.instance_id,
                            "replacing active executor state for reclaimed instance"
                        );
                    }

                    let Some(state) = instance.state else {
                        send_instance_failed(
                            instance.instance_id,
                            instance.entry_node,
                            Error::AssignInstances(
                                AssignInstancesError::QueuedInstanceMissingRunnerState,
                            ),
                            &sender,
                        );
                        continue;
                    };

                    let Some(dag) = instance.dag else {
                        send_instance_failed(
                            instance.instance_id,
                            instance.entry_node,
                            Error::AssignInstances(
                                AssignInstancesError::QueuedInstanceMissingWorkflowDag,
                            ),
                            &sender,
                        );
                        continue;
                    };

                    let mut executor =
                        waymark_runner::RunnerExecutor::new(dag, state, instance.action_results);
                    executor.set_instance_id(instance.instance_id);

                    let mut owner =
                        shard::Executor::new(instance.instance_id, executor, instance.entry_node);
                    let step = match owner.start() {
                        Ok(step) => step,
                        Err(err) => {
                            send_instance_failed(
                                instance.instance_id,
                                instance.entry_node,
                                Error::AssignInstances(AssignInstancesError::Start(err)),
                                &sender,
                            );
                            continue;
                        }
                    };
                    let done = step.instance_done.is_some();
                    if sender.send(shard::Event::Step(step)).is_err() {
                        return;
                    }
                    if !done {
                        executors.insert(instance.instance_id, owner);
                    }
                }
            }
            shard::Command::ActionCompletions(completions) => {
                let mut grouped: HashMap<InstanceId, Vec<ActionCompletion>> = HashMap::new();
                for completion in completions {
                    grouped
                        .entry(completion.executor_id)
                        .or_default()
                        .push(completion);
                }
                for (executor_id, batch) in grouped {
                    let Some(owner) = executors.get_mut(&executor_id) else {
                        warn!(
                            shard_id,
                            executor_id = %executor_id,
                            "completion for unknown executor"
                        );
                        continue;
                    };
                    let step = match owner.handle_completions(batch) {
                        Ok(Some(step)) => step,
                        Ok(None) => continue,
                        Err(err) => {
                            let entry_node = owner.entry_node;
                            executors.remove(&executor_id);
                            send_instance_failed(
                                executor_id,
                                entry_node,
                                Error::ActionCompletions(err),
                                &sender,
                            );
                            continue;
                        }
                    };
                    let done = step.instance_done.is_some();
                    if sender.send(shard::Event::Step(step)).is_err() {
                        return;
                    }
                    if done {
                        executors.remove(&executor_id);
                    }
                }
            }
            shard::Command::Wake(node_ids) => {
                let mut grouped: HashMap<InstanceId, Vec<ExecutionId>> = HashMap::new();
                for node_id in node_ids {
                    for (executor_id, owner) in &executors {
                        if owner.executor.state().nodes.contains_key(&node_id) {
                            grouped.entry(*executor_id).or_default().push(node_id);
                            break;
                        }
                    }
                }
                for (executor_id, batch) in grouped {
                    let Some(owner) = executors.get_mut(&executor_id) else {
                        continue;
                    };
                    let step = match owner.handle_wake(batch) {
                        Ok(Some(step)) => step,
                        Ok(None) => continue,
                        Err(err) => {
                            let entry_node = owner.entry_node;
                            executors.remove(&executor_id);
                            send_instance_failed(
                                executor_id,
                                entry_node,
                                Error::Wake(err),
                                &sender,
                            );
                            continue;
                        }
                    };
                    let done = step.instance_done.is_some();
                    if sender.send(shard::Event::Step(step)).is_err() {
                        return;
                    }
                    if done {
                        executors.remove(&executor_id);
                    }
                }
            }
            shard::Command::Evict(instance_ids) => {
                for instance_id in instance_ids {
                    executors.remove(&instance_id);
                }
            }
            shard::Command::Shutdown => {
                break;
            }
        }
    }
}
