//! Runloop for coordinating executors and worker pools.

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Notify, mpsc};
use uuid::Uuid;

use crate::rappel_core::backends::{
    ActionDone, BackendError, BaseBackend, GraphUpdate, InstanceDone, QueuedInstance,
};
use crate::rappel_core::dag::{DAG, DAGNode, OutputNode, ReturnNode};
use crate::rappel_core::runner::{
    ExecutorStep, RunnerExecutor, RunnerExecutorError, replay_variables,
};
use crate::rappel_core::workers::{
    ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError,
};

#[derive(Debug, thiserror::Error)]
pub enum RunLoopError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Backend(#[from] BackendError),
    #[error(transparent)]
    WorkerPool(#[from] WorkerPoolError),
    #[error(transparent)]
    RunnerExecutor(#[from] RunnerExecutorError),
}

#[derive(Clone, Debug, Default)]
pub struct RunLoopResult {
    pub completed_actions: HashMap<Uuid, Vec<crate::rappel_core::runner::ExecutionNode>>,
}

enum InstanceMessage {
    Batch(Vec<QueuedInstance>),
    Error(BackendError),
}

pub struct RunLoop {
    worker_pool: Arc<dyn BaseWorkerPool>,
    backend: Arc<dyn BaseBackend>,
    instance_batch_size: usize,
    instance_done_batch_size: usize,
    poll_interval: Duration,
    persistence_interval: Duration,
    executors: HashMap<Uuid, RunnerExecutor>,
    entry_nodes: HashMap<Uuid, Uuid>,
    inflight: HashSet<(Uuid, Uuid)>,
    completed_executors: HashSet<Uuid>,
    instances_done_pending: Vec<InstanceDone>,
    instances_idle: bool,
}

impl RunLoop {
    pub fn new(
        worker_pool: impl BaseWorkerPool + 'static,
        backend: impl BaseBackend + 'static,
        instance_batch_size: usize,
        instance_done_batch_size: Option<usize>,
        poll_interval: f64,
        persistence_interval: f64,
    ) -> Self {
        Self {
            worker_pool: Arc::new(worker_pool),
            backend: Arc::new(backend),
            instance_batch_size: std::cmp::max(1, instance_batch_size),
            instance_done_batch_size: std::cmp::max(
                1,
                instance_done_batch_size.unwrap_or(instance_batch_size),
            ),
            poll_interval: Duration::from_secs_f64(poll_interval.max(0.0)),
            persistence_interval: Duration::from_secs_f64(persistence_interval.max(0.0)),
            executors: HashMap::new(),
            entry_nodes: HashMap::new(),
            inflight: HashSet::new(),
            completed_executors: HashSet::new(),
            instances_done_pending: Vec::new(),
            instances_idle: false,
        }
    }

    pub fn register_executor(&mut self, mut executor: RunnerExecutor, entry_node: Uuid) -> Uuid {
        let executor_id = Uuid::new_v4();
        executor.set_instance_id(executor_id);
        self.executors.insert(executor_id, executor);
        self.entry_nodes.insert(executor_id, entry_node);
        executor_id
    }

    pub async fn run(&mut self) -> Result<RunLoopResult, RunLoopError> {
        let mut result = RunLoopResult {
            completed_actions: self.executors.keys().map(|id| (*id, Vec::new())).collect(),
        };

        let mut initial_steps: Vec<(Uuid, ExecutorStep)> = Vec::new();
        for (executor_id, executor) in self.executors.iter_mut() {
            let entry_node = self.entry_nodes.get(executor_id).copied().ok_or_else(|| {
                RunLoopError::Message(format!("missing entry node for executor {executor_id}"))
            })?;
            let step = executor.increment(entry_node)?;
            initial_steps.push((*executor_id, step));
        }
        self.persist_steps(&initial_steps).await?;
        for (executor_id, step) in initial_steps {
            self.handle_step(executor_id, step).await?;
        }

        let (completion_tx, mut completion_rx) = mpsc::channel::<Vec<ActionCompletion>>(32);
        let (instance_tx, mut instance_rx) = mpsc::channel::<InstanceMessage>(16);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_notify = Arc::new(Notify::new());

        let worker_pool = self.worker_pool.clone();
        let completion_stop = stop.clone();
        let completion_notify = stop_notify.clone();
        let completion_handle = tokio::spawn(async move {
            loop {
                if completion_stop.load(Ordering::SeqCst) {
                    break;
                }
                let completions = worker_pool.get_complete().await;
                if completions.is_empty() {
                    continue;
                }
                if completion_tx.send(completions).await.is_err() {
                    break;
                }
            }
            completion_notify.notify_waiters();
        });

        let backend = self.backend.clone();
        let poll_interval = self.poll_interval;
        let instance_batch_size = self.instance_batch_size;
        let instance_stop = stop.clone();
        let instance_notify = stop_notify.clone();
        let instance_handle = tokio::spawn(async move {
            loop {
                if instance_stop.load(Ordering::SeqCst) {
                    break;
                }
                let batch = backend.get_queued_instances(instance_batch_size).await;
                let message = match batch {
                    Ok(instances) => InstanceMessage::Batch(instances),
                    Err(err) => InstanceMessage::Error(err),
                };
                if instance_tx.send(message).await.is_err() {
                    break;
                }
                if poll_interval > Duration::ZERO {
                    tokio::time::sleep(poll_interval).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(0)).await;
                }
            }
            instance_notify.notify_waiters();
        });

        let persistence_interval = if self.persistence_interval > Duration::ZERO {
            self.persistence_interval
        } else {
            Duration::from_secs(3600)
        };
        let mut persistence_tick = tokio::time::interval(persistence_interval);

        loop {
            if self.should_stop() {
                break;
            }
            tokio::select! {
                Some(completions) = completion_rx.recv() => {
                    self.process_completions(completions, &mut result).await?;
                }
                Some(message) = instance_rx.recv() => {
                    match message {
                        InstanceMessage::Batch(instances) => {
                            self.process_instances(instances, &mut result).await?;
                        }
                        InstanceMessage::Error(err) => {
                            return Err(RunLoopError::Backend(err));
                        }
                    }
                }
                _ = persistence_tick.tick() => {
                    if self.persistence_interval > Duration::ZERO {
                        self.flush_instances_done().await?;
                    }
                }
                else => break,
            }
        }

        stop.store(true, Ordering::SeqCst);
        stop_notify.notify_waiters();
        let _ = completion_handle.await;
        let _ = instance_handle.await;
        self.flush_instances_done().await?;
        Ok(result)
    }

    async fn process_completions(
        &mut self,
        completions: Vec<ActionCompletion>,
        result: &mut RunLoopResult,
    ) -> Result<(), RunLoopError> {
        if completions.is_empty() {
            return Ok(());
        }
        let mut grouped: HashMap<Uuid, Vec<ActionCompletion>> = HashMap::new();
        for completion in completions {
            grouped
                .entry(completion.executor_id)
                .or_default()
                .push(completion);
        }

        let mut steps: Vec<(Uuid, ExecutorStep)> = Vec::new();
        for (executor_id, batch) in grouped {
            let executor = self
                .executors
                .get_mut(&executor_id)
                .ok_or_else(|| RunLoopError::Message(format!("unknown executor: {executor_id}")))?;
            let mut finished_nodes = Vec::new();
            for completion in batch {
                let node = executor
                    .state()
                    .nodes
                    .get(&completion.node_id)
                    .ok_or_else(|| {
                        RunLoopError::Message(format!(
                            "unknown execution node: {}",
                            completion.node_id
                        ))
                    })?
                    .clone();
                executor.set_action_result(completion.node_id, completion.result);
                result
                    .completed_actions
                    .entry(executor_id)
                    .or_default()
                    .push(node);
                self.inflight.remove(&(executor_id, completion.node_id));
                finished_nodes.push(completion.node_id);
            }
            if !finished_nodes.is_empty() {
                let step = executor.increment_batch(&finished_nodes)?;
                steps.push((executor_id, step));
            }
        }

        self.persist_steps(&steps).await?;
        for (executor_id, step) in steps {
            self.handle_step(executor_id, step).await?;
        }
        Ok(())
    }

    async fn process_instances(
        &mut self,
        instances: Vec<QueuedInstance>,
        result: &mut RunLoopResult,
    ) -> Result<(), RunLoopError> {
        if instances.is_empty() {
            self.instances_idle = true;
            return Ok(());
        }
        self.instances_idle = false;
        let mut steps: Vec<(Uuid, ExecutorStep)> = Vec::new();
        for instance in instances {
            let executor_id = self.register_instance(instance, result);
            let executor = self
                .executors
                .get_mut(&executor_id)
                .ok_or_else(|| RunLoopError::Message(format!("missing executor {executor_id}")))?;
            let entry_node = self.entry_nodes.get(&executor_id).copied().ok_or_else(|| {
                RunLoopError::Message(format!("missing entry node for executor {executor_id}"))
            })?;
            let step = executor.increment(entry_node)?;
            steps.push((executor_id, step));
        }
        self.persist_steps(&steps).await?;
        for (executor_id, step) in steps {
            self.handle_step(executor_id, step).await?;
        }
        Ok(())
    }

    fn register_instance(&mut self, instance: QueuedInstance, result: &mut RunLoopResult) -> Uuid {
        let executor = if let Some(state) = instance.state {
            RunnerExecutor::new(
                instance.dag.clone(),
                Some(state),
                None,
                None,
                instance.action_results.clone(),
                Some(self.backend.clone()),
            )
        } else {
            RunnerExecutor::new(
                instance.dag.clone(),
                None,
                instance.nodes.clone(),
                instance.edges.clone(),
                instance.action_results.clone(),
                Some(self.backend.clone()),
            )
        };
        let executor_id = instance.instance_id;
        let mut executor = executor;
        executor.set_instance_id(executor_id);
        self.executors.insert(executor_id, executor);
        self.entry_nodes.insert(executor_id, instance.entry_node);
        result.completed_actions.entry(executor_id).or_default();
        executor_id
    }

    fn should_stop(&self) -> bool {
        self.inflight.is_empty() && self.instances_idle
    }

    async fn handle_step(
        &mut self,
        executor_id: Uuid,
        step: ExecutorStep,
    ) -> Result<(), RunLoopError> {
        {
            let (executors, inflight, worker_pool) =
                (&mut self.executors, &mut self.inflight, &self.worker_pool);
            let executor = executors
                .get_mut(&executor_id)
                .ok_or_else(|| RunLoopError::Message(format!("unknown executor: {executor_id}")))?;
            for action in step.actions {
                let action_spec = action.action.clone().ok_or_else(|| {
                    RunLoopError::Message("action node missing action spec".to_string())
                })?;
                let key = (executor_id, action.node_id);
                if inflight.contains(&key) {
                    continue;
                }
                executor.clear_action_result(action.node_id);
                executor
                    .state_mut()
                    .mark_running(action.node_id)
                    .map_err(|err| RunLoopError::Message(err.0))?;
                let kwargs = executor.resolve_action_kwargs(&action_spec)?;
                let request = ActionRequest {
                    executor_id,
                    node_id: action.node_id,
                    action_name: action_spec.action_name,
                    kwargs,
                };
                worker_pool.queue(request)?;
                inflight.insert(key);
            }
        }
        self.finalize_executor_if_done(executor_id)?;
        self.flush_instances_done_if_needed().await?;
        Ok(())
    }

    fn finalize_executor_if_done(&mut self, executor_id: Uuid) -> Result<(), RunLoopError> {
        if self.completed_executors.contains(&executor_id) {
            return Ok(());
        }
        if self
            .inflight
            .iter()
            .any(|(exec_id, _)| exec_id == &executor_id)
        {
            return Ok(());
        }
        let executor = self
            .executors
            .get(&executor_id)
            .ok_or_else(|| RunLoopError::Message(format!("missing executor {executor_id}")))?;
        let (result_payload, error_payload) = self.compute_instance_payload(executor);
        self.queue_instance_done(InstanceDone {
            executor_id,
            entry_node: *self.entry_nodes.get(&executor_id).unwrap_or(&executor_id),
            result: result_payload,
            error: error_payload,
        });
        self.completed_executors.insert(executor_id);
        Ok(())
    }

    fn queue_instance_done(&mut self, instance_done: InstanceDone) {
        self.instances_done_pending.push(instance_done);
    }

    async fn flush_instances_done_if_needed(&mut self) -> Result<(), RunLoopError> {
        if self.instances_done_pending.len() >= self.instance_done_batch_size {
            self.flush_instances_done().await?;
        }
        Ok(())
    }

    async fn flush_instances_done(&mut self) -> Result<(), RunLoopError> {
        if self.instances_done_pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.instances_done_pending);
        let backend = self.backend.batching();
        backend.save_instances_done(&pending).await?;
        Ok(())
    }

    fn compute_instance_payload(
        &self,
        executor: &RunnerExecutor,
    ) -> (Option<Value>, Option<Value>) {
        let outputs = output_vars(executor.dag());
        match replay_variables(executor.state(), executor.action_results()) {
            Ok(replayed) => {
                if outputs.is_empty() {
                    let mut map = serde_json::Map::new();
                    for (key, value) in replayed.variables {
                        map.insert(key, value);
                    }
                    return (Some(Value::Object(map)), None);
                }
                let mut map = serde_json::Map::new();
                for name in outputs {
                    let value = replayed
                        .variables
                        .get(&name)
                        .cloned()
                        .unwrap_or(Value::Null);
                    map.insert(name, value);
                }
                (Some(Value::Object(map)), None)
            }
            Err(err) => {
                let error_value = error_value("ReplayError", &err.to_string());
                (None, Some(error_value))
            }
        }
    }

    async fn persist_steps(&self, steps: &[(Uuid, ExecutorStep)]) -> Result<(), RunLoopError> {
        let mut actions_done: Vec<ActionDone> = Vec::new();
        let mut graph_updates: Vec<GraphUpdate> = Vec::new();
        for (_, step) in steps {
            if let Some(updates) = &step.updates {
                if !updates.actions_done.is_empty() {
                    actions_done.extend(updates.actions_done.clone());
                }
                if !updates.graph_updates.is_empty() {
                    graph_updates.extend(updates.graph_updates.clone());
                }
            }
        }
        if actions_done.is_empty() && graph_updates.is_empty() {
            return Ok(());
        }
        let backend = self.backend.batching();
        if !actions_done.is_empty() {
            backend.save_actions_done(&actions_done).await?;
        }
        if !graph_updates.is_empty() {
            backend.save_graphs(&graph_updates).await?;
        }
        Ok(())
    }
}

fn output_vars(dag: &DAG) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for node in dag.nodes.values() {
        match node {
            DAGNode::Output(OutputNode { io_vars, .. }) => {
                for name in io_vars {
                    if seen.insert(name.clone()) {
                        names.push(name.clone());
                    }
                }
            }
            DAGNode::Return(ReturnNode {
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

fn error_value(kind: &str, message: &str) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("type".to_string(), Value::String(kind.to_string()));
    map.insert("message".to_string(), Value::String(message.to_string()));
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use crate::messages::ast as ir;
    use crate::rappel_core::backends::MemoryBackend;
    use crate::rappel_core::dag::{
        ActionCallNode, ActionCallParams, DAGEdge, DAGNode, InputNode, OutputNode,
    };
    use crate::rappel_core::runner::RunnerState;
    use crate::rappel_core::workers::ActionCallable;

    #[tokio::test]
    async fn test_runloop_executes_actions() {
        let mut dag = DAG::default();
        let input_node = InputNode::new("input", vec!["x".to_string()], Some("main".to_string()));
        let action_node = ActionCallNode::new(
            "action",
            "double",
            ActionCallParams {
                module_name: None,
                kwargs: HashMap::new(),
                kwarg_exprs: HashMap::from([(
                    "value".to_string(),
                    ir::Expr {
                        kind: Some(ir::expr::Kind::Variable(ir::Variable {
                            name: "x".to_string(),
                        })),
                        span: None,
                    },
                )]),
                policies: Vec::new(),
                targets: Some(vec!["y".to_string()]),
                target: None,
                parallel_index: None,
                aggregates_to: None,
                spread_loop_var: None,
                spread_collection_expr: None,
                function_name: Some("main".to_string()),
            },
        );
        let output_node =
            OutputNode::new("output", vec!["y".to_string()], Some("main".to_string()));

        dag.add_node(DAGNode::Input(input_node.clone()));
        dag.add_node(DAGNode::ActionCall(action_node.clone()));
        dag.add_node(DAGNode::Output(output_node.clone()));
        dag.add_edge(DAGEdge::state_machine(
            input_node.id.clone(),
            action_node.id.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action_node.id.clone(),
            output_node.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let _ = state
            .record_assignment(
                vec!["x".to_string()],
                &ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(4)),
                    })),
                    span: None,
                },
                None,
                Some("input x = 4".to_string()),
            )
            .expect("record assignment");
        let entry_exec = state
            .queue_template_node(&input_node.id, None)
            .expect("queue entry node");

        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let backend = MemoryBackend::with_queue(queue.clone());

        let mut actions: HashMap<String, ActionCallable> = HashMap::new();
        actions.insert(
            "double".to_string(),
            Arc::new(|kwargs| {
                Box::pin(async move {
                    let value = kwargs
                        .get("value")
                        .and_then(|value| value.as_i64())
                        .unwrap_or(0);
                    Ok(Value::Number((value * 2).into()))
                })
            }),
        );
        let worker_pool = crate::rappel_core::workers::InlineWorkerPool::new(actions);

        let mut runloop = RunLoop::new(worker_pool, backend, 25, None, 0.0, 0.1);
        queue.lock().expect("queue lock").push_back(QueuedInstance {
            dag: dag.clone(),
            entry_node: entry_exec.node_id,
            state: Some(state),
            nodes: None,
            edges: None,
            action_results: Some(HashMap::new()),
            instance_id: Uuid::new_v4(),
        });

        let result = runloop.run().await.expect("runloop");
        let executor_id = *result.completed_actions.keys().next().expect("executor id");
        let executor = runloop.executors.get(&executor_id).expect("executor");
        let results: Vec<_> = executor.action_results().values().cloned().collect();
        assert_eq!(results, vec![Value::Number(8.into())]);
    }
}
