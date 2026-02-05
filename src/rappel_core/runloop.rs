//! Runloop for coordinating executors and worker pools.

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Notify, mpsc, watch};
use tracing::error;
use uuid::Uuid;

use crate::backends::{
    ActionDone, BackendError, CoreBackend, GraphUpdate, InstanceDone, QueuedInstance,
};
use crate::observability::obs;
use crate::rappel_core::dag::{DAG, DAGNode, OutputNode, ReturnNode};
use crate::rappel_core::runner::{
    ExecutorStep, RunnerExecutor, RunnerExecutorError, replay_variables,
};
use crate::workers::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

/// Raised when the run loop cannot coordinate execution.
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

/// Aggregated action completions from the run loop.
#[derive(Clone, Debug, Default)]
pub struct RunLoopResult {
    pub completed_actions: HashMap<Uuid, Vec<crate::rappel_core::runner::ExecutionNode>>,
}

enum InstanceMessage {
    Batch(Vec<QueuedInstance>),
    Error(BackendError),
}

enum LoopEvent {
    Completions(Vec<ActionCompletion>),
    Instance(InstanceMessage),
}

/// Coordinate RunnerExecutors with a shared worker pool.
///
/// RunLoop manages multiple executors concurrently. Each executor advances its
/// DAG template until it hits action calls, which are queued on a worker pool.
/// The run loop then polls for completions, delegates results back to the
/// owning executor, and continues until no actions remain in flight.
pub struct RunLoop {
    worker_pool: Arc<dyn BaseWorkerPool>,
    backend: Arc<dyn CoreBackend>,
    max_concurrent_instances: usize,
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
        backend: impl CoreBackend + 'static,
        max_concurrent_instances: usize,
        instance_done_batch_size: Option<usize>,
        poll_interval: f64,
        persistence_interval: f64,
    ) -> Self {
        let max_concurrent_instances = std::cmp::max(1, max_concurrent_instances);
        Self {
            worker_pool: Arc::new(worker_pool),
            backend: Arc::new(backend),
            max_concurrent_instances,
            instance_done_batch_size: std::cmp::max(
                1,
                instance_done_batch_size.unwrap_or(max_concurrent_instances),
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

    /// Register an executor and its entry node, returning an executor id.
    pub fn register_executor(&mut self, mut executor: RunnerExecutor, entry_node: Uuid) -> Uuid {
        let executor_id = Uuid::new_v4();
        executor.set_instance_id(executor_id);
        self.executors.insert(executor_id, executor);
        self.entry_nodes.insert(executor_id, entry_node);
        executor_id
    }

    /// Run all registered executors until no actions remain in flight.
    #[obs]
    pub async fn run(&mut self) -> Result<RunLoopResult, RunLoopError> {
        let mut result = RunLoopResult {
            completed_actions: self.executors.keys().map(|id| (*id, Vec::new())).collect(),
        };

        self.worker_pool.launch().await?;

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

        let available_instance_slots = Arc::new(AtomicUsize::new(self.available_instance_slots()));

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
                let completions = tokio::select! {
                    _ = completion_notify.notified() => {
                        break;
                    }
                    completions = worker_pool.get_complete() => completions,
                };
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
        let max_concurrent_instances = self.max_concurrent_instances;
        let instance_available_slots = Arc::clone(&available_instance_slots);
        let instance_stop = stop.clone();
        let instance_notify = stop_notify.clone();
        let instance_handle = tokio::spawn(async move {
            loop {
                if instance_stop.load(Ordering::SeqCst) {
                    break;
                }
                let available_slots = instance_available_slots.load(Ordering::SeqCst);
                let batch_size = std::cmp::min(available_slots, max_concurrent_instances);
                if batch_size == 0 {
                    if poll_interval > Duration::ZERO {
                        tokio::time::sleep(poll_interval).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(0)).await;
                    }
                    continue;
                }
                let batch = backend.get_queued_instances(batch_size).await;
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

            // Wait for at least one event
            let first_event = tokio::select! {
                Some(completions) = completion_rx.recv() => {
                    Some(LoopEvent::Completions(completions))
                }
                Some(message) = instance_rx.recv() => {
                    Some(LoopEvent::Instance(message))
                }
                _ = persistence_tick.tick() => {
                    if self.persistence_interval > Duration::ZERO {
                        self.flush_instances_done().await?;
                    }
                    None
                }
                else => break,
            };

            // If we got a persistence tick, continue to next iteration
            let first_event = match first_event {
                Some(event) => event,
                None => continue,
            };

            // Drain ALL available completions and instances (non-blocking)
            let mut all_completions: Vec<ActionCompletion> = Vec::new();
            let mut all_instances: Vec<QueuedInstance> = Vec::new();

            // Process first event
            match first_event {
                LoopEvent::Completions(completions) => {
                    all_completions.extend(completions);
                }
                LoopEvent::Instance(InstanceMessage::Batch(instances)) => {
                    if instances.is_empty() {
                        self.instances_idle = true;
                    } else {
                        self.instances_idle = false;
                        all_instances.extend(instances);
                    }
                }
                LoopEvent::Instance(InstanceMessage::Error(err)) => {
                    return Err(RunLoopError::Backend(err));
                }
            }

            // Drain remaining completions (non-blocking)
            while let Ok(completions) = completion_rx.try_recv() {
                all_completions.extend(completions);
            }

            // Drain remaining instances (non-blocking)
            while let Ok(message) = instance_rx.try_recv() {
                match message {
                    InstanceMessage::Batch(instances) => {
                        if instances.is_empty() {
                            self.instances_idle = true;
                        } else {
                            self.instances_idle = false;
                            all_instances.extend(instances);
                        }
                    }
                    InstanceMessage::Error(err) => {
                        return Err(RunLoopError::Backend(err));
                    }
                }
            }

            // Process everything in one batch:
            // 1. Compute all steps
            // 2. Single persist
            // 3. Then queue new actions
            self.process_batch(all_completions, all_instances, &mut result)
                .await?;
            self.store_available_instance_slots(&available_instance_slots);
        }

        stop.store(true, Ordering::SeqCst);
        stop_notify.notify_waiters();
        let _ = completion_handle.await;
        let _ = instance_handle.await;
        self.flush_instances_done().await?;
        Ok(result)
    }

    /// Process a batch of completions and new instances together.
    /// This ensures we make a single DB round-trip for all pending work,
    /// while maintaining durability: persist THEN queue new actions.
    #[obs]
    async fn process_batch(
        &mut self,
        completions: Vec<ActionCompletion>,
        instances: Vec<QueuedInstance>,
        result: &mut RunLoopResult,
    ) -> Result<(), RunLoopError> {
        let mut all_steps: Vec<(Uuid, ExecutorStep)> = Vec::new();

        // Process completions into steps
        if !completions.is_empty() {
            let steps = self.compute_completion_steps(completions, result)?;
            all_steps.extend(steps);
        }

        // Process new instances into steps
        if !instances.is_empty() {
            let steps = self.compute_instance_steps(instances, result)?;
            all_steps.extend(steps);
        }

        if all_steps.is_empty() {
            return Ok(());
        }

        // SINGLE persist call for all steps - this is the durability point
        self.persist_steps(&all_steps).await?;

        // ONLY AFTER persist succeeds, queue new actions
        for (executor_id, step) in all_steps {
            self.handle_step(executor_id, step).await?;
        }

        Ok(())
    }

    /// Compute steps from action completions without persisting or queueing.
    fn compute_completion_steps(
        &mut self,
        completions: Vec<ActionCompletion>,
        result: &mut RunLoopResult,
    ) -> Result<Vec<(Uuid, ExecutorStep)>, RunLoopError> {
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
                    .get(&completion.execution_id)
                    .ok_or_else(|| {
                        RunLoopError::Message(format!(
                            "unknown execution node: {}",
                            completion.execution_id
                        ))
                    })?
                    .clone();
                executor.set_action_result(completion.execution_id, completion.result);
                result
                    .completed_actions
                    .entry(executor_id)
                    .or_default()
                    .push(node);
                self.inflight
                    .remove(&(executor_id, completion.execution_id));
                finished_nodes.push(completion.execution_id);
            }
            if !finished_nodes.is_empty() {
                let step = executor.increment_batch(&finished_nodes)?;
                steps.push((executor_id, step));
            }
        }
        Ok(steps)
    }

    /// Compute steps from new instances without persisting or queueing.
    fn compute_instance_steps(
        &mut self,
        instances: Vec<QueuedInstance>,
        result: &mut RunLoopResult,
    ) -> Result<Vec<(Uuid, ExecutorStep)>, RunLoopError> {
        let mut steps: Vec<(Uuid, ExecutorStep)> = Vec::new();
        for instance in instances {
            let executor_id = self.register_instance(instance, result)?;
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
        Ok(steps)
    }

    fn register_instance(
        &mut self,
        instance: QueuedInstance,
        result: &mut RunLoopResult,
    ) -> Result<Uuid, RunLoopError> {
        let state = instance.state.ok_or_else(|| {
            RunLoopError::Message("queued instance missing runner state".to_string())
        })?;
        let executor = RunnerExecutor::new(
            instance.dag.clone(),
            state,
            instance.action_results.clone(),
            Some(self.backend.clone()),
        );
        let executor_id = instance.instance_id;
        let mut executor = executor;
        executor.set_instance_id(executor_id);
        self.executors.insert(executor_id, executor);
        self.entry_nodes.insert(executor_id, instance.entry_node);
        result.completed_actions.entry(executor_id).or_default();
        Ok(executor_id)
    }

    fn should_stop(&self) -> bool {
        self.inflight.is_empty() && self.instances_idle
    }

    #[obs]
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
                    execution_id: action.node_id,
                    action_name: action_spec.action_name,
                    module_name: action_spec.module_name.clone(),
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

    #[obs]
    async fn flush_instances_done(&mut self) -> Result<(), RunLoopError> {
        if self.instances_done_pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.instances_done_pending);
        self.backend.save_instances_done(&pending).await?;
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

    fn active_instances(&self) -> usize {
        self.executors
            .len()
            .saturating_sub(self.completed_executors.len())
    }

    fn available_instance_slots(&self) -> usize {
        self.max_concurrent_instances
            .saturating_sub(self.active_instances())
    }

    fn store_available_instance_slots(&self, slots: &Arc<AtomicUsize>) {
        slots.store(self.available_instance_slots(), Ordering::SeqCst);
    }

    #[obs]
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
        // IMPORTANT: These writes MUST complete before we queue new actions.
        // This ensures durability - if we crash after queueing actions, we have
        // a record in the DB of what state we were in, enabling correct recovery.
        if !actions_done.is_empty() {
            self.backend.save_actions_done(&actions_done).await?;
        }
        if !graph_updates.is_empty() {
            self.backend.save_graphs(&graph_updates).await?;
        }
        Ok(())
    }
}

/// Supervise a run loop, restarting on errors until shutdown.
pub async fn runloop_supervisor<B, W>(
    backend: B,
    worker_pool: W,
    max_concurrent_instances: usize,
    instance_done_batch_size: Option<usize>,
    poll_interval: Duration,
    persistence_interval: Duration,
    shutdown_rx: watch::Receiver<bool>,
) where
    B: CoreBackend + Clone + Send + Sync + 'static,
    W: BaseWorkerPool + Clone + Send + Sync + 'static,
{
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(5);

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        let mut runloop = RunLoop::new(
            worker_pool.clone(),
            backend.clone(),
            max_concurrent_instances,
            instance_done_batch_size,
            poll_interval.as_secs_f64(),
            persistence_interval.as_secs_f64(),
        );

        let result = runloop.run().await;

        if *shutdown_rx.borrow() {
            break;
        }

        match result {
            Ok(_) => {
                backoff = Duration::from_millis(200);
                if poll_interval > Duration::ZERO {
                    tokio::time::sleep(poll_interval).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Err(err) => {
                error!(error = %err, "runloop exited with error; restarting");
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
        }
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

    use crate::backends::MemoryBackend;
    use crate::messages::ast as ir;
    use crate::rappel_core::dag::{
        ActionCallNode, ActionCallParams, DAGEdge, DAGNode, InputNode, OutputNode,
    };
    use crate::rappel_core::runner::RunnerState;
    use crate::workers::ActionCallable;

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
        let worker_pool = crate::workers::InlineWorkerPool::new(actions);

        let mut runloop = RunLoop::new(worker_pool, backend, 25, None, 0.0, 0.1);
        queue.lock().expect("queue lock").push_back(QueuedInstance {
            dag: dag.clone(),
            entry_node: entry_exec.node_id,
            state: Some(state),
            action_results: HashMap::new(),
            instance_id: Uuid::new_v4(),
        });

        let result = runloop.run().await.expect("runloop");
        let executor_id = *result.completed_actions.keys().next().expect("executor id");
        let executor = runloop.executors.get(&executor_id).expect("executor");
        let results: Vec<_> = executor.action_results().values().cloned().collect();
        assert_eq!(results, vec![Value::Number(8.into())]);
    }
}
