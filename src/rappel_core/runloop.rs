//! Runloop for coordinating executors and worker pools.

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc as std_mpsc,
};
use std::thread;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Notify, mpsc, watch};
use tracing::{error, warn};
use uuid::Uuid;

use crate::backends::{
    ActionDone, BackendError, CoreBackend, GraphUpdate, InstanceDone, QueuedInstance,
};
use crate::observability::obs;
use crate::rappel_core::dag::{DAG, DAGNode, OutputNode, ReturnNode};
use crate::rappel_core::runner::{
    DurableUpdates, ExecutorStep, RunnerExecutor, RunnerExecutorError, replay_variables,
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

enum InstanceMessage {
    Batch(Vec<QueuedInstance>),
    Error(BackendError),
}

enum ShardCommand {
    AssignInstances(Vec<QueuedInstance>),
    ActionCompletions(Vec<ActionCompletion>),
    Shutdown,
}

struct ShardStep {
    actions: Vec<ActionRequest>,
    updates: Option<DurableUpdates>,
    instance_done: Option<InstanceDone>,
}

enum ShardEvent {
    Step(ShardStep),
    Error(RunLoopError),
}

enum CoordinatorEvent {
    Completions(Vec<ActionCompletion>),
    Instance(InstanceMessage),
    Shard(ShardEvent),
}

struct ShardExecutor {
    executor_id: Uuid,
    executor: RunnerExecutor,
    entry_node: Uuid,
    inflight: HashSet<Uuid>,
    completed: bool,
}

impl ShardExecutor {
    fn new(executor_id: Uuid, executor: RunnerExecutor, entry_node: Uuid) -> Self {
        Self {
            executor_id,
            executor,
            entry_node,
            inflight: HashSet::new(),
            completed: false,
        }
    }

    fn start(&mut self) -> Result<ShardStep, RunLoopError> {
        let step = self.executor.increment(&[self.entry_node])?;
        self.apply_step(step)
    }

    fn handle_completions(
        &mut self,
        completions: Vec<ActionCompletion>,
    ) -> Result<Option<ShardStep>, RunLoopError> {
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

    fn apply_step(&mut self, step: ExecutorStep) -> Result<ShardStep, RunLoopError> {
        let mut actions = Vec::new();
        for action in &step.actions {
            let action_spec = action.action.clone().ok_or_else(|| {
                RunLoopError::Message("action node missing action spec".to_string())
            })?;
            if self.inflight.contains(&action.node_id) {
                continue;
            }
            self.executor.clear_action_result(action.node_id);
            self.executor
                .state_mut()
                .mark_running(action.node_id)
                .map_err(|err| RunLoopError::Message(err.0))?;
            let kwargs = self.executor.resolve_action_kwargs(&action_spec)?;
            actions.push(ActionRequest {
                executor_id: self.executor_id,
                execution_id: action.node_id,
                action_name: action_spec.action_name,
                module_name: action_spec.module_name.clone(),
                kwargs,
            });
            self.inflight.insert(action.node_id);
        }

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

        Ok(ShardStep {
            actions,
            updates: step.updates,
            instance_done,
        })
    }
}

fn run_executor_shard(
    shard_id: usize,
    backend: Arc<dyn CoreBackend>,
    receiver: std_mpsc::Receiver<ShardCommand>,
    sender: mpsc::UnboundedSender<ShardEvent>,
) {
    let mut executors: HashMap<Uuid, ShardExecutor> = HashMap::new();

    let send_error = |err: RunLoopError, sender: &mpsc::UnboundedSender<ShardEvent>| {
        let _ = sender.send(ShardEvent::Error(err));
    };

    while let Ok(command) = receiver.recv() {
        match command {
            ShardCommand::AssignInstances(instances) => {
                for instance in instances {
                    let state = match instance.state {
                        Some(state) => state,
                        None => {
                            send_error(
                                RunLoopError::Message(
                                    "queued instance missing runner state".to_string(),
                                ),
                                &sender,
                            );
                            continue;
                        }
                    };
                    let mut executor = RunnerExecutor::new(
                        instance.dag,
                        state,
                        instance.action_results,
                        Some(backend.clone()),
                    );
                    executor.set_instance_id(instance.instance_id);
                    let mut owner =
                        ShardExecutor::new(instance.instance_id, executor, instance.entry_node);
                    let step = match owner.start() {
                        Ok(step) => step,
                        Err(err) => {
                            send_error(err, &sender);
                            return;
                        }
                    };
                    let done = step.instance_done.is_some();
                    if sender.send(ShardEvent::Step(step)).is_err() {
                        return;
                    }
                    if !done {
                        executors.insert(instance.instance_id, owner);
                    }
                }
            }
            ShardCommand::ActionCompletions(completions) => {
                let mut grouped: HashMap<Uuid, Vec<ActionCompletion>> = HashMap::new();
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
                            send_error(err, &sender);
                            return;
                        }
                    };
                    let done = step.instance_done.is_some();
                    if sender.send(ShardEvent::Step(step)).is_err() {
                        return;
                    }
                    if done {
                        executors.remove(&executor_id);
                    }
                }
            }
            ShardCommand::Shutdown => {
                break;
            }
        }
    }
}

/// Run loop that fans out executor work across CPU-bound shard threads.
pub struct RunLoop {
    worker_pool: Arc<dyn BaseWorkerPool>,
    backend: Arc<dyn CoreBackend>,
    max_concurrent_instances: usize,
    instance_done_batch_size: usize,
    poll_interval: Duration,
    persistence_interval: Duration,
    shard_count: usize,
}

#[derive(Clone, Debug)]
pub struct RunLoopSupervisorConfig {
    pub max_concurrent_instances: usize,
    pub executor_shards: usize,
    pub instance_done_batch_size: Option<usize>,
    pub poll_interval: Duration,
    pub persistence_interval: Duration,
}

impl RunLoop {
    pub fn new(
        worker_pool: impl BaseWorkerPool + 'static,
        backend: impl CoreBackend + 'static,
        max_concurrent_instances: usize,
        instance_done_batch_size: Option<usize>,
        poll_interval: Duration,
        persistence_interval: Duration,
        shard_count: usize,
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
            poll_interval,
            persistence_interval,
            shard_count: std::cmp::max(1, shard_count),
        }
    }

    fn available_instance_slots(&self, active_instances: usize) -> usize {
        self.max_concurrent_instances
            .saturating_sub(active_instances)
    }

    fn store_available_instance_slots(&self, slots: &Arc<AtomicUsize>, active_instances: usize) {
        slots.store(
            self.available_instance_slots(active_instances),
            Ordering::SeqCst,
        );
    }

    async fn persist_shard_steps(&self, steps: &[ShardStep]) -> Result<(), RunLoopError> {
        let mut actions_done: Vec<ActionDone> = Vec::new();
        let mut graph_updates: Vec<GraphUpdate> = Vec::new();
        for step in steps {
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
        if !actions_done.is_empty() {
            self.backend.save_actions_done(&actions_done).await?;
        }
        if !graph_updates.is_empty() {
            self.backend.save_graphs(&graph_updates).await?;
        }
        Ok(())
    }

    async fn flush_instances_done(
        &self,
        pending: &mut Vec<InstanceDone>,
    ) -> Result<(), RunLoopError> {
        if pending.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(pending);
        self.backend.save_instances_done(&batch).await?;
        Ok(())
    }

    #[obs]
    pub async fn run(&mut self) -> Result<(), RunLoopError> {
        self.worker_pool.launch().await?;

        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ShardEvent>();
        let mut shard_senders: Vec<std_mpsc::Sender<ShardCommand>> =
            Vec::with_capacity(self.shard_count);
        let mut shard_handles = Vec::with_capacity(self.shard_count);

        for shard_id in 0..self.shard_count {
            let (cmd_tx, cmd_rx) = std_mpsc::channel();
            let backend = self.backend.clone();
            let event_tx = event_tx.clone();
            let handle = thread::Builder::new()
                .name(format!("rappel-executor-{shard_id}"))
                .spawn(move || run_executor_shard(shard_id, backend, cmd_rx, event_tx))
                .map_err(|err| {
                    RunLoopError::Message(format!(
                        "failed to spawn executor shard {shard_id}: {err}"
                    ))
                })?;
            shard_senders.push(cmd_tx);
            shard_handles.push(handle);
        }
        drop(event_tx);

        let available_instance_slots = Arc::new(AtomicUsize::new(self.available_instance_slots(0)));

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

        let mut next_shard = 0usize;
        let mut executor_shards: HashMap<Uuid, usize> = HashMap::new();
        let mut instances_idle = false;
        let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
        let mut run_result = Ok(());

        loop {
            if instances_idle && executor_shards.is_empty() {
                break;
            }

            let first_event = tokio::select! {
                Some(completions) = completion_rx.recv() => {
                    Some(CoordinatorEvent::Completions(completions))
                }
                Some(message) = instance_rx.recv() => {
                    Some(CoordinatorEvent::Instance(message))
                }
                Some(event) = event_rx.recv() => {
                    Some(CoordinatorEvent::Shard(event))
                }
                _ = persistence_tick.tick() => {
                    if self.persistence_interval > Duration::ZERO {
                        self.flush_instances_done(&mut instances_done_pending).await?;
                    }
                    None
                }
                else => break,
            };

            let first_event = match first_event {
                Some(event) => event,
                None => {
                    continue;
                }
            };

            let mut all_completions: Vec<ActionCompletion> = Vec::new();
            let mut all_instances: Vec<QueuedInstance> = Vec::new();
            let mut all_steps: Vec<ShardStep> = Vec::new();
            let mut saw_empty_instances = false;

            match first_event {
                CoordinatorEvent::Completions(completions) => {
                    all_completions.extend(completions);
                }
                CoordinatorEvent::Instance(InstanceMessage::Batch(instances)) => {
                    if instances.is_empty() {
                        saw_empty_instances = true;
                    } else {
                        all_instances.extend(instances);
                    }
                }
                CoordinatorEvent::Instance(InstanceMessage::Error(err)) => {
                    run_result = Err(RunLoopError::Backend(err));
                    break;
                }
                CoordinatorEvent::Shard(event) => match event {
                    ShardEvent::Step(step) => all_steps.push(step),
                    ShardEvent::Error(err) => {
                        run_result = Err(err);
                        break;
                    }
                },
            }

            while let Ok(completions) = completion_rx.try_recv() {
                all_completions.extend(completions);
            }
            while let Ok(message) = instance_rx.try_recv() {
                match message {
                    InstanceMessage::Batch(instances) => {
                        if instances.is_empty() {
                            saw_empty_instances = true;
                        } else {
                            all_instances.extend(instances);
                        }
                    }
                    InstanceMessage::Error(err) => {
                        run_result = Err(RunLoopError::Backend(err));
                        break;
                    }
                }
            }
            if run_result.is_err() {
                break;
            }
            while let Ok(event) = event_rx.try_recv() {
                match event {
                    ShardEvent::Step(step) => all_steps.push(step),
                    ShardEvent::Error(err) => {
                        run_result = Err(err);
                        break;
                    }
                }
            }
            if run_result.is_err() {
                break;
            }

            if !all_completions.is_empty() {
                let mut by_shard: HashMap<usize, Vec<ActionCompletion>> = HashMap::new();
                for completion in all_completions {
                    if let Some(shard_idx) = executor_shards.get(&completion.executor_id).copied() {
                        by_shard.entry(shard_idx).or_default().push(completion);
                    } else {
                        warn!(
                            executor_id = %completion.executor_id,
                            "completion for unknown executor"
                        );
                    }
                }
                for (shard_idx, batch) in by_shard {
                    if let Some(sender) = shard_senders.get(shard_idx) {
                        let _ = sender.send(ShardCommand::ActionCompletions(batch));
                    }
                }
            }

            if !all_instances.is_empty() {
                instances_idle = false;
                let mut by_shard: HashMap<usize, Vec<QueuedInstance>> = HashMap::new();
                for instance in all_instances {
                    let shard_idx = next_shard % self.shard_count;
                    next_shard = next_shard.wrapping_add(1);
                    executor_shards.insert(instance.instance_id, shard_idx);
                    by_shard.entry(shard_idx).or_default().push(instance);
                }
                for (shard_idx, batch) in by_shard {
                    if let Some(sender) = shard_senders.get(shard_idx) {
                        let _ = sender.send(ShardCommand::AssignInstances(batch));
                    }
                }
            } else if saw_empty_instances {
                instances_idle = true;
            }

            if !all_steps.is_empty() {
                if let Err(err) = self.persist_shard_steps(&all_steps).await {
                    run_result = Err(err);
                    break;
                }
                for step in all_steps {
                    for request in step.actions {
                        if let Err(err) = self.worker_pool.queue(request) {
                            run_result = Err(err.into());
                            break;
                        }
                    }
                    if run_result.is_err() {
                        break;
                    }
                    if let Some(instance_done) = step.instance_done {
                        executor_shards.remove(&instance_done.executor_id);
                        instances_done_pending.push(instance_done);
                    }
                }
                if run_result.is_err() {
                    break;
                }
            }

            self.store_available_instance_slots(&available_instance_slots, executor_shards.len());

            if instances_done_pending.len() >= self.instance_done_batch_size
                && let Err(err) = self.flush_instances_done(&mut instances_done_pending).await
            {
                run_result = Err(err);
                break;
            }
        }

        stop.store(true, Ordering::SeqCst);
        stop_notify.notify_waiters();
        let _ = completion_handle.await;
        let _ = instance_handle.await;
        if run_result.is_ok()
            && let Err(err) = self.flush_instances_done(&mut instances_done_pending).await
        {
            run_result = Err(err);
        }

        for sender in shard_senders {
            let _ = sender.send(ShardCommand::Shutdown);
        }
        for handle in shard_handles {
            if let Err(err) = handle.join() {
                error!(?err, "executor shard thread panicked");
            }
        }

        run_result
    }
}

/// Supervise a run loop, restarting on errors until shutdown.
pub async fn runloop_supervisor<B, W>(
    backend: B,
    worker_pool: W,
    config: RunLoopSupervisorConfig,
    shutdown_rx: watch::Receiver<bool>,
) where
    B: CoreBackend + Clone + Send + Sync + 'static,
    W: BaseWorkerPool + Clone + Send + Sync + 'static,
{
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(5);

    let RunLoopSupervisorConfig {
        max_concurrent_instances,
        executor_shards,
        instance_done_batch_size,
        poll_interval,
        persistence_interval,
    } = config;

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        let mut runloop = RunLoop::new(
            worker_pool.clone(),
            backend.clone(),
            max_concurrent_instances,
            instance_done_batch_size,
            poll_interval,
            persistence_interval,
            executor_shards,
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

fn compute_instance_payload(executor: &RunnerExecutor) -> (Option<Value>, Option<Value>) {
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

fn build_instance_done(
    executor_id: Uuid,
    entry_node: Uuid,
    executor: &RunnerExecutor,
) -> InstanceDone {
    let (result_payload, error_payload) = compute_instance_payload(executor);
    InstanceDone {
        executor_id,
        entry_node,
        result: result_payload,
        error: error_payload,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

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

        let mut runloop = RunLoop::new(
            worker_pool,
            backend.clone(),
            25,
            None,
            Duration::from_secs_f64(0.0),
            Duration::from_secs_f64(0.1),
            1,
        );
        queue.lock().expect("queue lock").push_back(QueuedInstance {
            dag: dag.clone(),
            entry_node: entry_exec.node_id,
            state: Some(state),
            action_results: HashMap::new(),
            instance_id: Uuid::new_v4(),
        });

        runloop.run().await.expect("runloop");
        let instances_done = backend.instances_done();
        assert_eq!(instances_done.len(), 1);
        let done = &instances_done[0];
        let output = done.result.clone().expect("instance result");
        let Value::Object(map) = output else {
            panic!("expected output object");
        };
        assert_eq!(map.get("y"), Some(&Value::Number(8.into())));
    }
}
