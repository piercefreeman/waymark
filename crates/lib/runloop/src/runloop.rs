//! Runloop for coordinating executors and worker pools.

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc as std_mpsc,
};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use waymark_backends_core::BackendError;
use waymark_core_backend::{
    ActionDone, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim, QueuedInstance,
    QueuedInstanceBatch,
};
use waymark_workflow_registry_backend::WorkflowRegistryBackend;

use crate::commit_barrier::CommitBarrier;
use crate::lock::{InstanceLockTracker, spawn_lock_heartbeat};
use crate::runloop::channel_utils::send_with_stop;
use waymark_dag::{DAG, DAGNode, OutputNode, ReturnNode};
use waymark_observability::obs;
use waymark_runner::{
    DurableUpdates, ExecutorStep, RunnerExecutor, RunnerExecutorError, SleepRequest,
    replay_variables,
};
use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

#[cfg(test)]
mod tests;

mod parts {
    use super::*;

    pub mod blocked_until_by_instance;
    pub mod completions;
    pub mod failed_instances;
    pub mod inflight_dispatches;
    pub mod instances;
    pub mod step_persist_acks;
    pub mod steps;
    pub mod wakes;
}

mod channel_utils;
mod lock_utils;
mod ops;
mod value_utils;

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
    Batch { instances: Vec<QueuedInstance> },
    Error(BackendError),
}

enum ShardCommand {
    AssignInstances(Vec<QueuedInstance>),
    ActionCompletions(Vec<ActionCompletion>),
    Wake(Vec<Uuid>),
    Evict(Vec<Uuid>),
    Shutdown,
}

struct ShardStep {
    executor_id: Uuid,
    actions: Vec<ActionRequest>,
    sleep_requests: Vec<SleepRequest>,
    updates: Option<DurableUpdates>,
    instance_done: Option<InstanceDone>,
}

enum ShardEvent {
    Step(ShardStep),
    InstanceFailed {
        executor_id: Uuid,
        entry_node: Uuid,
        error: String,
    },
}

#[derive(Clone, Debug)]
struct SleepWake {
    executor_id: Uuid,
    node_id: Uuid,
}

#[derive(Clone, Debug)]
struct InflightActionDispatch {
    executor_id: Uuid,
    attempt_number: u32,
    dispatch_token: Uuid,
    timeout_seconds: u32,
    deadline_at: Option<DateTime<Utc>>,
}

enum CoordinatorEvent {
    Completions(Vec<ActionCompletion>),
    Instance(InstanceMessage),
    Shard(ShardEvent),
    SleepWake(SleepWake),
    PersistAck(PersistAck),
    ActionTimeoutTick,
}

struct PersistCommand {
    batch_id: u64,
    instance_ids: HashSet<Uuid>,
    graph_instance_ids: HashSet<Uuid>,
    actions_done: Vec<ActionDone>,
    graph_updates: Vec<GraphUpdate>,
}

enum PersistAck {
    StepsPersisted {
        batch_id: u64,
        lock_statuses: Vec<InstanceLockStatus>,
    },
    StepsPersistFailed {
        batch_id: u64,
        error: RunLoopError,
    },
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

    fn handle_wake(&mut self, node_ids: Vec<Uuid>) -> Result<Option<ShardStep>, RunLoopError> {
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

    fn apply_step(&mut self, step: ExecutorStep) -> Result<ShardStep, RunLoopError> {
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

        Ok(ShardStep {
            executor_id: self.executor_id,
            actions,
            sleep_requests,
            updates: step.updates,
            instance_done,
        })
    }
}

fn run_executor_shard(
    shard_id: usize,
    backend: Arc<dyn waymark_core_backend::CoreBackend>,
    receiver: std_mpsc::Receiver<ShardCommand>,
    sender: mpsc::UnboundedSender<ShardEvent>,
) {
    let mut executors: HashMap<Uuid, ShardExecutor> = HashMap::new();

    let send_instance_failed =
        |executor_id: Uuid,
         entry_node: Uuid,
         err: RunLoopError,
         sender: &mpsc::UnboundedSender<ShardEvent>| {
            let _ = sender.send(ShardEvent::InstanceFailed {
                executor_id,
                entry_node,
                error: err.to_string(),
            });
        };

    while let Ok(command) = receiver.recv() {
        match command {
            ShardCommand::AssignInstances(instances) => {
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
                    let state = match instance.state {
                        Some(state) => state,
                        None => {
                            send_instance_failed(
                                instance.instance_id,
                                instance.entry_node,
                                RunLoopError::Message(
                                    "queued instance missing runner state".to_string(),
                                ),
                                &sender,
                            );
                            continue;
                        }
                    };
                    let dag = match instance.dag {
                        Some(dag) => dag,
                        None => {
                            send_instance_failed(
                                instance.instance_id,
                                instance.entry_node,
                                RunLoopError::Message(
                                    "queued instance missing workflow DAG".to_string(),
                                ),
                                &sender,
                            );
                            continue;
                        }
                    };
                    let mut executor = RunnerExecutor::new(
                        dag,
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
                            send_instance_failed(
                                instance.instance_id,
                                instance.entry_node,
                                err,
                                &sender,
                            );
                            continue;
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
                            let entry_node = owner.entry_node;
                            executors.remove(&executor_id);
                            send_instance_failed(executor_id, entry_node, err, &sender);
                            continue;
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
            ShardCommand::Wake(node_ids) => {
                let mut grouped: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
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
                            send_instance_failed(executor_id, entry_node, err, &sender);
                            continue;
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
            ShardCommand::Evict(instance_ids) => {
                for instance_id in instance_ids {
                    executors.remove(&instance_id);
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
    core_backend: Arc<dyn waymark_core_backend::CoreBackend>,
    registry_backend: Arc<dyn WorkflowRegistryBackend>,
    workflow_cache: HashMap<Uuid, Arc<DAG>>,
    max_concurrent_instances: usize,
    instance_done_batch_size: usize,
    poll_interval: Duration,
    persistence_interval: Duration,
    shard_count: usize,
    lock_uuid: Uuid,
    lock_ttl: Duration,
    lock_heartbeat: Duration,
    evict_sleep_threshold: Duration,
    skip_sleep: bool,
    active_instance_gauge: Option<Arc<AtomicUsize>>,
    shutdown_token: tokio_util::sync::CancellationToken,
    exit_on_idle: bool,
}

#[derive(Clone, Debug)]
pub struct RunLoopConfig {
    pub max_concurrent_instances: usize,
    pub executor_shards: usize,
    pub instance_done_batch_size: Option<usize>,
    pub poll_interval: Duration,
    pub persistence_interval: Duration,
    pub lock_uuid: Uuid,
    pub lock_ttl: Duration,
    pub lock_heartbeat: Duration,
    pub evict_sleep_threshold: Duration,
    pub skip_sleep: bool,
    pub active_instance_gauge: Option<Arc<AtomicUsize>>,
}

impl RunLoop {
    pub fn new(
        worker_pool: impl BaseWorkerPool + 'static,
        backend: impl waymark_core_backend::CoreBackend + WorkflowRegistryBackend + 'static,
        config: RunLoopConfig,
    ) -> Self {
        Self::new_internal(
            worker_pool,
            backend,
            config,
            tokio_util::sync::CancellationToken::new(),
            true,
        )
    }

    pub fn new_with_shutdown(
        worker_pool: impl BaseWorkerPool + 'static,
        backend: impl waymark_core_backend::CoreBackend + WorkflowRegistryBackend + 'static,
        config: RunLoopConfig,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self::new_internal(worker_pool, backend, config, shutdown_token, false)
    }

    fn new_internal(
        worker_pool: impl BaseWorkerPool + 'static,
        backend: impl waymark_core_backend::CoreBackend + WorkflowRegistryBackend + 'static,
        config: RunLoopConfig,
        shutdown_token: tokio_util::sync::CancellationToken,
        exit_on_idle: bool,
    ) -> Self {
        let max_concurrent_instances = std::cmp::max(1, config.max_concurrent_instances);
        let backend = Arc::new(backend);
        let core_backend: Arc<dyn waymark_core_backend::CoreBackend> = backend.clone();
        let registry_backend: Arc<dyn WorkflowRegistryBackend> = backend;
        Self {
            worker_pool: Arc::new(worker_pool),
            core_backend,
            registry_backend,
            workflow_cache: HashMap::new(),
            max_concurrent_instances,
            instance_done_batch_size: std::cmp::max(
                1,
                config
                    .instance_done_batch_size
                    .unwrap_or(max_concurrent_instances),
            ),
            poll_interval: config.poll_interval,
            persistence_interval: config.persistence_interval,
            shard_count: std::cmp::max(1, config.executor_shards),
            lock_uuid: config.lock_uuid,
            lock_ttl: config.lock_ttl,
            lock_heartbeat: config.lock_heartbeat,
            evict_sleep_threshold: config.evict_sleep_threshold,
            skip_sleep: config.skip_sleep,
            active_instance_gauge: config.active_instance_gauge.clone(),
            shutdown_token,
            exit_on_idle,
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
        if let Some(gauge) = &self.active_instance_gauge {
            gauge.store(active_instances, Ordering::SeqCst);
        }
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
            let backend = self.core_backend.clone();
            let event_tx = event_tx.clone();
            let handle = thread::Builder::new()
                .name(format!("waymark-executor-{shard_id}"))
                .stack_size(128 * 1024 * 1024 /* 128 MB */)
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
        self.store_available_instance_slots(&available_instance_slots, 0);

        let (completion_tx, mut completion_rx) = mpsc::channel::<Vec<ActionCompletion>>(32);
        let (instance_tx, mut instance_rx) = mpsc::channel::<InstanceMessage>(16);
        let (sleep_tx, mut sleep_rx) = mpsc::unbounded_channel::<SleepWake>();

        let lock_tracker = InstanceLockTracker::new(self.lock_uuid);
        let lock_handle = spawn_lock_heartbeat(
            self.core_backend.clone(),
            lock_tracker.clone(),
            self.lock_heartbeat,
            self.lock_ttl,
            self.shutdown_token.clone().cancelled_owned(),
        );

        let worker_pool = self.worker_pool.clone();
        let completion_shutdown_token = self.shutdown_token.clone();
        let completion_handle = tokio::spawn(async move {
            let _completion_shutdown_guard = completion_shutdown_token.drop_guard_ref();
            loop {
                if completion_shutdown_token.is_cancelled() {
                    info!("completion task stop flag set");
                    break;
                }
                debug!("completion task awaiting completions");
                let completions = tokio::select! {
                    _ = completion_shutdown_token.cancelled() => {
                        info!("completion task stop notified");
                        break;
                    }
                    completions = worker_pool.get_complete() => {
                        debug!(count = completions.len(), "completion task received completions");
                        completions
                    },
                };
                if completions.is_empty() {
                    continue;
                }
                debug!(
                    count = completions.len(),
                    "completion task sending completions"
                );

                if !send_with_stop(
                    &completion_tx,
                    completions,
                    completion_shutdown_token.cancelled(),
                    "completions",
                )
                .await
                {
                    break;
                }

                debug!("completion task sent completions");
            }
            info!("completion task exiting");
        });

        let backend = self.core_backend.clone();
        let poll_interval = self.poll_interval;
        let max_concurrent_instances = self.max_concurrent_instances;
        let lock_uuid = self.lock_uuid;
        let lock_ttl = self.lock_ttl;
        let instance_available_slots = Arc::clone(&available_instance_slots);
        let instance_shutdown_token = self.shutdown_token.clone();
        let instance_handle = tokio::spawn(async move {
            let _instance_shutdown_guard = instance_shutdown_token.drop_guard_ref();
            loop {
                if instance_shutdown_token.is_cancelled() {
                    info!("instance poller stop flag set");
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
                let lock_expires_at = Utc::now()
                    + chrono::Duration::from_std(lock_ttl)
                        .unwrap_or_else(|_| chrono::Duration::seconds(0));
                let batch = backend
                    .get_queued_instances(
                        batch_size,
                        LockClaim {
                            lock_uuid,
                            lock_expires_at,
                        },
                    )
                    .await;
                let message = match batch {
                    Ok(QueuedInstanceBatch { instances }) => {
                        let count = instances.len();
                        debug!(count, "polled queued instances");
                        InstanceMessage::Batch { instances }
                    }
                    Err(err) => InstanceMessage::Error(err),
                };
                if !send_with_stop(
                    &instance_tx,
                    message,
                    instance_shutdown_token.cancelled(),
                    "instance message",
                )
                .await
                {
                    break;
                }
                if poll_interval > Duration::ZERO {
                    tokio::time::sleep(poll_interval).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(0)).await;
                }
            }
            info!("instance poller exiting");
        });

        const PERSIST_COALESCE_WINDOW: Duration = Duration::from_millis(2);
        const PERSIST_COALESCE_MAX_COMMANDS: usize = 128;

        let (persist_tx, mut persist_rx) = mpsc::channel::<PersistCommand>(64);
        let (persist_ack_tx, mut persist_ack_rx) = mpsc::unbounded_channel::<PersistAck>();
        let persist_backend = self.core_backend.clone();
        let persist_lock_uuid = self.lock_uuid;
        let persist_lock_ttl = self.lock_ttl;
        let persist_handle = tokio::spawn(async move {
            loop {
                let first_command = persist_rx.recv().await;
                let Some(first_command) = first_command else {
                    info!("persistence task channel closed");
                    break;
                };
                let mut step_commands = vec![first_command];
                let mut channel_closed = false;
                let deadline = Instant::now() + PERSIST_COALESCE_WINDOW;
                while step_commands.len() < PERSIST_COALESCE_MAX_COMMANDS {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    let wait = deadline.saturating_duration_since(now);
                    match tokio::time::timeout(wait, persist_rx.recv()).await {
                        Ok(Some(command)) => step_commands.push(command),
                        Ok(None) => {
                            channel_closed = true;
                            break;
                        }
                        Err(_) => break,
                    }
                }

                let mut all_actions_done: Vec<ActionDone> = Vec::new();
                let mut all_graph_updates: Vec<GraphUpdate> = Vec::new();
                for command in &mut step_commands {
                    all_actions_done.append(&mut command.actions_done);
                    all_graph_updates.append(&mut command.graph_updates);
                }

                let outcome: Result<HashMap<Uuid, InstanceLockStatus>, BackendError> = async {
                    if !all_actions_done.is_empty() {
                        persist_backend.save_actions_done(&all_actions_done).await?;
                    }
                    if all_graph_updates.is_empty() {
                        return Ok(HashMap::new());
                    }
                    let lock_expires_at = Utc::now()
                        + chrono::Duration::from_std(persist_lock_ttl)
                            .unwrap_or_else(|_| chrono::Duration::seconds(0));
                    let lock_statuses = persist_backend
                        .save_graphs(
                            LockClaim {
                                lock_uuid: persist_lock_uuid,
                                lock_expires_at,
                            },
                            &all_graph_updates,
                        )
                        .await?;
                    let mut lock_status_by_instance: HashMap<Uuid, InstanceLockStatus> =
                        HashMap::with_capacity(lock_statuses.len());
                    for status in lock_statuses {
                        lock_status_by_instance.insert(status.instance_id, status);
                    }
                    Ok(lock_status_by_instance)
                }
                .await;

                match outcome {
                    Ok(lock_status_by_instance) => {
                        for command in step_commands {
                            if command.instance_ids.is_empty() {
                                continue;
                            }
                            let graph_instance_count = command.graph_instance_ids.len();
                            let mut missing_lock_statuses = 0usize;
                            let mut lock_statuses = Vec::with_capacity(graph_instance_count);
                            for instance_id in command.graph_instance_ids {
                                if let Some(status) = lock_status_by_instance.get(&instance_id) {
                                    lock_statuses.push(status.clone());
                                } else {
                                    missing_lock_statuses += 1;
                                    lock_statuses.push(InstanceLockStatus {
                                        instance_id,
                                        lock_uuid: None,
                                        lock_expires_at: None,
                                    });
                                }
                            }
                            if missing_lock_statuses > 0 {
                                warn!(
                                    batch_id = command.batch_id,
                                    graph_instance_count,
                                    missing_lock_statuses,
                                    "persist ack missing graph lock statuses"
                                );
                            }
                            let ack = PersistAck::StepsPersisted {
                                batch_id: command.batch_id,
                                lock_statuses,
                            };
                            if persist_ack_tx.send(ack).is_err() {
                                warn!("persistence ack receiver dropped");
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        let error_message = format!("persistence batch failed: {error}");
                        for command in step_commands {
                            let ack = PersistAck::StepsPersistFailed {
                                batch_id: command.batch_id,
                                error: RunLoopError::Message(error_message.clone()),
                            };
                            if persist_ack_tx.send(ack).is_err() {
                                warn!("persistence ack receiver dropped");
                                break;
                            }
                        }
                    }
                }
                if channel_closed {
                    info!("persistence task channel closed");
                    break;
                }
            }
            info!("persistence task exiting");
        });

        let persistence_interval = if self.persistence_interval > Duration::ZERO {
            self.persistence_interval
        } else {
            Duration::from_secs(3600)
        };
        let mut persistence_tick = tokio::time::interval(persistence_interval);
        let timeout_scan_interval = if self.poll_interval > Duration::ZERO {
            std::cmp::max(
                Duration::from_millis(25),
                std::cmp::min(self.poll_interval, Duration::from_millis(250)),
            )
        } else {
            Duration::from_millis(100)
        };
        let mut action_timeout_tick = tokio::time::interval(timeout_scan_interval);
        action_timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut next_shard = 0usize;
        let mut executor_shards: HashMap<Uuid, usize> = HashMap::new();
        let mut inflight_actions: HashMap<Uuid, usize> = HashMap::new();
        let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
        let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
        let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
        let mut blocked_until_by_instance: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
        let mut commit_barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
        let mut instances_idle = false;
        let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
        let shutdown_token = self.shutdown_token.clone();

        let mut run_result = 'runloop: loop {
            if shutdown_token.is_cancelled() {
                info!("runloop exiting: shutdown requested");
                break 'runloop Ok(());
            }

            if self.exit_on_idle
                && instances_idle
                && executor_shards.is_empty()
                && sleeping_nodes.is_empty()
            {
                warn!(
                    inflight = inflight_actions.len(),
                    blocked = blocked_until_by_instance.len(),
                    "runloop exiting: idle with no active executors"
                );
                break 'runloop Ok(());
            }

            let first_event = tokio::select! {
                _ = shutdown_token.cancelled() => {
                    info!("runloop exiting: shutdown requested");
                    break 'runloop Ok(());
                }
                Some(completions) = completion_rx.recv() => {
                    Some(CoordinatorEvent::Completions(completions))
                }
                Some(message) = instance_rx.recv() => {
                    Some(CoordinatorEvent::Instance(message))
                }
                Some(wake) = sleep_rx.recv() => {
                    Some(CoordinatorEvent::SleepWake(wake))
                }
                Some(event) = event_rx.recv() => {
                    Some(CoordinatorEvent::Shard(event))
                }
                Some(ack) = persist_ack_rx.recv() => {
                    Some(CoordinatorEvent::PersistAck(ack))
                }
                _ = persistence_tick.tick() => {
                    if self.persistence_interval > Duration::ZERO {
                        ops::flush_instances_done(self.core_backend.as_ref(), &mut instances_done_pending).await?;
                    }
                    None
                }
                _ = action_timeout_tick.tick() => {
                    Some(CoordinatorEvent::ActionTimeoutTick)
                }
                else => {
                    warn!("runloop exiting: event channels closed");
                    break 'runloop Ok(());
                },
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
            let mut all_failed_instances: Vec<InstanceDone> = Vec::new();
            let mut all_wakes: Vec<SleepWake> = Vec::new();
            let mut all_persist_acks: Vec<PersistAck> = Vec::new();
            let mut saw_empty_instances = false;

            match first_event {
                CoordinatorEvent::Completions(completions) => {
                    all_completions.extend(completions);
                }
                CoordinatorEvent::Instance(InstanceMessage::Batch { instances }) => {
                    if instances.is_empty() {
                        saw_empty_instances = true;
                    } else {
                        all_instances.extend(instances);
                    }
                }
                CoordinatorEvent::Instance(InstanceMessage::Error(err)) => {
                    warn!(error = %err, "runloop exiting: instance poller backend error");
                    break 'runloop Err(RunLoopError::Backend(err));
                }
                CoordinatorEvent::Shard(event) => match event {
                    ShardEvent::Step(step) => all_steps.push(step),
                    ShardEvent::InstanceFailed {
                        executor_id,
                        entry_node,
                        error,
                    } => {
                        all_failed_instances.push(InstanceDone {
                            executor_id,
                            entry_node,
                            result: None,
                            error: Some(error_value("ExecutionError", &error)),
                        });
                    }
                },
                CoordinatorEvent::SleepWake(wake) => {
                    all_wakes.push(wake);
                }
                CoordinatorEvent::PersistAck(ack) => {
                    all_persist_acks.push(ack);
                }
                CoordinatorEvent::ActionTimeoutTick => {}
            }

            while let Ok(completions) = completion_rx.try_recv() {
                all_completions.extend(completions);
            }
            while let Ok(message) = instance_rx.try_recv() {
                match message {
                    InstanceMessage::Batch { instances } => {
                        if instances.is_empty() {
                            saw_empty_instances = true;
                        } else {
                            all_instances.extend(instances);
                        }
                    }
                    InstanceMessage::Error(err) => {
                        warn!(error = %err, "runloop exiting: instance poller backend error");
                        break 'runloop Err(RunLoopError::Backend(err));
                    }
                }
            }

            while let Ok(event) = event_rx.try_recv() {
                match event {
                    ShardEvent::Step(step) => all_steps.push(step),
                    ShardEvent::InstanceFailed {
                        executor_id,
                        entry_node,
                        error,
                    } => {
                        all_failed_instances.push(InstanceDone {
                            executor_id,
                            entry_node,
                            result: None,
                            error: Some(error_value("ExecutionError", &error)),
                        });
                    }
                }
            }

            while let Ok(wake) = sleep_rx.try_recv() {
                all_wakes.push(wake);
            }
            while let Ok(ack) = persist_ack_rx.try_recv() {
                all_persist_acks.push(ack);
            }

            // Derive timeout completion from inflight dispatches.
            parts::inflight_dispatches::prepend_timeout_completions_from_inflight_dispatches(
                &mut all_completions,
                &inflight_dispatches,
            );

            // Handle step persist acks.
            {
                let ctx = parts::step_persist_acks::Context {
                    executor_shards: &mut executor_shards,
                    shard_senders: &shard_senders,
                    lock_tracker: &lock_tracker,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                    instances_done_pending: &mut instances_done_pending,
                    sleep_tx: &sleep_tx,
                };
                let result = parts::step_persist_acks::handle(
                    ctx,
                    self.core_backend.as_ref(),
                    self.worker_pool.as_ref(),
                    self.lock_uuid,
                    self.skip_sleep,
                    all_persist_acks,
                )
                .await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let error = match error {
                        parts::step_persist_acks::Error::StepsPersistFailed(run_loop_error) => {
                            run_loop_error
                        }
                        parts::step_persist_acks::Error::StepsPersisted(run_loop_error) => {
                            run_loop_error
                        }
                    };
                    break 'runloop Err(error);
                }
            }

            // Handle all completions.
            {
                let ctx = parts::completions::Context {
                    executor_shards: &mut executor_shards,
                    shard_senders: &shard_senders,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    commit_barrier: &mut commit_barrier,
                };
                parts::completions::handle(ctx, all_completions);
            }

            // Handle all wakes.
            {
                let ctx = parts::wakes::Context {
                    executor_shards: &mut executor_shards,
                    shard_senders: &shard_senders,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                };
                parts::wakes::handle(ctx, all_wakes);
            }

            // Handle all instances.
            {
                let ctx = parts::instances::Context {
                    executor_shards: &mut executor_shards,
                    shard_senders: &mut shard_senders,
                    lock_tracker: &lock_tracker,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                    workflow_cache: &mut self.workflow_cache,
                    registry_backend: self.registry_backend.as_ref(),
                };

                let result = parts::instances::handle(
                    ctx,
                    &mut instances_idle,
                    &mut next_shard,
                    self.shard_count,
                    all_instances,
                    saw_empty_instances,
                )
                .await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let parts::instances::Error::Hydrate(error) = error;
                    break 'runloop Err(error);
                }
            }

            // Handle failed instances.
            {
                let ctx = parts::failed_instances::Context {
                    executor_shards: &mut executor_shards,
                    lock_tracker: &lock_tracker,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                };
                parts::failed_instances::handle(
                    ctx,
                    all_failed_instances,
                    &mut instances_done_pending,
                );
            }

            // Handle steps.
            {
                let ctx = parts::steps::Context {
                    shutdown_signal: shutdown_token.cancelled(),
                    persist_tx: &persist_tx,
                    commit_barrier: &mut commit_barrier,
                };
                let result = parts::steps::handle(ctx, all_steps).await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let error = match error {
                        err @ parts::steps::Error::SubmittingPersistBatch => {
                            RunLoopError::Message(err.to_string())
                        }
                    };
                    break 'runloop Err(error);
                }
            }

            // Handle all blocked-until-by-instances.
            {
                let ctx = parts::blocked_until_by_instance::Context {
                    executor_shards: &mut executor_shards,
                    shard_senders: &mut shard_senders,
                    lock_tracker: &lock_tracker,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                };
                let result = parts::blocked_until_by_instance::handle(
                    ctx,
                    self.core_backend.as_ref(),
                    lock_uuid,
                    self.evict_sleep_threshold,
                )
                .await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let parts::blocked_until_by_instance::Error::EvictInstance(error) = error;
                    break 'runloop Err(error);
                }
            }

            self.store_available_instance_slots(&available_instance_slots, executor_shards.len());

            if instances_done_pending.len() >= self.instance_done_batch_size
                && let Err(err) = ops::flush_instances_done(
                    self.core_backend.as_ref(),
                    &mut instances_done_pending,
                )
                .await
            {
                break 'runloop Err(err);
            }
        };

        info!(
            instances_idle,
            executors = executor_shards.len(),
            sleeping = sleeping_nodes.len(),
            inflight = inflight_actions.len(),
            blocked = blocked_until_by_instance.len(),
            "runloop stopping"
        );
        if let Err(err) = &run_result {
            error!(error = %err, "runloop stopping due to error");
        }
        if commit_barrier.pending_batch_count() > 0 {
            warn!(
                pending = commit_barrier.pending_batch_count(),
                "runloop stopping with unacked persist batches"
            );
        }
        drop(persist_tx);
        let _ = persist_handle.await;
        shutdown_token.cancel();
        let _ = completion_handle.await;
        let _ = instance_handle.await;
        let _ = lock_handle.await;
        if run_result.is_ok()
            && let Err(err) =
                ops::flush_instances_done(self.core_backend.as_ref(), &mut instances_done_pending)
                    .await
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

        let remaining_locks = lock_tracker.snapshot();
        if !remaining_locks.is_empty()
            && let Err(err) = self
                .core_backend
                .release_instance_locks(self.lock_uuid, &remaining_locks)
                .await
        {
            warn!(error = %err, count = remaining_locks.len(), "failed to release instance locks on shutdown");
        }

        if let Some(gauge) = &self.active_instance_gauge {
            gauge.store(0, Ordering::SeqCst);
        }

        run_result
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
