//! Runloop for coordinating executors and worker pools.

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc as std_mpsc,
};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::Value;
use tokio::sync::{Notify, mpsc, watch};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::backends::{
    ActionDone, BackendError, CoreBackend, GraphUpdate, InstanceDone, InstanceLockStatus,
    LockClaim, QueuedInstance, QueuedInstanceBatch, WorkflowRegistryBackend,
};
use crate::messages::ast as ir;
use crate::observability::obs;
use crate::waymark_core::dag::{DAG, DAGNode, OutputNode, ReturnNode, convert_to_dag};
use crate::waymark_core::lock::{InstanceLockTracker, spawn_lock_heartbeat};
use crate::waymark_core::runner::synthetic_exceptions::{
    SyntheticExceptionType, build_synthetic_exception_value,
};
use crate::waymark_core::runner::{
    DurableUpdates, ExecutorStep, RunnerExecutor, RunnerExecutorError, SleepRequest,
    replay_variables,
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

struct EvictionState<'a> {
    executor_shards: &'a mut HashMap<Uuid, usize>,
    shard_senders: &'a [std_mpsc::Sender<ShardCommand>],
    lock_tracker: &'a InstanceLockTracker,
    inflight_actions: &'a mut HashMap<Uuid, usize>,
    inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
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
    ActionTimeoutTick,
}

async fn send_instance_message_with_stop(
    instance_tx: &mpsc::Sender<InstanceMessage>,
    message: InstanceMessage,
    stop_notify: &Notify,
) -> bool {
    let send_fut = instance_tx.send(message);
    tokio::pin!(send_fut);
    let mut warned = false;
    loop {
        tokio::select! {
            res = &mut send_fut => {
                if res.is_err() {
                    warn!("instance poller receiver dropped");
                    return false;
                }
                return true;
            }
            _ = stop_notify.notified() => {
                info!("instance poller stop notified during send");
                return false;
            }
            _ = tokio::time::sleep(Duration::from_secs(2)), if !warned => {
                warn!("instance poller send pending >2s");
                warned = true;
            }
        }
    }
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
    backend: Arc<dyn CoreBackend>,
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
    core_backend: Arc<dyn CoreBackend>,
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
    shutdown_rx: Option<watch::Receiver<bool>>,
}

#[derive(Clone, Debug)]
pub struct RunLoopSupervisorConfig {
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
        backend: impl CoreBackend + WorkflowRegistryBackend + 'static,
        config: RunLoopSupervisorConfig,
    ) -> Self {
        Self::new_internal(worker_pool, backend, config, None)
    }

    pub fn new_with_shutdown(
        worker_pool: impl BaseWorkerPool + 'static,
        backend: impl CoreBackend + WorkflowRegistryBackend + 'static,
        config: RunLoopSupervisorConfig,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::new_internal(worker_pool, backend, config, Some(shutdown_rx))
    }

    fn new_internal(
        worker_pool: impl BaseWorkerPool + 'static,
        backend: impl CoreBackend + WorkflowRegistryBackend + 'static,
        config: RunLoopSupervisorConfig,
        shutdown_rx: Option<watch::Receiver<bool>>,
    ) -> Self {
        let max_concurrent_instances = std::cmp::max(1, config.max_concurrent_instances);
        let backend = Arc::new(backend);
        let core_backend: Arc<dyn CoreBackend> = backend.clone();
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
            shutdown_rx,
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

    fn lock_mismatches(&self, locks: &[InstanceLockStatus]) -> HashSet<Uuid> {
        let now = Utc::now();
        locks
            .iter()
            .filter(|status| {
                status.lock_uuid != Some(self.lock_uuid)
                    || status
                        .lock_expires_at
                        .map(|expires_at| expires_at <= now)
                        .unwrap_or(true)
            })
            .map(|status| status.instance_id)
            .collect()
    }

    async fn hydrate_instances(
        &mut self,
        instances: &mut [QueuedInstance],
    ) -> Result<(), RunLoopError> {
        let mut missing = Vec::new();
        for instance in instances.iter() {
            if !self
                .workflow_cache
                .contains_key(&instance.workflow_version_id)
            {
                missing.push(instance.workflow_version_id);
            }
        }
        missing.sort();
        missing.dedup();

        if !missing.is_empty() {
            let versions = self
                .registry_backend
                .get_workflow_versions(&missing)
                .await
                .map_err(RunLoopError::Backend)?;
            for version in versions {
                let program = ir::Program::decode(&version.program_proto[..])
                    .map_err(|err| RunLoopError::Message(format!("invalid workflow IR: {err}")))?;
                let dag = convert_to_dag(&program)
                    .map_err(|err| RunLoopError::Message(format!("invalid workflow DAG: {err}")))?;
                self.workflow_cache.insert(version.id, Arc::new(dag));
            }
        }

        for instance in instances.iter_mut() {
            let dag = self
                .workflow_cache
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

    async fn persist_shard_steps(
        &self,
        steps: &[ShardStep],
    ) -> Result<Vec<InstanceLockStatus>, RunLoopError> {
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
            return Ok(Vec::new());
        }
        if !actions_done.is_empty() {
            self.core_backend.save_actions_done(&actions_done).await?;
        }
        if !graph_updates.is_empty() {
            let lock_expires_at = Utc::now()
                + chrono::Duration::from_std(self.lock_ttl)
                    .unwrap_or_else(|_| chrono::Duration::seconds(0));
            return Ok(self
                .core_backend
                .save_graphs(
                    LockClaim {
                        lock_uuid: self.lock_uuid,
                        lock_expires_at,
                    },
                    &graph_updates,
                )
                .await?);
        }
        Ok(Vec::new())
    }

    async fn flush_instances_done(
        &self,
        pending: &mut Vec<InstanceDone>,
    ) -> Result<(), RunLoopError> {
        if pending.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(pending);
        self.core_backend.save_instances_done(&batch).await?;
        Ok(())
    }

    async fn evict_instances(
        &self,
        instance_ids: &[Uuid],
        state: &mut EvictionState<'_>,
    ) -> Result<(), RunLoopError> {
        if instance_ids.is_empty() {
            return Ok(());
        }
        let mut by_shard: HashMap<usize, Vec<Uuid>> = HashMap::new();
        let evicted_instance_ids: HashSet<Uuid> = instance_ids.iter().copied().collect();
        for instance_id in instance_ids {
            if let Some(shard_idx) = state.executor_shards.remove(instance_id) {
                by_shard.entry(shard_idx).or_default().push(*instance_id);
            }
            state.inflight_actions.remove(instance_id);
            if let Some(nodes) = state.sleeping_by_instance.remove(instance_id) {
                for node_id in nodes {
                    state.sleeping_nodes.remove(&node_id);
                }
            }
            state.blocked_until_by_instance.remove(instance_id);
        }
        state
            .inflight_dispatches
            .retain(|_, dispatch| !evicted_instance_ids.contains(&dispatch.executor_id));
        state.lock_tracker.remove_all(instance_ids.iter().copied());
        for (shard_idx, ids) in by_shard {
            if let Some(sender) = state.shard_senders.get(shard_idx) {
                let _ = sender.send(ShardCommand::Evict(ids));
            }
        }
        self.core_backend
            .release_instance_locks(self.lock_uuid, instance_ids)
            .await?;
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
            let backend = self.core_backend.clone();
            let event_tx = event_tx.clone();
            let handle = thread::Builder::new()
                .name(format!("waymark-executor-{shard_id}"))
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
        let stop = Arc::new(AtomicBool::new(false));
        let stop_notify = Arc::new(Notify::new());
        let lock_tracker = InstanceLockTracker::new(self.lock_uuid);
        let lock_handle = spawn_lock_heartbeat(
            self.core_backend.clone(),
            lock_tracker.clone(),
            self.lock_heartbeat,
            self.lock_ttl,
            stop.clone(),
            stop_notify.clone(),
        );

        let worker_pool = self.worker_pool.clone();
        let completion_stop = stop.clone();
        let completion_notify = stop_notify.clone();
        let completion_handle = tokio::spawn(async move {
            loop {
                if completion_stop.load(Ordering::SeqCst) {
                    info!("completion task stop flag set");
                    break;
                }
                debug!("completion task awaiting completions");
                let completions = tokio::select! {
                    _ = completion_notify.notified() => {
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
                let send_fut = completion_tx.send(completions);
                tokio::pin!(send_fut);
                let mut warned = false;
                let mut stop_during_send = false;
                let send_result = loop {
                    tokio::select! {
                        res = &mut send_fut => break Some(res),
                        _ = completion_notify.notified() => {
                            info!("completion task stop notified during send");
                            stop_during_send = true;
                            break None;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(2)), if !warned => {
                            warn!("completion task send pending >2s");
                            warned = true;
                        }
                    }
                };
                if stop_during_send {
                    break;
                }
                if send_result.is_none() || send_result.unwrap().is_err() {
                    warn!("completion task receiver dropped");
                    break;
                }
                debug!("completion task sent completions");
            }
            info!("completion task exiting");
            completion_notify.notify_waiters();
        });

        let backend = self.core_backend.clone();
        let poll_interval = self.poll_interval;
        let max_concurrent_instances = self.max_concurrent_instances;
        let lock_uuid = self.lock_uuid;
        let lock_ttl = self.lock_ttl;
        let instance_available_slots = Arc::clone(&available_instance_slots);
        let instance_stop = stop.clone();
        let instance_notify = stop_notify.clone();
        let instance_handle = tokio::spawn(async move {
            loop {
                if instance_stop.load(Ordering::SeqCst) {
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
                if !send_instance_message_with_stop(&instance_tx, message, &instance_notify).await {
                    break;
                }
                if poll_interval > Duration::ZERO {
                    tokio::time::sleep(poll_interval).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(0)).await;
                }
            }
            info!("instance poller exiting");
            instance_notify.notify_waiters();
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
        let mut instances_idle = false;
        let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
        let mut run_result = Ok(());
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            if let Some(rx) = shutdown_rx.as_ref()
                && *rx.borrow()
            {
                info!("runloop exiting: shutdown requested");
                break;
            }

            if shutdown_rx.is_none()
                && instances_idle
                && executor_shards.is_empty()
                && sleeping_nodes.is_empty()
            {
                warn!(
                    inflight = inflight_actions.len(),
                    blocked = blocked_until_by_instance.len(),
                    "runloop exiting: idle with no active executors"
                );
                break;
            }

            let first_event = tokio::select! {
                shutdown_signal = async {
                    if let Some(rx) = shutdown_rx.as_mut() {
                        rx.changed().await.is_ok()
                    } else {
                        std::future::pending::<bool>().await
                    }
                } => {
                    if !shutdown_signal || shutdown_rx.as_ref().is_some_and(|rx| *rx.borrow()) {
                        info!("runloop exiting: shutdown requested");
                        break;
                    }
                    None
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
                _ = persistence_tick.tick() => {
                    if self.persistence_interval > Duration::ZERO {
                        self.flush_instances_done(&mut instances_done_pending).await?;
                    }
                    None
                }
                _ = action_timeout_tick.tick() => {
                    Some(CoordinatorEvent::ActionTimeoutTick)
                }
                else => {
                    warn!("runloop exiting: event channels closed");
                    break;
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
                    run_result = Err(RunLoopError::Backend(err));
                    break;
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
                        run_result = Err(RunLoopError::Backend(err));
                        break;
                    }
                }
            }
            if run_result.is_err() {
                warn!("runloop exiting: error after draining instance messages");
                break;
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
            if run_result.is_err() {
                warn!("runloop exiting: error after draining shard events");
                break;
            }
            while let Ok(wake) = sleep_rx.try_recv() {
                all_wakes.push(wake);
            }

            if !inflight_dispatches.is_empty() {
                let now = Utc::now();
                let timed_out_ids: Vec<Uuid> = inflight_dispatches
                    .iter()
                    .filter_map(|(execution_id, dispatch)| {
                        dispatch
                            .deadline_at
                            .filter(|deadline| *deadline <= now)
                            .map(|_| *execution_id)
                    })
                    .collect();
                if !timed_out_ids.is_empty() {
                    let mut timeout_completions = Vec::with_capacity(timed_out_ids.len());
                    for execution_id in timed_out_ids {
                        let Some(dispatch) = inflight_dispatches.get(&execution_id) else {
                            continue;
                        };
                        timeout_completions.push(ActionCompletion {
                            executor_id: dispatch.executor_id,
                            execution_id,
                            attempt_number: dispatch.attempt_number,
                            dispatch_token: dispatch.dispatch_token,
                            result: action_timeout_value(
                                execution_id,
                                dispatch.attempt_number,
                                dispatch.timeout_seconds,
                            ),
                        });
                    }
                    if !timeout_completions.is_empty() {
                        timeout_completions.append(&mut all_completions);
                        all_completions = timeout_completions;
                    }
                }
            }

            if !all_completions.is_empty() {
                let mut accepted = Vec::new();
                for completion in all_completions {
                    let Some(expected) = inflight_dispatches.get(&completion.execution_id) else {
                        debug!(
                            executor_id = %completion.executor_id,
                            execution_id = %completion.execution_id,
                            "dropping completion for unknown inflight action"
                        );
                        continue;
                    };
                    if expected.executor_id != completion.executor_id {
                        debug!(
                            expected_executor_id = %expected.executor_id,
                            completion_executor_id = %completion.executor_id,
                            execution_id = %completion.execution_id,
                            "dropping completion with mismatched executor ownership"
                        );
                        continue;
                    }
                    if expected.dispatch_token != completion.dispatch_token
                        || expected.attempt_number != completion.attempt_number
                    {
                        debug!(
                            execution_id = %completion.execution_id,
                            expected_attempt = expected.attempt_number,
                            completion_attempt = completion.attempt_number,
                            expected_dispatch_token = %expected.dispatch_token,
                            completion_dispatch_token = %completion.dispatch_token,
                            "dropping stale completion for prior attempt"
                        );
                        continue;
                    }
                    inflight_dispatches.remove(&completion.execution_id);
                    if let Some(count) = inflight_actions.get_mut(&completion.executor_id) {
                        if *count > 0 {
                            *count -= 1;
                        }
                        if *count == 0 {
                            inflight_actions.remove(&completion.executor_id);
                        }
                    }
                    accepted.push(completion);
                }
                if !accepted.is_empty() {
                    let mut by_shard: HashMap<usize, Vec<ActionCompletion>> = HashMap::new();
                    for completion in accepted {
                        if let Some(shard_idx) =
                            executor_shards.get(&completion.executor_id).copied()
                        {
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
            }

            if !all_wakes.is_empty() {
                let now = Utc::now();
                let mut by_shard: HashMap<usize, Vec<Uuid>> = HashMap::new();
                for wake in all_wakes {
                    let Some(request) = sleeping_nodes.get(&wake.node_id) else {
                        continue;
                    };
                    if request.wake_at > now {
                        continue;
                    }
                    sleeping_nodes.remove(&wake.node_id);
                    if let Some(nodes) = sleeping_by_instance.get_mut(&wake.executor_id) {
                        nodes.remove(&wake.node_id);
                        if nodes.is_empty() {
                            sleeping_by_instance.remove(&wake.executor_id);
                            blocked_until_by_instance.remove(&wake.executor_id);
                        } else {
                            let mut next_wake: Option<DateTime<Utc>> = None;
                            for node_id in nodes.iter() {
                                if let Some(entry) = sleeping_nodes.get(node_id) {
                                    next_wake = Some(match next_wake {
                                        Some(existing) => existing.min(entry.wake_at),
                                        None => entry.wake_at,
                                    });
                                }
                            }
                            if let Some(next_wake) = next_wake {
                                blocked_until_by_instance.insert(wake.executor_id, next_wake);
                            }
                        }
                    }
                    if let Some(shard_idx) = executor_shards.get(&wake.executor_id).copied() {
                        by_shard.entry(shard_idx).or_default().push(wake.node_id);
                    }
                }
                for (shard_idx, nodes) in by_shard {
                    if let Some(sender) = shard_senders.get(shard_idx) {
                        let _ = sender.send(ShardCommand::Wake(nodes));
                    }
                }
            }

            let had_instances = !all_instances.is_empty();
            if had_instances {
                instances_idle = false;
                if let Err(err) = self.hydrate_instances(&mut all_instances).await {
                    run_result = Err(err);
                    break;
                }
                debug!(count = all_instances.len(), "hydrated queued instances");
                let mut by_shard: HashMap<usize, Vec<QueuedInstance>> = HashMap::new();
                let mut claimed_instance_ids = Vec::with_capacity(all_instances.len());
                let mut replaced_instance_ids = Vec::new();
                for instance in all_instances {
                    let shard_idx = if let Some(existing_shard_idx) =
                        executor_shards.get(&instance.instance_id).copied()
                    {
                        // If an already-active instance reappears from the queue, treat
                        // the prior in-memory executor as stale and replace it.
                        replaced_instance_ids.push(instance.instance_id);
                        inflight_actions.insert(instance.instance_id, 0);
                        if let Some(nodes) = sleeping_by_instance.remove(&instance.instance_id) {
                            for node_id in nodes {
                                sleeping_nodes.remove(&node_id);
                            }
                        }
                        blocked_until_by_instance.remove(&instance.instance_id);
                        existing_shard_idx
                    } else {
                        let shard_idx = next_shard % self.shard_count;
                        next_shard = next_shard.wrapping_add(1);
                        executor_shards.insert(instance.instance_id, shard_idx);
                        inflight_actions.insert(instance.instance_id, 0);
                        shard_idx
                    };
                    claimed_instance_ids.push(instance.instance_id);
                    by_shard.entry(shard_idx).or_default().push(instance);
                }
                if !replaced_instance_ids.is_empty() {
                    warn!(
                        replaced = replaced_instance_ids.len(),
                        "replacing active executors for reclaimed queued instances"
                    );
                    let replaced_set: HashSet<Uuid> =
                        replaced_instance_ids.iter().copied().collect();
                    inflight_dispatches
                        .retain(|_, dispatch| !replaced_set.contains(&dispatch.executor_id));
                }
                let claimed_count = claimed_instance_ids.len();
                lock_tracker.insert_all(claimed_instance_ids);
                debug!(
                    count = claimed_count,
                    lock_uuid = %lock_tracker.lock_uuid(),
                    "tracked instance locks"
                );
                for (shard_idx, batch) in by_shard {
                    if let Some(sender) = shard_senders.get(shard_idx) {
                        let _ = sender.send(ShardCommand::AssignInstances(batch));
                    }
                }
            } else if saw_empty_instances {
                instances_idle = true;
            }

            let failed_executor_ids: HashSet<Uuid> = all_failed_instances
                .iter()
                .map(|instance_done| instance_done.executor_id)
                .collect();
            if !all_failed_instances.is_empty() {
                for instance_done in all_failed_instances {
                    warn!(
                        executor_id = %instance_done.executor_id,
                        error = ?instance_done.error,
                        "marking instance as failed after shard execution error"
                    );
                    executor_shards.remove(&instance_done.executor_id);
                    inflight_actions.remove(&instance_done.executor_id);
                    inflight_dispatches
                        .retain(|_, dispatch| dispatch.executor_id != instance_done.executor_id);
                    lock_tracker.remove_all([instance_done.executor_id]);
                    if let Some(nodes) = sleeping_by_instance.remove(&instance_done.executor_id) {
                        for node_id in nodes {
                            sleeping_nodes.remove(&node_id);
                        }
                    }
                    blocked_until_by_instance.remove(&instance_done.executor_id);
                    instances_done_pending.push(instance_done);
                }
            }

            if !all_steps.is_empty() {
                let lock_statuses = match self.persist_shard_steps(&all_steps).await {
                    Ok(statuses) => statuses,
                    Err(err) => {
                        run_result = Err(err);
                        break;
                    }
                };
                let evict_ids = self.lock_mismatches(&lock_statuses);
                if !evict_ids.is_empty()
                    && let Err(err) = self
                        .evict_instances(
                            &evict_ids.iter().copied().collect::<Vec<_>>(),
                            &mut EvictionState {
                                executor_shards: &mut executor_shards,
                                shard_senders: &shard_senders,
                                lock_tracker: &lock_tracker,
                                inflight_actions: &mut inflight_actions,
                                inflight_dispatches: &mut inflight_dispatches,
                                sleeping_nodes: &mut sleeping_nodes,
                                sleeping_by_instance: &mut sleeping_by_instance,
                                blocked_until_by_instance: &mut blocked_until_by_instance,
                            },
                        )
                        .await
                {
                    run_result = Err(err);
                    break;
                }
                for step in all_steps {
                    if failed_executor_ids.contains(&step.executor_id) {
                        continue;
                    }
                    if evict_ids.contains(&step.executor_id) {
                        continue;
                    }
                    for request in step.actions {
                        let dispatch = request.clone();
                        if let Err(err) = self.worker_pool.queue(request) {
                            run_result = Err(err.into());
                            break;
                        }
                        *inflight_actions.entry(step.executor_id).or_insert(0) += 1;
                        let deadline_at = if dispatch.timeout_seconds > 0 {
                            Some(
                                Utc::now()
                                    + chrono::Duration::seconds(i64::from(
                                        dispatch.timeout_seconds,
                                    )),
                            )
                        } else {
                            None
                        };
                        inflight_dispatches.insert(
                            dispatch.execution_id,
                            InflightActionDispatch {
                                executor_id: dispatch.executor_id,
                                attempt_number: dispatch.attempt_number,
                                dispatch_token: dispatch.dispatch_token,
                                timeout_seconds: dispatch.timeout_seconds,
                                deadline_at,
                            },
                        );
                    }
                    if run_result.is_err() {
                        break;
                    }
                    for mut sleep_request in step.sleep_requests {
                        if self.skip_sleep {
                            sleep_request.wake_at = Utc::now();
                        }
                        let existing = sleeping_nodes.get(&sleep_request.node_id);
                        let should_update = match existing {
                            Some(existing) => sleep_request.wake_at < existing.wake_at,
                            None => true,
                        };
                        let wake_at = match existing {
                            Some(existing) if !should_update => existing.wake_at,
                            _ => sleep_request.wake_at,
                        };
                        sleeping_by_instance
                            .entry(step.executor_id)
                            .or_default()
                            .insert(sleep_request.node_id);
                        blocked_until_by_instance
                            .entry(step.executor_id)
                            .and_modify(|existing| {
                                if wake_at < *existing {
                                    *existing = wake_at;
                                }
                            })
                            .or_insert(wake_at);

                        if should_update {
                            sleeping_nodes.insert(sleep_request.node_id, sleep_request.clone());
                            let sleep_tx = sleep_tx.clone();
                            let executor_id = step.executor_id;
                            let node_id = sleep_request.node_id;
                            let wake_at = sleep_request.wake_at;
                            tokio::spawn(async move {
                                if let Ok(wait) = wake_at.signed_duration_since(Utc::now()).to_std()
                                    && wait > Duration::ZERO
                                {
                                    tokio::time::sleep(wait).await;
                                }
                                let _ = sleep_tx.send(SleepWake {
                                    executor_id,
                                    node_id,
                                });
                            });
                        }
                    }
                    if let Some(instance_done) = step.instance_done {
                        executor_shards.remove(&instance_done.executor_id);
                        inflight_actions.remove(&instance_done.executor_id);
                        inflight_dispatches.retain(|_, dispatch| {
                            dispatch.executor_id != instance_done.executor_id
                        });
                        lock_tracker.remove_all([instance_done.executor_id]);
                        if let Some(nodes) = sleeping_by_instance.remove(&instance_done.executor_id)
                        {
                            for node_id in nodes {
                                sleeping_nodes.remove(&node_id);
                            }
                        }
                        blocked_until_by_instance.remove(&instance_done.executor_id);
                        instances_done_pending.push(instance_done);
                    }
                }
                if run_result.is_err() {
                    break;
                }
            }

            if !blocked_until_by_instance.is_empty() {
                let now = Utc::now();
                let evict_ids: Vec<Uuid> = blocked_until_by_instance
                    .iter()
                    .filter_map(|(instance_id, wake_at)| {
                        let inflight = inflight_actions.get(instance_id).copied().unwrap_or(0);
                        if inflight > 0 {
                            return None;
                        }
                        let sleep_for = wake_at.signed_duration_since(now).to_std().ok()?;
                        if sleep_for > self.evict_sleep_threshold {
                            Some(*instance_id)
                        } else {
                            None
                        }
                    })
                    .collect();
                if !evict_ids.is_empty()
                    && let Err(err) = self
                        .evict_instances(
                            &evict_ids,
                            &mut EvictionState {
                                executor_shards: &mut executor_shards,
                                shard_senders: &shard_senders,
                                lock_tracker: &lock_tracker,
                                inflight_actions: &mut inflight_actions,
                                inflight_dispatches: &mut inflight_dispatches,
                                sleeping_nodes: &mut sleeping_nodes,
                                sleeping_by_instance: &mut sleeping_by_instance,
                                blocked_until_by_instance: &mut blocked_until_by_instance,
                            },
                        )
                        .await
                {
                    run_result = Err(err);
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
        stop.store(true, Ordering::SeqCst);
        stop_notify.notify_waiters();
        let _ = completion_handle.await;
        let _ = instance_handle.await;
        let _ = lock_handle.await;
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

/// Supervise a run loop, restarting on errors until shutdown.
pub async fn runloop_supervisor<B, W>(
    backend: B,
    worker_pool: W,
    config: RunLoopSupervisorConfig,
    shutdown_rx: watch::Receiver<bool>,
) where
    B: CoreBackend + WorkflowRegistryBackend + Clone + Send + Sync + 'static,
    W: BaseWorkerPool + Clone + Send + Sync + 'static,
{
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(5);

    let poll_interval = config.poll_interval;

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        info!(
            max_concurrent_instances = config.max_concurrent_instances,
            executor_shards = config.executor_shards,
            poll_interval_ms = config.poll_interval.as_millis(),
            lock_uuid = %config.lock_uuid,
            "runloop starting"
        );
        let mut runloop = RunLoop::new_with_shutdown(
            worker_pool.clone(),
            backend.clone(),
            config.clone(),
            shutdown_rx.clone(),
        );

        let result = runloop.run().await;

        if *shutdown_rx.borrow() {
            break;
        }

        match result {
            Ok(_) => {
                warn!("runloop exited cleanly (unexpected); restarting");
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

fn action_timeout_value(execution_id: Uuid, attempt_number: u32, timeout_seconds: u32) -> Value {
    build_synthetic_exception_value(
        SyntheticExceptionType::ActionTimeout,
        format!(
            "action {execution_id} attempt {attempt_number} timed out after {timeout_seconds}s"
        ),
        vec![
            (
                "timeout_seconds".to_string(),
                Value::Number(serde_json::Number::from(timeout_seconds)),
            ),
            (
                "attempt".to_string(),
                Value::Number(serde_json::Number::from(attempt_number)),
            ),
        ],
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use prost::Message;
    use sha2::{Digest, Sha256};

    use crate::backends::{
        ActionAttemptStatus, MemoryBackend, WorkflowRegistration, WorkflowRegistryBackend,
    };
    use crate::messages::ast as ir;
    use crate::waymark_core::dag::convert_to_dag;
    use crate::waymark_core::ir_parser::parse_program;
    use crate::waymark_core::runner::RunnerState;
    use crate::waymark_core::runner::state::NodeStatus;
    use crate::workers::ActionCallable;

    #[tokio::test]
    async fn test_runloop_executes_actions() {
        let source = r#"
fn main(input: [x], output: [y]):
    y = @tests.fixtures.test_actions.double(value=x)
    return y
"#;
        let program = parse_program(source.trim()).expect("parse program");
        let program_proto = program.encode_to_vec();
        let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
        let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

        let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
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
        let entry_node = dag
            .entry_node
            .as_ref()
            .expect("DAG entry node not found")
            .clone();
        let entry_exec = state
            .queue_template_node(&entry_node, None)
            .expect("queue entry node");

        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let backend = MemoryBackend::with_queue(queue.clone());
        let workflow_version_id = backend
            .upsert_workflow_version(&WorkflowRegistration {
                workflow_name: "test".to_string(),
                workflow_version: ir_hash.clone(),
                ir_hash,
                program_proto,
                concurrent: false,
            })
            .await
            .expect("register workflow version");

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
            RunLoopSupervisorConfig {
                max_concurrent_instances: 25,
                executor_shards: 1,
                instance_done_batch_size: None,
                poll_interval: Duration::from_secs_f64(0.0),
                persistence_interval: Duration::from_secs_f64(0.1),
                lock_uuid: Uuid::new_v4(),
                lock_ttl: Duration::from_secs(15),
                lock_heartbeat: Duration::from_secs(5),
                evict_sleep_threshold: Duration::from_secs(10),
                skip_sleep: false,
                active_instance_gauge: None,
            },
        );
        queue.lock().expect("queue lock").push_back(QueuedInstance {
            workflow_version_id,
            schedule_id: None,
            dag: None,
            entry_node: entry_exec.node_id,
            state: Some(state),
            action_results: HashMap::new(),
            instance_id: Uuid::new_v4(),
            scheduled_at: None,
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

    #[tokio::test]
    async fn test_runloop_times_out_action_and_persists_timestamps() {
        let source = r#"
fn main(input: [], output: [y]):
    y = @tests.fixtures.test_actions.hang()[timeout: 1 s]
    return y
"#;
        let program = parse_program(source.trim()).expect("parse program");
        let program_proto = program.encode_to_vec();
        let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
        let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

        let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
        let entry_node = dag
            .entry_node
            .as_ref()
            .expect("DAG entry node not found")
            .clone();
        let entry_exec = state
            .queue_template_node(&entry_node, None)
            .expect("queue entry node");

        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let backend = MemoryBackend::with_queue(queue.clone());
        let workflow_version_id = backend
            .upsert_workflow_version(&WorkflowRegistration {
                workflow_name: "test_timeout".to_string(),
                workflow_version: ir_hash.clone(),
                ir_hash,
                program_proto,
                concurrent: false,
            })
            .await
            .expect("register workflow version");

        let mut actions: HashMap<String, ActionCallable> = HashMap::new();
        actions.insert(
            "hang".to_string(),
            Arc::new(|_kwargs| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    Ok(Value::String("late".to_string()))
                })
            }),
        );
        let worker_pool = crate::workers::InlineWorkerPool::new(actions);

        let mut runloop = RunLoop::new(
            worker_pool,
            backend.clone(),
            RunLoopSupervisorConfig {
                max_concurrent_instances: 25,
                executor_shards: 1,
                instance_done_batch_size: None,
                poll_interval: Duration::from_secs_f64(0.0),
                persistence_interval: Duration::from_secs_f64(0.05),
                lock_uuid: Uuid::new_v4(),
                lock_ttl: Duration::from_secs(15),
                lock_heartbeat: Duration::from_secs(5),
                evict_sleep_threshold: Duration::from_secs(10),
                skip_sleep: false,
                active_instance_gauge: None,
            },
        );
        queue.lock().expect("queue lock").push_back(QueuedInstance {
            workflow_version_id,
            schedule_id: None,
            dag: None,
            entry_node: entry_exec.node_id,
            state: Some(state),
            action_results: HashMap::new(),
            instance_id: Uuid::new_v4(),
            scheduled_at: None,
        });

        runloop.run().await.expect("runloop");

        let actions_done = backend.actions_done();
        assert_eq!(actions_done.len(), 1);
        let action_done = &actions_done[0];
        assert_eq!(action_done.status, ActionAttemptStatus::TimedOut);
        assert!(action_done.started_at.is_some());
        assert!(action_done.completed_at.is_some());
        assert!(action_done.duration_ms.is_some());

        let execution_id = action_done.execution_id;
        let graph_updates = backend.graph_updates();
        let mut saw_running_snapshot = false;
        let mut saw_failed_snapshot = false;
        for update in graph_updates {
            let Some(node) = update.nodes.get(&execution_id) else {
                continue;
            };
            if node.status == NodeStatus::Running && node.started_at.is_some() {
                saw_running_snapshot = true;
            }
            if node.status == NodeStatus::Failed
                && node.started_at.is_some()
                && node.completed_at.is_some()
            {
                saw_failed_snapshot = true;
            }
        }
        assert!(saw_running_snapshot, "expected running graph snapshot");
        assert!(saw_failed_snapshot, "expected failed graph snapshot");

        let instances_done = backend.instances_done();
        assert_eq!(instances_done.len(), 1);
        assert!(instances_done[0].result.is_none());
        let Value::Object(error_obj) = instances_done[0]
            .error
            .clone()
            .expect("instance error payload")
        else {
            panic!("expected error payload object");
        };
        assert_eq!(
            error_obj.get("type"),
            Some(&Value::String("ActionTimeout".to_string()))
        );
    }

    #[tokio::test]
    async fn test_runloop_marks_instance_failed_on_executor_error() {
        let source = r#"
fn main(input: [x], output: [y]):
    y = @tests.fixtures.test_actions.double(value=x)
    return y
"#;
        let program = parse_program(source.trim()).expect("parse program");
        let program_proto = program.encode_to_vec();
        let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
        let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

        // Intentionally omit input assignment so action kwarg resolution fails at runtime.
        let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
        let entry_node = dag
            .entry_node
            .as_ref()
            .expect("DAG entry node not found")
            .clone();
        let entry_exec = state
            .queue_template_node(&entry_node, None)
            .expect("queue entry node");

        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let backend = MemoryBackend::with_queue(queue.clone());
        let workflow_version_id = backend
            .upsert_workflow_version(&WorkflowRegistration {
                workflow_name: "test".to_string(),
                workflow_version: ir_hash.clone(),
                ir_hash,
                program_proto,
                concurrent: false,
            })
            .await
            .expect("register workflow version");

        let worker_pool = crate::workers::InlineWorkerPool::new(HashMap::new());
        let mut runloop = RunLoop::new(
            worker_pool,
            backend.clone(),
            RunLoopSupervisorConfig {
                max_concurrent_instances: 25,
                executor_shards: 1,
                instance_done_batch_size: None,
                poll_interval: Duration::from_secs_f64(0.0),
                persistence_interval: Duration::from_secs_f64(0.1),
                lock_uuid: Uuid::new_v4(),
                lock_ttl: Duration::from_secs(15),
                lock_heartbeat: Duration::from_secs(5),
                evict_sleep_threshold: Duration::from_secs(10),
                skip_sleep: false,
                active_instance_gauge: None,
            },
        );
        let instance_id = Uuid::new_v4();
        queue.lock().expect("queue lock").push_back(QueuedInstance {
            workflow_version_id,
            schedule_id: None,
            dag: None,
            entry_node: entry_exec.node_id,
            state: Some(state),
            action_results: HashMap::new(),
            instance_id,
            scheduled_at: None,
        });

        runloop
            .run()
            .await
            .expect("runloop should continue after instance failure");
        let instances_done = backend.instances_done();
        assert_eq!(instances_done.len(), 1);

        let done = &instances_done[0];
        assert_eq!(done.executor_id, instance_id);
        assert!(done.result.is_none());
        let error = done.error.as_ref().expect("instance error");
        let Value::Object(error_obj) = error else {
            panic!("expected error payload object");
        };
        assert_eq!(
            error_obj.get("type"),
            Some(&Value::String("ExecutionError".to_string()))
        );
        let message = error_obj
            .get("message")
            .and_then(Value::as_str)
            .expect("error message");
        assert!(message.contains("variable not found: x"));
    }

    #[tokio::test]
    async fn test_runloop_executes_for_loop_action_assignments() {
        let source = r#"
fn main(input: [limit], output: [result]):
    current = 0
    iterations = 0
    for _ in range(limit):
        current = @tests.fixtures.test_actions.increment(value=current)
        iterations = iterations + 1
    result = @tests.fixtures.test_actions.pack(limit=limit, final=current, iterations=iterations)
    return result
"#;
        let program = parse_program(source.trim()).expect("parse program");
        let program_proto = program.encode_to_vec();
        let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
        let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

        let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
        let _ = state
            .record_assignment(
                vec!["limit".to_string()],
                &ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(4)),
                    })),
                    span: None,
                },
                None,
                Some("input limit = 4".to_string()),
            )
            .expect("record assignment");
        let entry_node = dag
            .entry_node
            .as_ref()
            .expect("DAG entry node not found")
            .clone();
        let entry_exec = state
            .queue_template_node(&entry_node, None)
            .expect("queue entry node");

        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let backend = MemoryBackend::with_queue(queue.clone());
        let workflow_version_id = backend
            .upsert_workflow_version(&WorkflowRegistration {
                workflow_name: "test_loop_actions".to_string(),
                workflow_version: ir_hash.clone(),
                ir_hash,
                program_proto,
                concurrent: false,
            })
            .await
            .expect("register workflow version");

        let mut actions: HashMap<String, ActionCallable> = HashMap::new();
        let increment_inputs = Arc::new(Mutex::new(Vec::new()));
        let increment_inputs_clone = Arc::clone(&increment_inputs);
        actions.insert(
            "increment".to_string(),
            Arc::new(move |kwargs| {
                let increment_inputs = Arc::clone(&increment_inputs_clone);
                Box::pin(async move {
                    let value = kwargs
                        .get("value")
                        .and_then(|value| value.as_i64())
                        .unwrap_or(0);
                    increment_inputs
                        .lock()
                        .expect("increment inputs lock")
                        .push(value);
                    Ok(Value::Number((value + 1).into()))
                })
            }),
        );
        actions.insert(
            "pack".to_string(),
            Arc::new(|kwargs| {
                Box::pin(async move {
                    let limit = kwargs.get("limit").cloned().unwrap_or(Value::Null);
                    let final_value = kwargs.get("final").cloned().unwrap_or(Value::Null);
                    let iterations = kwargs.get("iterations").cloned().unwrap_or(Value::Null);
                    Ok(Value::Object(
                        [
                            ("limit".to_string(), limit),
                            ("final".to_string(), final_value),
                            ("iterations".to_string(), iterations),
                        ]
                        .into_iter()
                        .collect(),
                    ))
                })
            }),
        );
        let worker_pool = crate::workers::InlineWorkerPool::new(actions);

        let mut runloop = RunLoop::new(
            worker_pool,
            backend.clone(),
            RunLoopSupervisorConfig {
                max_concurrent_instances: 25,
                executor_shards: 1,
                instance_done_batch_size: None,
                poll_interval: Duration::from_secs_f64(0.0),
                persistence_interval: Duration::from_secs_f64(0.1),
                lock_uuid: Uuid::new_v4(),
                lock_ttl: Duration::from_secs(15),
                lock_heartbeat: Duration::from_secs(5),
                evict_sleep_threshold: Duration::from_secs(10),
                skip_sleep: false,
                active_instance_gauge: None,
            },
        );
        queue.lock().expect("queue lock").push_back(QueuedInstance {
            workflow_version_id,
            schedule_id: None,
            dag: None,
            entry_node: entry_exec.node_id,
            state: Some(state),
            action_results: HashMap::new(),
            instance_id: Uuid::new_v4(),
            scheduled_at: None,
        });

        runloop.run().await.expect("runloop");
        let instances_done = backend.instances_done();
        assert_eq!(instances_done.len(), 1);
        let done = &instances_done[0];
        let output = done.result.clone().expect("instance result");
        let Value::Object(map) = output else {
            panic!("expected output object");
        };
        let Value::Object(result_map) = map
            .get("result")
            .cloned()
            .expect("result payload should include result")
        else {
            panic!("expected nested result object");
        };
        assert_eq!(
            *increment_inputs.lock().expect("increment inputs lock"),
            vec![0, 1, 2, 3]
        );
        assert_eq!(result_map.get("limit"), Some(&Value::Number(4.into())));
        assert_eq!(result_map.get("final"), Some(&Value::Number(4.into())));
        assert_eq!(result_map.get("iterations"), Some(&Value::Number(4.into())));
    }

    #[tokio::test]
    async fn test_instance_poller_send_unblocks_on_stop_notification() {
        let (instance_tx, mut instance_rx) = mpsc::channel::<InstanceMessage>(1);
        instance_tx
            .send(InstanceMessage::Batch {
                instances: Vec::new(),
            })
            .await
            .expect("seed channel");

        let stop_notify = Arc::new(Notify::new());
        let send_task = tokio::spawn({
            let instance_tx = instance_tx.clone();
            let stop_notify = Arc::clone(&stop_notify);
            async move {
                send_instance_message_with_stop(
                    &instance_tx,
                    InstanceMessage::Batch {
                        instances: Vec::new(),
                    },
                    &stop_notify,
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        stop_notify.notify_waiters();
        let sent = tokio::time::timeout(Duration::from_millis(300), send_task)
            .await
            .expect("send task should complete")
            .expect("send task should not panic");
        assert!(!sent, "send should abort when stop is notified");

        let _ = instance_rx.recv().await;
    }

    #[tokio::test]
    async fn test_instance_poller_send_succeeds_when_channel_has_capacity() {
        let (instance_tx, mut instance_rx) = mpsc::channel::<InstanceMessage>(1);
        let stop_notify = Notify::new();
        let sent = send_instance_message_with_stop(
            &instance_tx,
            InstanceMessage::Batch {
                instances: Vec::new(),
            },
            &stop_notify,
        )
        .await;
        assert!(sent);

        let received = instance_rx.recv().await.expect("queued message");
        match received {
            InstanceMessage::Batch { instances } => assert!(instances.is_empty()),
            InstanceMessage::Error(err) => panic!("unexpected error message: {err}"),
        }
    }
}
