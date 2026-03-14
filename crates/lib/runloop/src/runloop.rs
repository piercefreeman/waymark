//! Runloop for coordinating executors and worker pools.

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc as std_mpsc,
};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use waymark_backends_core::BackendError;
use waymark_core_backend::{InstanceDone, LockClaim, QueuedInstance, QueuedInstanceBatch};
use waymark_workflow_registry_backend::WorkflowRegistryBackend;

use crate::commit_barrier::CommitBarrier;
use crate::lock::{InstanceLockTracker, spawn_lock_heartbeat};
use crate::runloop::channel_utils::send_with_stop;
use crate::{error_value, persist, shard};

use waymark_dag::DAG;
use waymark_observability::obs;
use waymark_runner::{RunnerExecutorError, SleepRequest};
use waymark_worker_core::{ActionCompletion, BaseWorkerPool, WorkerPoolError};

#[cfg(test)]
mod test_support;

mod parts {
    use super::ops;

    pub mod completions;
    pub mod deferred_instances;
    pub mod failed_instances;
    pub mod inflight_dispatches;
    pub mod new_instances;
    pub mod step_persist_acks;
    pub mod steps;
    pub mod wakes;
}

mod ops {
    pub mod apply_confirmed_step;
    pub mod evict_instances;
    pub mod flush_instances_done;
    pub mod hydrate_instances;
}

mod channel_utils;
mod lock_utils;

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
    Shard(shard::Event),
    SleepWake(SleepWake),
    PersistAck(persist::Ack),
    ActionTimeoutTick,
}

/// Run loop that fans out executor work across CPU-bound shard threads.
pub struct RunLoop {
    worker_pool: Arc<dyn BaseWorkerPool>,
    core_backend: Arc<dyn waymark_core_backend::CoreBackend>,
    registry_backend: Arc<dyn WorkflowRegistryBackend>,
    workflow_cache: HashMap<Uuid, Arc<DAG>>,
    available_instance_slot_tracker: Arc<crate::available_instance_slots::Tracker>,
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
        #[allow(deprecated)]
        let available_instance_slots_calc =
            crate::available_instance_slots::Calc::new_saturating(config.max_concurrent_instances);
        let instance_done_batch_size = available_instance_slots_calc.max_concurrent_instances.get();

        let available_instance_slot_tracker =
            crate::available_instance_slots::Tracker::from_scratch(available_instance_slots_calc);
        let available_instance_slot_tracker = Arc::new(available_instance_slot_tracker);

        let backend = Arc::new(backend);
        let core_backend: Arc<dyn waymark_core_backend::CoreBackend> = backend.clone();
        let registry_backend: Arc<dyn WorkflowRegistryBackend> = backend;

        Self {
            worker_pool: Arc::new(worker_pool),
            core_backend,
            registry_backend,
            workflow_cache: HashMap::new(),
            available_instance_slot_tracker,
            instance_done_batch_size,
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

    fn store_available_instance_slots(&self, active_instances: usize) {
        self.available_instance_slot_tracker
            .update_saturating(active_instances);
        if let Some(gauge) = &self.active_instance_gauge {
            gauge.store(active_instances, Ordering::SeqCst);
        }
    }

    #[obs]
    pub async fn run(&mut self) -> Result<(), RunLoopError> {
        self.worker_pool.launch().await?;

        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<shard::Event>();
        let mut shard_senders: Vec<std_mpsc::Sender<shard::Command>> =
            Vec::with_capacity(self.shard_count);
        let mut shard_handles = Vec::with_capacity(self.shard_count);

        for shard_id in 0..self.shard_count {
            let (cmd_tx, cmd_rx) = std_mpsc::channel();
            let backend = self.core_backend.clone();
            let event_tx = event_tx.clone();
            let handle = thread::Builder::new()
                .name(format!("waymark-executor-{shard_id}"))
                .stack_size(128 * 1024 * 1024 /* 128 MB */)
                .spawn(move || shard::run_executor_shard(shard_id, backend, cmd_rx, event_tx))
                .map_err(|err| {
                    RunLoopError::Message(format!(
                        "failed to spawn executor shard {shard_id}: {err}"
                    ))
                })?;
            shard_senders.push(cmd_tx);
            shard_handles.push(handle);
        }
        drop(event_tx);

        self.store_available_instance_slots(0);

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
        let lock_uuid = self.lock_uuid;
        let lock_ttl = self.lock_ttl;
        let available_instance_slot_tracker = Arc::clone(&self.available_instance_slot_tracker);
        let instance_shutdown_token = self.shutdown_token.clone();
        let instance_handle = tokio::spawn(async move {
            let _instance_shutdown_guard = instance_shutdown_token.drop_guard_ref();
            loop {
                if instance_shutdown_token.is_cancelled() {
                    info!("instance poller stop flag set");
                    break;
                }
                let available_slots = available_instance_slot_tracker.get();
                let Some(batch_size) = std::num::NonZeroUsize::new(available_slots) else {
                    if poll_interval > Duration::ZERO {
                        tokio::time::sleep(poll_interval).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(0)).await;
                    }
                    continue;
                };

                let lock_expires_at = Utc::now()
                    + chrono::Duration::from_std(lock_ttl)
                        .unwrap_or_else(|_| chrono::Duration::seconds(0));
                let batch = backend
                    .get_queued_instances(
                        batch_size.get(), // TODO: switch `get_queued_instances` to `NonZeroUsize`
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

        // TODO: move this initialization out of the runloop
        let (persist_tx, persist_rx) = mpsc::channel::<persist::Command>(64);
        let (persist_ack_tx, mut persist_ack_rx) = mpsc::unbounded_channel::<persist::Ack>();
        let persist_handle = tokio::spawn(persist::r#loop::run(persist::r#loop::Params {
            command_rx: persist_rx,
            ack_tx: persist_ack_tx,
            core_backend: self.core_backend.clone(),
            lock_ttl: self.lock_ttl,
            lock_uuid: self.lock_uuid,
        }));

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
        let mut commit_barrier: CommitBarrier<shard::Step> = CommitBarrier::new();
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
                        let params = ops::flush_instances_done::Params {
                            core_backend: self.core_backend.as_ref(),
                            pending: &mut instances_done_pending,
                        };
                        ops::flush_instances_done::run(params)
                        .await?;
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
            let mut all_steps: Vec<shard::Step> = Vec::new();
            let mut all_failed_instances: Vec<InstanceDone> = Vec::new();
            let mut all_wakes: Vec<SleepWake> = Vec::new();
            let mut all_persist_acks: Vec<persist::Ack> = Vec::new();
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
                    shard::Event::Step(step) => all_steps.push(step),
                    shard::Event::InstanceFailed {
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
                    shard::Event::Step(step) => all_steps.push(step),
                    shard::Event::InstanceFailed {
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
            parts::inflight_dispatches::handle(parts::inflight_dispatches::Params {
                all_completions: &mut all_completions,
                inflight_dispatches: &inflight_dispatches,
            });

            // Handle step persist acks.
            {
                let params = parts::step_persist_acks::Params {
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
                    core_backend: self.core_backend.as_ref(),
                    worker_pool: self.worker_pool.as_ref(),
                    lock_uuid: self.lock_uuid,
                    skip_sleep: self.skip_sleep,
                    all_persist_acks,
                };
                let result = parts::step_persist_acks::handle(params).await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let error = match error {
                        parts::step_persist_acks::Error::StepsPersistFailed(run_loop_error) => {
                            RunLoopError::Message(run_loop_error)
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
                let params = parts::completions::Params {
                    executor_shards: &mut executor_shards,
                    shard_senders: &shard_senders,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    commit_barrier: &mut commit_barrier,
                    all_completions,
                };
                parts::completions::handle(params);
            }

            // Handle all wakes.
            {
                let params = parts::wakes::Params {
                    executor_shards: &mut executor_shards,
                    shard_senders: &shard_senders,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                    all_wakes,
                };
                parts::wakes::handle(params);
            }

            // Handle all instances.
            {
                let params = parts::new_instances::Params {
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
                    instances_idle: &mut instances_idle,
                    next_shard: &mut next_shard,
                    shard_count: self.shard_count,
                    all_instances,
                    saw_empty_instances,
                };

                let result = parts::new_instances::handle(params).await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let parts::new_instances::Error::Hydrate(error) = error;
                    break 'runloop Err(error);
                }
            }

            // Handle failed instances.
            {
                let params = parts::failed_instances::Params {
                    executor_shards: &mut executor_shards,
                    lock_tracker: &lock_tracker,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                    all_failed_instances,
                    instances_done_pending: &mut instances_done_pending,
                };
                parts::failed_instances::handle(params);
            }

            // Handle steps.
            {
                let params = parts::steps::Params {
                    shutdown_signal: shutdown_token.cancelled(),
                    persist_tx: &persist_tx,
                    commit_barrier: &mut commit_barrier,
                    all_steps,
                };
                let result = parts::steps::handle(params).await;
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
                let params = parts::deferred_instances::Params {
                    executor_shards: &mut executor_shards,
                    shard_senders: &mut shard_senders,
                    lock_tracker: &lock_tracker,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                    core_backend: self.core_backend.as_ref(),
                    lock_uuid,
                    evict_sleep_threshold: self.evict_sleep_threshold,
                };
                let result = parts::deferred_instances::handle(params).await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let parts::deferred_instances::Error::EvictInstance(error) = error;
                    break 'runloop Err(error);
                }
            }

            self.store_available_instance_slots(executor_shards.len());

            if instances_done_pending.len() >= self.instance_done_batch_size
                && let Err(err) =
                    ops::flush_instances_done::run(ops::flush_instances_done::Params {
                        core_backend: self.core_backend.as_ref(),
                        pending: &mut instances_done_pending,
                    })
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
            && let Err(err) = ops::flush_instances_done::run(ops::flush_instances_done::Params {
                core_backend: self.core_backend.as_ref(),
                pending: &mut instances_done_pending,
            })
            .await
        {
            run_result = Err(err);
        }

        for sender in shard_senders {
            let _ = sender.send(shard::Command::Shutdown);
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
