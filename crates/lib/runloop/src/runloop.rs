//! Runloop for coordinating executors and worker pools.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc as std_mpsc,
};
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;
use waymark_backends_core::BackendError;
use waymark_core_backend::{InstanceDone, QueuedInstance};
use waymark_nonzero_duration::NonZeroDuration;

use crate::commit_barrier::CommitBarrier;
use crate::instance_lock_heartbeat;
use crate::{error_value, persist, queued_instances_polling, shard};

use waymark_dag::DAG;
use waymark_observability::obs;
use waymark_runner::{RunnerExecutorError, SleepRequest};
use waymark_worker_core::{ActionCompletion, WorkerPoolError};

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

mod lock_utils;

/// Raised when the run loop cannot coordinate execution.
#[derive(Debug, thiserror::Error)]
pub enum Error<CoreBackendPollError> {
    #[error("{0}")]
    Message(String),

    #[error("core backend poll: {0}")]
    CoreBackendPoll(#[source] CoreBackendPollError),

    #[error(transparent)]
    Backend(#[from] BackendError),

    #[error(transparent)]
    WorkerPool(#[from] WorkerPoolError),

    #[error(transparent)]
    RunnerExecutor(RunnerExecutorError),
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

enum CoordinatorEvent<CoreBackendPollError> {
    Completions(Vec<ActionCompletion>),
    Instance(queued_instances_polling::Message<CoreBackendPollError>),
    Shard(shard::Event),
    SleepWake(SleepWake),
    PersistAck(persist::Ack),
    ActionTimeoutTick,
}

/// Run loop that fans out executor work across CPU-bound shard threads.
pub struct RunLoop<WorkerPool, CoreBackend, RegistryBackend>
where
    WorkerPool: ?Sized,
    CoreBackend: ?Sized,
    RegistryBackend: ?Sized,
{
    worker_pool: Arc<WorkerPool>,
    core_backend: Arc<CoreBackend>,
    registry_backend: Arc<RegistryBackend>,
    workflow_cache: HashMap<Uuid, Arc<DAG>>,
    available_instance_slot_tracker: Arc<crate::available_instance_slots::Tracker>,
    instance_done_batch_size: NonZeroUsize,
    poll_interval: Option<NonZeroDuration>,
    persistence_interval: Option<NonZeroDuration>,
    shard_count: NonZeroUsize,
    lock_uuid: Uuid,
    lock_ttl: NonZeroDuration,
    lock_heartbeat: NonZeroDuration,
    evict_sleep_threshold: NonZeroDuration,
    skip_sleep: bool,
    active_instance_gauge: Option<Arc<AtomicUsize>>,
    shutdown_token: tokio_util::sync::CancellationToken,
    exit_on_idle: bool,
}

#[derive(Clone, Debug)]
pub struct RunLoopConfig {
    pub max_concurrent_instances: NonZeroUsize,
    pub executor_shards: NonZeroUsize,
    pub instance_done_batch_size: Option<NonZeroUsize>,
    pub poll_interval: Option<NonZeroDuration>,
    pub persistence_interval: Option<NonZeroDuration>,
    pub lock_uuid: Uuid,
    pub lock_ttl: NonZeroDuration,
    pub lock_heartbeat: NonZeroDuration,
    pub evict_sleep_threshold: NonZeroDuration,
    pub skip_sleep: bool,
    pub active_instance_gauge: Option<Arc<AtomicUsize>>,
}

impl<WorkerPool, Backend> RunLoop<WorkerPool, Backend, Backend>
where
    WorkerPool: ?Sized,
    Backend: ?Sized,
{
    pub fn new(
        worker_pool: impl Into<Arc<WorkerPool>>,
        backend: impl Into<Arc<Backend>>,
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
        worker_pool: impl Into<Arc<WorkerPool>>,
        backend: impl Into<Arc<Backend>>,
        config: RunLoopConfig,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self::new_internal(worker_pool, backend, config, shutdown_token, false)
    }

    fn new_internal(
        worker_pool: impl Into<Arc<WorkerPool>>,
        backend: impl Into<Arc<Backend>>,
        config: RunLoopConfig,
        shutdown_token: tokio_util::sync::CancellationToken,
        exit_on_idle: bool,
    ) -> Self {
        let available_instance_slots_calc = crate::available_instance_slots::Calc {
            max_concurrent_instances: config.max_concurrent_instances,
        };
        let instance_done_batch_size = config
            .instance_done_batch_size
            .unwrap_or(config.max_concurrent_instances);

        let available_instance_slot_tracker =
            crate::available_instance_slots::Tracker::from_scratch(available_instance_slots_calc);
        let available_instance_slot_tracker = Arc::new(available_instance_slot_tracker);

        let worker_pool = worker_pool.into();
        let backend = backend.into();

        // Split the bcakend into multiple values.
        let core_backend = Arc::clone(&backend);
        let registry_backend = backend;

        Self {
            worker_pool,
            core_backend,
            registry_backend,
            workflow_cache: HashMap::new(),
            available_instance_slot_tracker,
            instance_done_batch_size,
            poll_interval: config.poll_interval,
            persistence_interval: config.persistence_interval,
            shard_count: config.executor_shards,
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
}

impl<WorkerPool, CoreBackend, RegistryBackend> RunLoop<WorkerPool, CoreBackend, RegistryBackend>
where
    WorkerPool: ?Sized,
    CoreBackend: ?Sized,
    RegistryBackend: ?Sized,
    WorkerPool: waymark_worker_core::BaseWorkerPool,
    CoreBackend: waymark_core_backend::CoreBackend,
    RegistryBackend: waymark_workflow_registry_backend::WorkflowRegistryBackend,
    // TODO: review after splitting out spawns
    WorkerPool: Send + Sync + 'static,
    CoreBackend: Send + Sync + 'static,
    CoreBackend::PollQueuedInstancesError: core::fmt::Display,
    CoreBackend::PollQueuedInstancesError: core::fmt::Debug,
    CoreBackend::PollQueuedInstancesError: Send + Sync + 'static,
{
    fn store_available_instance_slots(&self, active_instances: usize) {
        self.available_instance_slot_tracker
            .update_saturating(active_instances);
        if let Some(gauge) = &self.active_instance_gauge {
            gauge.store(active_instances, Ordering::SeqCst);
        }
    }

    #[obs]
    pub async fn run(
        &mut self,
    ) -> Result<(), Error<queued_instances_polling::r#loop::BackendErrorFor<CoreBackend>>> {
        self.worker_pool.launch().await.map_err(Error::WorkerPool)?;

        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<shard::Event>();
        let mut shard_senders: Vec<std_mpsc::Sender<shard::Command>> =
            Vec::with_capacity(self.shard_count.get());
        let mut shard_handles = Vec::with_capacity(self.shard_count.get());

        for shard_id in 0..self.shard_count.get() {
            let (cmd_tx, cmd_rx) = std_mpsc::channel();
            let event_tx = event_tx.clone();
            let handle = thread::Builder::new()
                .name(format!("waymark-executor-{shard_id}"))
                .stack_size(128 * 1024 * 1024 /* 128 MB */)
                .spawn(move || shard::run_executor_shard(shard_id, cmd_rx, event_tx))
                .map_err(|err| {
                    Error::Message(format!("failed to spawn executor shard {shard_id}: {err}"))
                })?;
            shard_senders.push(cmd_tx);
            shard_handles.push(handle);
        }
        drop(event_tx);

        self.store_available_instance_slots(0);

        let (completion_tx, mut completion_rx) = mpsc::channel::<Vec<ActionCompletion>>(32);
        let (instance_tx, mut instance_rx) = mpsc::channel::<
            queued_instances_polling::Message<
                queued_instances_polling::r#loop::BackendErrorFor<CoreBackend>,
            >,
        >(16);
        let (sleep_tx, mut sleep_rx) = mpsc::unbounded_channel::<SleepWake>();

        // TODO: move this initialization out of the runloop
        let lock_tracker = instance_lock_heartbeat::Tracker::default();
        let lock_handle = tokio::spawn({
            let shutdown_guard = self.shutdown_token.clone().drop_guard();
            let params = instance_lock_heartbeat::r#loop::Params {
                core_backend: self.core_backend.clone(),
                tracker: lock_tracker.clone(),
                heartbeat_interval: self.lock_heartbeat,
                lock_ttl: self.lock_ttl,
                lock_uuid: self.lock_uuid,
                shutdown_signal: self.shutdown_token.clone().cancelled_owned(),
            };
            async move {
                let _shutdown_guard = shutdown_guard;
                instance_lock_heartbeat::r#loop::run(params).await
            }
        });

        // TODO: move this initialization out of the runloop
        let completion_handle = tokio::spawn({
            let shutdown_guard = self.shutdown_token.clone().drop_guard();
            let params = crate::completions_polling::r#loop::Params {
                shutdown_token: self.shutdown_token.clone(),
                worker_pool: self.worker_pool.clone(),
                completion_tx,
            };
            async move {
                let _shutdown_guard = shutdown_guard;
                crate::completions_polling::r#loop::run(params).await
            }
        });

        // TODO: move this initialization out of the runloop
        let instance_handle = tokio::spawn({
            let shutdown_guard = self.shutdown_token.clone().drop_guard();
            let params = queued_instances_polling::r#loop::Params {
                shutdown_token: self.shutdown_token.clone(),
                core_backend: self.core_backend.clone(),
                available_instance_slots_tracker: Arc::clone(&self.available_instance_slot_tracker),
                poll_interval: self.poll_interval,
                lock_uuid: self.lock_uuid,
                lock_ttl: self.lock_ttl,
                queued_instance_tx: instance_tx,
            };
            async move {
                let _shutdown_guard = shutdown_guard;
                queued_instances_polling::r#loop::run(params).await
            }
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

        let persistence_interval = self
            .persistence_interval
            .unwrap_or(NonZeroDuration::from_secs(3600).unwrap());
        let mut persistence_tick = tokio::time::interval(persistence_interval.get());
        let timeout_scan_interval = self
            .poll_interval
            .map(|poll_interval| {
                std::cmp::max(
                    Duration::from_millis(25),
                    std::cmp::min(poll_interval.get(), Duration::from_millis(250)),
                )
            })
            .unwrap_or(Duration::from_millis(100));
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
                    // TODO: why do we only do this if the `self.persistence_interval`
                    // is set, while we also have a local `persistence_interval` that
                    // is guaranteed to be set?
                    if self.persistence_interval.is_some() {
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
            let mut queued_instances_poller_is_pending = false;

            match first_event {
                CoordinatorEvent::Completions(completions) => {
                    all_completions.extend(completions);
                }
                CoordinatorEvent::Instance(queued_instances_polling::Message::Batch {
                    instances,
                }) => {
                    all_instances.extend(instances);
                }
                CoordinatorEvent::Instance(queued_instances_polling::Message::Pending) => {
                    queued_instances_poller_is_pending = true;
                }
                CoordinatorEvent::Instance(queued_instances_polling::Message::Error(err)) => {
                    warn!(error = %err, "runloop exiting: instance poller backend error");
                    break 'runloop Err(Error::CoreBackendPoll(err));
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
                    queued_instances_polling::Message::Batch { instances } => {
                        all_instances.extend(instances);
                    }
                    queued_instances_polling::Message::Pending => {
                        queued_instances_poller_is_pending = true;
                    }
                    queued_instances_polling::Message::Error(err) => {
                        warn!(error = %err, "runloop exiting: instance poller backend error");
                        break 'runloop Err(Error::CoreBackendPoll(err));
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
                            Error::Message(run_loop_error)
                        }
                        parts::step_persist_acks::Error::StepsPersisted(steps_persisted_error) => {
                            match steps_persisted_error {
                                parts::step_persist_acks::StepsPersistedError::EvictInstances(backend_error) => backend_error.into(),
                                parts::step_persist_acks::StepsPersistedError::ApplyConfirmedStep(worker_pool_error) => worker_pool_error.into(),
                            }
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
            if let Some(all_instances) = NEVec::try_from_vec(all_instances) {
                // Record that we've been processing something this tick.
                instances_idle = false;

                let params = parts::new_instances::Params {
                    executor_shards: &mut executor_shards,
                    shard_senders: &mut shard_senders,
                    lock_tracker: &lock_tracker,
                    lock_uuid: self.lock_uuid,
                    inflight_actions: &mut inflight_actions,
                    inflight_dispatches: &mut inflight_dispatches,
                    sleeping_nodes: &mut sleeping_nodes,
                    sleeping_by_instance: &mut sleeping_by_instance,
                    blocked_until_by_instance: &mut blocked_until_by_instance,
                    commit_barrier: &mut commit_barrier,
                    workflow_cache: &mut self.workflow_cache,
                    registry_backend: self.registry_backend.as_ref(),
                    next_shard: &mut next_shard,
                    shard_count: self.shard_count,
                    all_instances,
                };

                let result = parts::new_instances::handle(params).await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let parts::new_instances::Error::Hydrate(error) = error;
                    let error = match error {
                        ops::hydrate_instances::Error::GetWorkflowVersions(backend_error) => {
                            backend_error.into()
                        }
                        err @ ops::hydrate_instances::Error::IrProgramDecode { .. }
                        | err @ ops::hydrate_instances::Error::ConvertToDag { .. }
                        | err @ ops::hydrate_instances::Error::WorkflowCacheGetNone { .. } => {
                            Error::Message(err.to_string())
                        }
                    };
                    break 'runloop Err(error);
                }
            } else if queued_instances_poller_is_pending {
                // Record that we've been idle this tick.
                instances_idle = true;
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
                            Error::Message(err.to_string())
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
                    lock_uuid: self.lock_uuid,
                    evict_sleep_threshold: self.evict_sleep_threshold,
                };
                let result = parts::deferred_instances::handle(params).await;
                if let Err(error) = result {
                    // TODO: properly expose actual type-safe causal error from
                    // the runloop.
                    // For now we reduce all the extra useful information to just
                    // put the error into the "dumb" unified error.
                    let parts::deferred_instances::Error::EvictInstance(error) = error;
                    break 'runloop Err(error.into());
                }
            }

            self.store_available_instance_slots(executor_shards.len());

            if instances_done_pending.len() >= self.instance_done_batch_size.get()
                && let Err(err) =
                    ops::flush_instances_done::run(ops::flush_instances_done::Params {
                        core_backend: self.core_backend.as_ref(),
                        pending: &mut instances_done_pending,
                    })
                    .await
            {
                break 'runloop Err(err.into());
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
            run_result = Err(err.into());
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
