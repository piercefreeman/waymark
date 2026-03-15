use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use tracing::warn;
use uuid::Uuid;
use waymark_core_backend::{InstanceDone, InstanceLockStatus};
use waymark_runner::SleepRequest;
use waymark_worker_core::ActionCompletion;

use crate::{
    commit_barrier::{CommitBarrier, DeferredInstanceEvent},
    instance_lock_heartbeat, persist,
    runloop::{InflightActionDispatch, SleepWake},
    shard,
};

#[cfg(test)]
mod tests;

// TODO: use proper semantic errors here.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("steps persted: {0}")]
    StepsPersisted(StepsPersistedError),

    #[error("steps persist failed: {0}")]
    StepsPersistFailed(String),
}

pub struct Params<'a, CoreBackend: ?Sized, WorkerPool: ?Sized> {
    /// Maps each active instance/executor to the shard currently responsible for it.
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    /// Per-shard command channels used to send follow-up completions, wakes, and evictions.
    pub shard_senders: &'a [std::sync::mpsc::Sender<shard::Command>],
    /// Tracks which backend locks this runloop currently believes it owns.
    pub lock_tracker: &'a instance_lock_heartbeat::Tracker,
    /// Counts how many action executions are still outstanding for each executor.
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    /// Tracks the currently valid dispatch token/attempt for each inflight action execution.
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    /// Active sleep requests keyed by execution node so confirmed steps can register and clean sleeps.
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    /// Reverse index of sleeping node IDs by executor for sleep bookkeeping and cleanup.
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    /// Earliest wake time currently blocking each executor from making progress.
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    /// Tracks deferred instance events that are released once persistence succeeds.
    pub commit_barrier: &'a mut CommitBarrier<shard::Step>,
    /// Buffer of terminal instance outcomes that still need durable persistence.
    pub instances_done_pending: &'a mut Vec<InstanceDone>,
    /// Channel used to enqueue future wake notifications for sleeping nodes.
    pub sleep_tx: &'a tokio::sync::mpsc::UnboundedSender<SleepWake>,
    /// Backend used for lock release and other persistence-side follow-up work.
    pub core_backend: &'a CoreBackend,
    /// Worker pool used to dispatch actions from newly confirmed shard steps.
    pub worker_pool: &'a WorkerPool,
    /// Lock owner ID for this runloop, used to validate persist acknowledgments.
    pub lock_uuid: Uuid,
    /// Test/debug knob that forces sleeps to wake immediately.
    pub skip_sleep: bool,
    /// Persist acknowledgments collected during the current coordinator tick.
    pub all_persist_acks: Vec<persist::Ack>,
}

/// Processes persistence acknowledgments and unblocks deferred instance events.
///
/// **Why this part exists:** When steps are persisted to the backend, the persist task
/// emits acks back to the runloop. These acks signal that the graph state is durable
/// and previously deferred events (completions, wakes) can now be safe to process.
///
/// **What it does:**
/// - Receives persistence acknowledgments for batches of instances
/// - Collects any deferred completions and wakes that were staged during blocking
/// - Immediately routes them back to their handlers (completions/wakes parts)
/// - Maintains the invariant that no event is lost due to a crash:
///   deferred events are either persisted or deferred again until the next persist ack
pub async fn handle<CoreBackend, WorkerPool>(
    params: Params<'_, CoreBackend, WorkerPool>,
) -> Result<(), Error>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
    WorkerPool: ?Sized + waymark_worker_core::BaseWorkerPool,
{
    let Params {
        executor_shards,
        shard_senders,
        lock_tracker,
        inflight_actions,
        inflight_dispatches,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
        instances_done_pending,
        sleep_tx,
        core_backend,
        worker_pool,
        lock_uuid,
        skip_sleep,
        all_persist_acks,
    } = params;

    for ack in all_persist_acks {
        match ack {
            persist::Ack::StepsPersisted {
                batch_id,
                lock_statuses,
            } => {
                let Some(batch) = commit_barrier.take_batch(batch_id) else {
                    warn!(batch_id, "received ack for unknown persist batch");
                    continue;
                };

                let params = StepsPersistedParams {
                    executor_shards,
                    shard_senders,
                    lock_tracker,
                    inflight_actions,
                    inflight_dispatches,
                    sleeping_nodes,
                    sleeping_by_instance,
                    blocked_until_by_instance,
                    commit_barrier,
                    instances_done_pending,
                    sleep_tx,
                    core_backend,
                    worker_pool,
                    lock_uuid,
                    skip_sleep,
                    batch,
                    lock_statuses,
                };
                handle_steps_persisted(params)
                    .await
                    .map_err(Error::StepsPersisted)?;
            }
            persist::Ack::StepsPersistFailed { batch_id, error } => {
                warn!(batch_id, error = %error, "persist step batch failed");
                return Err(Error::StepsPersistFailed(error));
            }
        }
    }
    Ok(())
}

struct StepsPersistedParams<'a, CoreBackend: ?Sized, WorkerPool: ?Sized> {
    /// Maps each active instance/executor to the shard currently responsible for it.
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    /// Per-shard command channels used to send follow-up completions, wakes, and evictions.
    pub shard_senders: &'a [std::sync::mpsc::Sender<shard::Command>],
    /// Tracks which backend locks this runloop currently believes it owns.
    pub lock_tracker: &'a instance_lock_heartbeat::Tracker,
    /// Counts how many action executions are still outstanding for each executor.
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    /// Tracks the currently valid dispatch token/attempt for each inflight action execution.
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    /// Active sleep requests keyed by execution node so confirmed steps can register and clean sleeps.
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    /// Reverse index of sleeping node IDs by executor for sleep bookkeeping and cleanup.
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    /// Earliest wake time currently blocking each executor from making progress.
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    /// Tracks deferred instance events that are released once persistence succeeds.
    pub commit_barrier: &'a mut CommitBarrier<shard::Step>,
    /// Buffer of terminal instance outcomes that still need durable persistence.
    pub instances_done_pending: &'a mut Vec<InstanceDone>,
    /// Channel used to enqueue future wake notifications for sleeping nodes.
    pub sleep_tx: &'a tokio::sync::mpsc::UnboundedSender<SleepWake>,
    /// Backend used for lock release and other persistence-side follow-up work.
    pub core_backend: &'a CoreBackend,
    /// Worker pool used to dispatch actions from newly confirmed shard steps.
    pub worker_pool: &'a WorkerPool,
    /// Lock owner ID for this runloop, used to validate persist acknowledgments.
    pub lock_uuid: Uuid,
    /// Test/debug knob that forces sleeps to wake immediately.
    pub skip_sleep: bool,
    /// Persisted batch that just became durable and can now be applied.
    pub batch: crate::commit_barrier::PendingPersistBatch<shard::Step>,
    /// Lock ownership results returned by the persistence task for the persisted batch.
    pub lock_statuses: Vec<InstanceLockStatus>,
}

#[derive(Debug, thiserror::Error)]
pub enum StepsPersistedError {
    #[error("evict instances: {0}")]
    EvictInstances(#[source] waymark_backends_core::BackendError),

    #[error("apply confirmed step: {0}")]
    ApplyConfirmedStep(#[source] waymark_worker_core::WorkerPoolError),
}

async fn handle_steps_persisted<CoreBackend, WorkerPool>(
    params: StepsPersistedParams<'_, CoreBackend, WorkerPool>,
) -> Result<(), StepsPersistedError>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
    WorkerPool: ?Sized + waymark_worker_core::BaseWorkerPool,
{
    let StepsPersistedParams {
        executor_shards,
        shard_senders,
        lock_tracker,
        inflight_actions,
        inflight_dispatches,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
        instances_done_pending,
        sleep_tx,
        core_backend,
        worker_pool,
        lock_uuid,
        skip_sleep,
        batch,
        lock_statuses,
    } = params;

    let evict_ids: HashSet<Uuid> =
        crate::runloop::lock_utils::lock_mismatches_for(&lock_statuses, lock_uuid)
            .into_iter()
            .filter(|instance_id| batch.instance_ids.contains(instance_id))
            .collect();

    if !evict_ids.is_empty() {
        let evict_ids_vec = evict_ids.iter().copied().collect::<Vec<_>>();
        let params = super::ops::evict_instances::Params {
            executor_shards,
            shard_senders,
            lock_tracker,
            inflight_actions,
            inflight_dispatches,
            sleeping_nodes,
            sleeping_by_instance,
            blocked_until_by_instance,
            core_backend,
            lock_uuid,
            instance_ids: &evict_ids_vec,
        };

        super::ops::evict_instances::run(params)
            .await
            .map_err(StepsPersistedError::EvictInstances)?;
    }
    for step in batch.steps {
        if !batch.instance_ids.contains(&step.executor_id) {
            continue;
        }
        if evict_ids.contains(&step.executor_id) {
            continue;
        }
        if !executor_shards.contains_key(&step.executor_id) && step.instance_done.is_none() {
            continue;
        }

        let params = super::ops::apply_confirmed_step::Params {
            executor_shards,
            lock_tracker,
            inflight_actions,
            inflight_dispatches,
            sleeping_nodes,
            sleeping_by_instance,
            blocked_until_by_instance,
            commit_barrier,
            instances_done_pending,
            sleep_tx,
            worker_pool,
            skip_sleep,
            step,
        };
        super::ops::apply_confirmed_step::run(params)
            .map_err(StepsPersistedError::ApplyConfirmedStep)?;
    }

    for instance_id in batch.instance_ids {
        if evict_ids.contains(&instance_id) {
            commit_barrier.remove_instance(instance_id);
            continue;
        }
        let events = commit_barrier.unblock_instance(instance_id);
        flush_deferred_instance_events(instance_id, events, executor_shards, shard_senders);
    }

    Ok(())
}

fn flush_deferred_instance_events(
    instance_id: Uuid,
    events: VecDeque<DeferredInstanceEvent>,
    executor_shards: &HashMap<Uuid, usize>,
    shard_senders: &[std::sync::mpsc::Sender<shard::Command>],
) {
    let Some(shard_idx) = executor_shards.get(&instance_id).copied() else {
        return;
    };
    let Some(sender) = shard_senders.get(shard_idx) else {
        return;
    };
    let mut completion_batch: Vec<ActionCompletion> = Vec::new();
    let mut wake_batch: Vec<Uuid> = Vec::new();
    for event in events {
        match event {
            DeferredInstanceEvent::Completion(completion) => {
                if !wake_batch.is_empty() {
                    let _ = sender.send(shard::Command::Wake(std::mem::take(&mut wake_batch)));
                }
                completion_batch.push(completion);
            }
            DeferredInstanceEvent::Wake(node_id) => {
                if !completion_batch.is_empty() {
                    let _ = sender.send(shard::Command::ActionCompletions(std::mem::take(
                        &mut completion_batch,
                    )));
                }
                wake_batch.push(node_id);
            }
        }
    }
    if !completion_batch.is_empty() {
        let _ = sender.send(shard::Command::ActionCompletions(completion_batch));
    }
    if !wake_batch.is_empty() {
        let _ = sender.send(shard::Command::Wake(wake_batch));
    }
}
