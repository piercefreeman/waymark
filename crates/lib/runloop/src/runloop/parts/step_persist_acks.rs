use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use tracing::warn;
use uuid::Uuid;
use waymark_core_backend::{InstanceDone, InstanceLockStatus};
use waymark_runner::SleepRequest;
use waymark_worker_core::ActionCompletion;

use crate::{
    commit_barrier::{CommitBarrier, DeferredInstanceEvent},
    lock::InstanceLockTracker,
    runloop::{InflightActionDispatch, PersistAck, ShardCommand, ShardStep, SleepWake},
};

// TODO: use proper semantic errors here.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("steps persted: {0}")]
    StepsPersisted(#[source] crate::RunLoopError),

    #[error("steps persist failed: {0}")]
    StepsPersistFailed(#[source] crate::RunLoopError),
}

pub struct Params<'a, CoreBackend: ?Sized, WorkerPool: ?Sized> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    pub lock_tracker: &'a InstanceLockTracker,
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
    pub instances_done_pending: &'a mut Vec<InstanceDone>,
    pub sleep_tx: &'a tokio::sync::mpsc::UnboundedSender<SleepWake>,
    pub core_backend: &'a CoreBackend,
    pub worker_pool: &'a WorkerPool,
    pub lock_uuid: Uuid,
    pub skip_sleep: bool,
    pub all_persist_acks: Vec<PersistAck>,
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
///   defered events are either persisted or deferred again until the next persist ack
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
            PersistAck::StepsPersisted {
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
            PersistAck::StepsPersistFailed { batch_id, error } => {
                warn!(batch_id, error = %error, "persist step batch failed");
                return Err(Error::StepsPersistFailed(error));
            }
        }
    }
    Ok(())
}

struct StepsPersistedParams<'a, CoreBackend: ?Sized, WorkerPool: ?Sized> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    pub lock_tracker: &'a InstanceLockTracker,
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
    pub instances_done_pending: &'a mut Vec<InstanceDone>,
    pub sleep_tx: &'a tokio::sync::mpsc::UnboundedSender<SleepWake>,
    pub core_backend: &'a CoreBackend,
    pub worker_pool: &'a WorkerPool,
    pub lock_uuid: Uuid,
    pub skip_sleep: bool,
    pub batch: crate::commit_barrier::PendingPersistBatch<ShardStep>,
    pub lock_statuses: Vec<InstanceLockStatus>,
}

async fn handle_steps_persisted<CoreBackend, WorkerPool>(
    params: StepsPersistedParams<'_, CoreBackend, WorkerPool>,
) -> Result<(), crate::RunLoopError>
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

        super::ops::evict_instances::run(params).await?;
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
        super::ops::apply_confirmed_step::run(params)?;
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
    shard_senders: &[std::sync::mpsc::Sender<ShardCommand>],
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
                    let _ = sender.send(ShardCommand::Wake(std::mem::take(&mut wake_batch)));
                }
                completion_batch.push(completion);
            }
            DeferredInstanceEvent::Wake(node_id) => {
                if !completion_batch.is_empty() {
                    let _ = sender.send(ShardCommand::ActionCompletions(std::mem::take(
                        &mut completion_batch,
                    )));
                }
                wake_batch.push(node_id);
            }
        }
    }
    if !completion_batch.is_empty() {
        let _ = sender.send(ShardCommand::ActionCompletions(completion_batch));
    }
    if !wake_batch.is_empty() {
        let _ = sender.send(ShardCommand::Wake(wake_batch));
    }
}
