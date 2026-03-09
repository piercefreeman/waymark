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
pub enum HandleStepPersistAcksError {
    #[error("steps persted: {0}")]
    StepsPersisted(#[source] crate::RunLoopError),

    #[error("steps persist failed: {0}")]
    StepsPersistFailed(#[source] crate::RunLoopError),
}

pub struct HandleStepPersistAcksContext<'a> {
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
}

pub async fn handle_step_persist_acks<CoreBackend, WorkerPool>(
    ctx: HandleStepPersistAcksContext<'_>,

    core_backend: &CoreBackend,
    worker_pool: &WorkerPool,
    lock_uuid: Uuid,
    skip_sleep: bool,

    all_persist_acks: Vec<PersistAck>,
) -> Result<(), HandleStepPersistAcksError>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
    WorkerPool: ?Sized + waymark_worker_core::BaseWorkerPool,
{
    let HandleStepPersistAcksContext {
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
    } = ctx;

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

                let ctx = HandleStepPersistedAckContext {
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
                };
                handle_steps_persisted_ack(
                    ctx,
                    core_backend,
                    worker_pool,
                    lock_uuid,
                    skip_sleep,
                    batch,
                    lock_statuses,
                )
                .await
                .map_err(HandleStepPersistAcksError::StepsPersisted)?;
            }
            PersistAck::StepsPersistFailed { batch_id, error } => {
                warn!(batch_id, error = %error, "persist step batch failed");
                return Err(HandleStepPersistAcksError::StepsPersistFailed(error));
            }
        }
    }
    Ok(())
}

pub struct HandleStepPersistedAckContext<'a> {
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
}

async fn handle_steps_persisted_ack<CoreBackend, WorkerPool>(
    ctx: HandleStepPersistedAckContext<'_>,

    core_backend: &CoreBackend,
    worker_pool: &WorkerPool,
    // The lock ID of the current runloop.
    lock_uuid: Uuid,
    skip_sleep: bool,

    batch: crate::commit_barrier::PendingPersistBatch<ShardStep>,
    lock_statuses: Vec<InstanceLockStatus>,
) -> Result<(), crate::RunLoopError>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
    WorkerPool: ?Sized + waymark_worker_core::BaseWorkerPool,
{
    let evict_ids: HashSet<Uuid> =
        crate::runloop::lock_utils::lock_mismatches_for(&lock_statuses, lock_uuid)
            .into_iter()
            .filter(|instance_id| batch.instance_ids.contains(instance_id))
            .collect();

    if !evict_ids.is_empty() {
        let ctx = super::ops::EvictInstancesContext {
            executor_shards: ctx.executor_shards,
            shard_senders: ctx.shard_senders,
            lock_tracker: ctx.lock_tracker,
            inflight_actions: ctx.inflight_actions,
            inflight_dispatches: ctx.inflight_dispatches,
            sleeping_nodes: ctx.sleeping_nodes,
            sleeping_by_instance: ctx.sleeping_by_instance,
            blocked_until_by_instance: ctx.blocked_until_by_instance,
        };

        if let Err(err) = super::ops::evict_instances(
            ctx,
            core_backend,
            lock_uuid,
            &evict_ids.iter().copied().collect::<Vec<_>>(),
        )
        .await
        {
            return Err(err);
        }
    }
    for step in batch.steps {
        if !batch.instance_ids.contains(&step.executor_id) {
            continue;
        }
        if evict_ids.contains(&step.executor_id) {
            continue;
        }
        if !ctx.executor_shards.contains_key(&step.executor_id) && step.instance_done.is_none() {
            continue;
        }

        let ctx = super::ops::ApplyConfirmedStepContext {
            executor_shards: ctx.executor_shards,
            lock_tracker: ctx.lock_tracker,
            inflight_actions: ctx.inflight_actions,
            inflight_dispatches: ctx.inflight_dispatches,
            sleeping_nodes: ctx.sleeping_nodes,
            sleeping_by_instance: ctx.sleeping_by_instance,
            blocked_until_by_instance: ctx.blocked_until_by_instance,
            commit_barrier: ctx.commit_barrier,
            instances_done_pending: ctx.instances_done_pending,
            sleep_tx: ctx.sleep_tx,
        };
        super::ops::apply_confirmed_step(ctx, worker_pool, skip_sleep, step)?;
    }

    for instance_id in batch.instance_ids {
        if evict_ids.contains(&instance_id) {
            ctx.commit_barrier.remove_instance(instance_id);
            continue;
        }
        let events = ctx.commit_barrier.unblock_instance(instance_id);
        flush_deferred_instance_events(instance_id, events, ctx.executor_shards, ctx.shard_senders);
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
