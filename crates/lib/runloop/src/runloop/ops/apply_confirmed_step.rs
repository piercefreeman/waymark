use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_core_backend::InstanceDone;
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier,
    lock::InstanceLockTracker,
    runloop::{InflightActionDispatch, RunLoopError, ShardStep, SleepWake},
};

pub struct ApplyConfirmedStepContext<'a> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
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

pub fn apply_confirmed_step<WorkerPool>(
    ctx: ApplyConfirmedStepContext<'_>,
    worker_pool: &WorkerPool,
    skip_sleep: bool,

    step: ShardStep,
) -> Result<(), RunLoopError>
where
    WorkerPool: ?Sized + waymark_worker_core::BaseWorkerPool,
{
    for request in step.actions {
        let dispatch = request.clone();
        worker_pool.queue(request)?;

        *ctx.inflight_actions.entry(step.executor_id).or_insert(0) += 1;
        let deadline_at = if dispatch.timeout_seconds > 0 {
            Some(Utc::now() + chrono::Duration::seconds(i64::from(dispatch.timeout_seconds)))
        } else {
            None
        };
        ctx.inflight_dispatches.insert(
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
    for mut sleep_request in step.sleep_requests {
        if skip_sleep {
            sleep_request.wake_at = Utc::now();
        }
        let existing = ctx.sleeping_nodes.get(&sleep_request.node_id);
        let should_update = match existing {
            Some(existing) => sleep_request.wake_at < existing.wake_at,
            None => true,
        };
        let wake_at = match existing {
            Some(existing) if !should_update => existing.wake_at,
            _ => sleep_request.wake_at,
        };
        ctx.sleeping_by_instance
            .entry(step.executor_id)
            .or_default()
            .insert(sleep_request.node_id);
        ctx.blocked_until_by_instance
            .entry(step.executor_id)
            .and_modify(|existing| {
                if wake_at < *existing {
                    *existing = wake_at;
                }
            })
            .or_insert(wake_at);

        if should_update {
            ctx.sleeping_nodes
                .insert(sleep_request.node_id, sleep_request.clone());
            let sleep_tx = ctx.sleep_tx.clone();
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
        ctx.executor_shards.remove(&instance_done.executor_id);
        ctx.inflight_actions.remove(&instance_done.executor_id);
        ctx.inflight_dispatches
            .retain(|_, dispatch| dispatch.executor_id != instance_done.executor_id);
        ctx.lock_tracker.remove_all([instance_done.executor_id]);
        if let Some(nodes) = ctx.sleeping_by_instance.remove(&instance_done.executor_id) {
            for node_id in nodes {
                ctx.sleeping_nodes.remove(&node_id);
            }
        }
        ctx.blocked_until_by_instance
            .remove(&instance_done.executor_id);
        ctx.commit_barrier
            .remove_instance(instance_done.executor_id);
        ctx.instances_done_pending.push(instance_done);
    }
    Ok(())
}
