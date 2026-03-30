#[cfg(test)]
mod tests;

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use chrono::{DateTime, Utc};
use waymark_core_backend::InstanceDone;
use waymark_ids::{ExecutionId, InstanceId};
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier,
    instance_lock_heartbeat,
    runloop::{InflightActionDispatch, SleepWake},
    shard,
};

pub struct Params<'a, WorkerPool: ?Sized> {
    /// Maps each active instance/executor to the shard currently responsible for it.
    pub executor_shards: &'a mut HashMap<InstanceId, usize>,
    /// Tracks which backend locks this runloop currently believes it owns.
    pub lock_tracker: &'a instance_lock_heartbeat::Tracker,
    /// Counts how many action executions are still outstanding for each executor.
    pub inflight_actions: &'a mut HashMap<InstanceId, usize>,
    /// Tracks the currently valid dispatch token/attempt for each inflight action execution.
    pub inflight_dispatches: &'a mut HashMap<ExecutionId, InflightActionDispatch>,
    /// Active sleep requests keyed by execution node for registration and cleanup.
    pub sleeping_nodes: &'a mut HashMap<ExecutionId, SleepRequest>,
    /// Reverse index of sleeping node IDs by executor for sleep bookkeeping and cleanup.
    pub sleeping_by_instance: &'a mut HashMap<InstanceId, HashSet<ExecutionId>>,
    /// Earliest wake time currently blocking each executor from making progress.
    pub blocked_until_by_instance: &'a mut HashMap<InstanceId, DateTime<Utc>>,
    /// Tracks deferred instance events so completed instances can be fully removed.
    pub commit_barrier: &'a mut CommitBarrier<shard::Step>,
    /// Buffer of terminal instance outcomes that still need durable persistence.
    pub instances_done_pending: &'a mut Vec<InstanceDone>,
    /// Channel used to enqueue future wake notifications for sleeping nodes.
    pub sleep_tx: &'a tokio::sync::mpsc::UnboundedSender<SleepWake>,
    /// Worker pool used to dispatch actions from the confirmed step.
    pub worker_pool: &'a WorkerPool,
    /// Test/debug knob that forces sleeps to wake immediately.
    pub skip_sleep: bool,
    /// Confirmed shard step whose actions, sleeps, and terminal result should be applied.
    pub step: shard::Step,
}

/// Applies a confirmed shard step by dispatching actions and registering sleep requests.
///
/// A shard step is one traversal slice of an instance DAG: it contains frontier
/// actions to dispatch now, sleep nodes to register, and an optional terminal
/// completion when the executor has finished.
///
/// This operation is the primary mechanism for executing graph work. It:
/// - Queues all actions in the step to the worker pool
/// - Tracks action deadlines and attempt numbers for timeout detection
/// - Registers sleep requests for waiting nodes, deduplicating if an earlier wake already exists
/// - Updates instance blocking times to reflect when the next node can continue
/// - Spawns background sleep timers that trigger wake events when ready
///
/// The `skip_sleep` flag is used during testing/debugging to immediately wake all sleeps.
pub fn run<WorkerPool>(
    params: Params<'_, WorkerPool>,
) -> Result<(), waymark_worker_core::WorkerPoolError>
where
    WorkerPool: ?Sized + waymark_worker_core::BaseWorkerPool,
{
    let Params {
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
    } = params;

    for request in step.actions {
        let dispatch = request.clone();
        worker_pool.queue(request)?;

        *inflight_actions.entry(step.executor_id).or_insert(0) += 1;
        let deadline_at = if dispatch.timeout_seconds > 0 {
            Some(Utc::now() + chrono::Duration::seconds(i64::from(dispatch.timeout_seconds)))
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
    for mut sleep_request in step.sleep_requests {
        if skip_sleep {
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
        inflight_dispatches.retain(|_, dispatch| dispatch.executor_id != instance_done.executor_id);
        lock_tracker.remove_all([instance_done.executor_id]);
        if let Some(nodes) = sleeping_by_instance.remove(&instance_done.executor_id) {
            for node_id in nodes {
                sleeping_nodes.remove(&node_id);
            }
        }
        blocked_until_by_instance.remove(&instance_done.executor_id);
        commit_barrier.remove_instance(instance_done.executor_id);
        instances_done_pending.push(instance_done);
    }
    Ok(())
}
