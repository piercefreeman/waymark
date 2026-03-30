#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use tracing::warn;
use waymark_core_backend::InstanceDone;
use waymark_ids::{ExecutionId, InstanceId};
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier, instance_lock_heartbeat, runloop::InflightActionDispatch, shard,
};

pub struct Params<'a> {
    /// Maps each active instance/executor to the shard currently responsible for it.
    pub executor_shards: &'a mut HashMap<InstanceId, usize>,
    /// Tracks which backend locks this runloop currently believes it owns.
    pub lock_tracker: &'a instance_lock_heartbeat::Tracker,
    /// Counts how many action executions are still outstanding for each executor.
    pub inflight_actions: &'a mut HashMap<InstanceId, usize>,
    /// Tracks the currently valid dispatch token/attempt for each inflight action execution.
    pub inflight_dispatches: &'a mut HashMap<ExecutionId, InflightActionDispatch>,
    /// Active sleep requests keyed by execution node for cleanup when an instance fails.
    pub sleeping_nodes: &'a mut HashMap<ExecutionId, SleepRequest>,
    /// Reverse index of sleeping node IDs by executor for bulk cleanup when an instance fails.
    pub sleeping_by_instance: &'a mut HashMap<InstanceId, HashSet<ExecutionId>>,
    /// Earliest wake time currently blocking each executor from making progress.
    pub blocked_until_by_instance: &'a mut HashMap<InstanceId, DateTime<Utc>>,
    /// Tracks deferred instance events so failed instances can be fully removed.
    pub commit_barrier: &'a mut CommitBarrier<shard::Step>,
    /// Failed instances reported by shards during the current coordinator tick.
    pub all_failed_instances: Vec<InstanceDone>,
    /// Buffer of terminal instance outcomes that still need durable persistence.
    pub instances_done_pending: &'a mut Vec<InstanceDone>,
}

/// Cleans up all state for instances that failed during shard execution.
///
/// **Why this part exists:** When a shard encounters unrecoverable errors during
/// processing, the entire instance must be marked failed and all associated state removed
/// to prevent resource leaks and stale bookkeeping.
///
/// **What it does:** For each failed instance:
/// - Removes from executor-to-shard mapping
/// - Clears inflight action counters and dispatch tracking
/// - Removes all associated sleep requests and wake tracking
/// - Removes instance lock entries
/// - Removes from commit barrier tracking
/// - Adds instance to pending done buffer for persistence
pub fn handle(params: Params<'_>) {
    let Params {
        executor_shards,
        lock_tracker,
        inflight_actions,
        inflight_dispatches,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
        all_failed_instances,
        instances_done_pending,
    } = params;

    if all_failed_instances.is_empty() {
        return;
    }

    for instance_done in all_failed_instances {
        warn!(
            executor_id = %instance_done.executor_id,
            error = ?instance_done.error,
            "marking instance as failed after shard execution error"
        );
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
}
