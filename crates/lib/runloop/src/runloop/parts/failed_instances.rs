#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use tracing::warn;
use uuid::Uuid;
use waymark_core_backend::InstanceDone;
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier,
    lock::InstanceLockTracker,
    runloop::{InflightActionDispatch, ShardStep},
};

pub struct Params<'a> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub lock_tracker: &'a InstanceLockTracker,
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
    pub all_failed_instances: Vec<InstanceDone>,
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
