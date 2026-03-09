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

pub struct HandleFailedInstancesContext<'a> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub lock_tracker: &'a InstanceLockTracker,
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
}

pub fn handle_failed_instances(
    ctx: HandleFailedInstancesContext<'_>,

    all_failed_instances: Vec<InstanceDone>,
    instances_done_pending: &mut Vec<InstanceDone>,
) {
    let HandleFailedInstancesContext {
        executor_shards,
        lock_tracker,
        inflight_actions,
        inflight_dispatches,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
    } = ctx;

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
