use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_runner::SleepRequest;

use crate::{
    lock::InstanceLockTracker,
    runloop::{InflightActionDispatch, RunLoopError, ShardCommand},
};

pub struct Params<'a, CoreBackend: ?Sized> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    pub lock_tracker: &'a InstanceLockTracker,
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    pub core_backend: &'a CoreBackend,
    pub lock_uuid: Uuid,
    pub instance_ids: &'a [Uuid],
}

/// Evicts instances from the runloop and releases their backend locks.
///
/// This operation removes an instance and all its associated state:
/// - Removes from executor-to-shard mapping
/// - Removes all inflight actions and sleep tracking for the instance
/// - Sends eviction command to the shard to clean up local state
/// - Releases locks held by the instance in the backend
///
/// Eviction occurs when instances have exceeded error thresholds, resource limits,
/// or are explicitly terminated by callers.
pub async fn run<CoreBackend>(params: Params<'_, CoreBackend>) -> Result<(), RunLoopError>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
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
        core_backend,
        lock_uuid,
        instance_ids,
    } = params;

    if instance_ids.is_empty() {
        return Ok(());
    }

    let mut by_shard: HashMap<usize, Vec<Uuid>> = HashMap::new();
    let evicted_instance_ids: HashSet<Uuid> = instance_ids.iter().copied().collect();
    for instance_id in instance_ids {
        if let Some(shard_idx) = executor_shards.remove(instance_id) {
            by_shard.entry(shard_idx).or_default().push(*instance_id);
        }
        inflight_actions.remove(instance_id);
        if let Some(nodes) = sleeping_by_instance.remove(instance_id) {
            for node_id in nodes {
                sleeping_nodes.remove(&node_id);
            }
        }
        blocked_until_by_instance.remove(instance_id);
    }
    inflight_dispatches.retain(|_, dispatch| !evicted_instance_ids.contains(&dispatch.executor_id));
    lock_tracker.remove_all(instance_ids.iter().copied());
    for (shard_idx, ids) in by_shard {
        if let Some(sender) = shard_senders.get(shard_idx) {
            let _ = sender.send(ShardCommand::Evict(ids));
        }
    }

    core_backend
        .release_instance_locks(lock_uuid, instance_ids)
        .await?;
    Ok(())
}
