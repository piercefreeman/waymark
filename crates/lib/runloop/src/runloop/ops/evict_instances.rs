use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use waymark_ids::{ExecutionId, InstanceId, LockId};
use waymark_runner::SleepRequest;

use crate::{instance_lock_heartbeat, runloop::InflightActionDispatch, shard};

pub struct Params<'a, CoreBackend: ?Sized> {
    /// Maps each active instance/executor to the shard currently responsible for it.
    pub executor_shards: &'a mut HashMap<InstanceId, usize>,
    /// Per-shard command channels used to tell shard workers to evict local executors.
    pub shard_senders: &'a [waymark_timed_channel::std::mpsc::Sender<shard::Command>],
    /// Tracks which backend locks this runloop currently believes it owns.
    pub lock_tracker: &'a instance_lock_heartbeat::Tracker,
    /// Counts how many action executions are still outstanding for each executor.
    pub inflight_actions: &'a mut HashMap<InstanceId, usize>,
    /// Tracks the currently valid dispatch token/attempt for each inflight action execution.
    pub inflight_dispatches: &'a mut HashMap<ExecutionId, InflightActionDispatch>,
    /// Active sleep requests keyed by execution node for cleanup during eviction.
    pub sleeping_nodes: &'a mut HashMap<ExecutionId, SleepRequest>,
    /// Reverse index of sleeping node IDs by executor for bulk cleanup during eviction.
    pub sleeping_by_instance: &'a mut HashMap<InstanceId, HashSet<ExecutionId>>,
    /// Earliest wake time currently blocking each executor from making progress.
    pub blocked_until_by_instance: &'a mut HashMap<InstanceId, DateTime<Utc>>,
    /// Backend used to release instance locks after in-memory eviction completes.
    pub core_backend: &'a CoreBackend,
    /// Lock owner ID for this runloop, used when releasing instance locks.
    pub lock_uuid: LockId,
    /// Instances to evict from runloop memory, shard state, and backend lock ownership.
    pub instance_ids: &'a [InstanceId],
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
pub async fn run<CoreBackend>(
    params: Params<'_, CoreBackend>,
) -> Result<(), waymark_backends_core::BackendError>
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

    let mut by_shard: HashMap<usize, Vec<InstanceId>> = HashMap::new();
    let evicted_instance_ids: HashSet<InstanceId> = instance_ids.iter().copied().collect();
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
            let _ = sender.send(shard::Command::Evict(ids));
        }
    }

    core_backend
        .release_instance_locks(lock_uuid, instance_ids)
        .await?;
    Ok(())
}
