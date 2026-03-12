use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_runner::SleepRequest;

use crate::{
    lock::InstanceLockTracker,
    runloop::{InflightActionDispatch, RunLoopError, ShardCommand},
};

pub struct EvictInstancesContext<'a> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    pub lock_tracker: &'a InstanceLockTracker,
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
}

pub async fn evict_instances<CoreBackend>(
    ctx: EvictInstancesContext<'_>,

    core_backend: &CoreBackend,
    lock_uuid: Uuid,

    instance_ids: &[Uuid],
) -> Result<(), RunLoopError>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
{
    if instance_ids.is_empty() {
        return Ok(());
    }

    let mut by_shard: HashMap<usize, Vec<Uuid>> = HashMap::new();
    let evicted_instance_ids: HashSet<Uuid> = instance_ids.iter().copied().collect();
    for instance_id in instance_ids {
        if let Some(shard_idx) = ctx.executor_shards.remove(instance_id) {
            by_shard.entry(shard_idx).or_default().push(*instance_id);
        }
        ctx.inflight_actions.remove(instance_id);
        if let Some(nodes) = ctx.sleeping_by_instance.remove(instance_id) {
            for node_id in nodes {
                ctx.sleeping_nodes.remove(&node_id);
            }
        }
        ctx.blocked_until_by_instance.remove(instance_id);
    }
    ctx.inflight_dispatches
        .retain(|_, dispatch| !evicted_instance_ids.contains(&dispatch.executor_id));
    ctx.lock_tracker.remove_all(instance_ids.iter().copied());
    for (shard_idx, ids) in by_shard {
        if let Some(sender) = ctx.shard_senders.get(shard_idx) {
            let _ = sender.send(ShardCommand::Evict(ids));
        }
    }

    core_backend
        .release_instance_locks(lock_uuid, instance_ids)
        .await?;
    Ok(())
}
