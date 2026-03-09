use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier,
    lock::InstanceLockTracker,
    runloop::{InflightActionDispatch, ShardCommand, ShardStep},
};

pub struct Context<'a> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    pub lock_tracker: &'a InstanceLockTracker,
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("evict instance: {0}")]
    EvictInstance(#[source] crate::RunLoopError),
}

pub async fn handle<CoreBackend>(
    ctx: Context<'_>,

    core_backend: &CoreBackend,
    lock_uuid: Uuid,

    evict_sleep_threshold: std::time::Duration,
) -> Result<(), Error>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
{
    let Context {
        executor_shards,
        shard_senders,
        lock_tracker,
        inflight_actions,
        inflight_dispatches,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
    } = ctx;

    if blocked_until_by_instance.is_empty() {
        return Ok(());
    }

    let now = Utc::now();
    let evict_ids: Vec<Uuid> = blocked_until_by_instance
        .iter()
        .filter_map(|(instance_id, wake_at)| {
            let inflight = inflight_actions.get(instance_id).copied().unwrap_or(0);
            if inflight > 0 {
                return None;
            }
            let sleep_for = wake_at.signed_duration_since(now).to_std().ok()?;
            if sleep_for > evict_sleep_threshold {
                Some(*instance_id)
            } else {
                None
            }
        })
        .collect();
    if !evict_ids.is_empty() {
        let ctx = super::ops::EvictInstancesContext {
            executor_shards,
            shard_senders,
            lock_tracker,
            inflight_actions,
            inflight_dispatches,
            sleeping_nodes,
            sleeping_by_instance,
            blocked_until_by_instance,
        };
        super::ops::evict_instances(ctx, core_backend, lock_uuid, &evict_ids)
            .await
            .map_err(Error::EvictInstance)?;

        for instance_id in evict_ids {
            commit_barrier.remove_instance(instance_id);
        }
    }

    Ok(())
}
