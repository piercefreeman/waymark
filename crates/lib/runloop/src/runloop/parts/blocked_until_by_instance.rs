use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier,
    lock::InstanceLockTracker,
    runloop::{InflightActionDispatch, ShardCommand, ShardStep},
};

#[cfg(test)]
mod tests;

pub struct Params<'a, CoreBackend: ?Sized> {
    /// Maps each active instance/executor to the shard currently responsible for it.
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    /// Per-shard command channels used to send eviction commands to shard workers.
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    /// Tracks which backend locks this runloop currently believes it owns.
    pub lock_tracker: &'a InstanceLockTracker,
    /// Counts how many action executions are still outstanding for each executor.
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    /// Tracks the currently valid dispatch token/attempt for each inflight action execution.
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    /// Active sleep requests keyed by execution node for cleanup during eviction.
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    /// Reverse index of sleeping node IDs by executor for bulk cleanup during eviction.
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    /// Earliest wake time currently blocking each executor from making progress.
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    /// Tracks deferred instance events so evicted instances can be fully removed.
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
    /// Backend used to release locks and persist eviction side effects.
    pub core_backend: &'a CoreBackend,
    /// Lock owner ID for this runloop, used when releasing instance locks.
    pub lock_uuid: Uuid,
    /// Maximum tolerated remaining sleep duration before the instance is evicted.
    pub evict_sleep_threshold: std::time::Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("evict instance: {0}")]
    EvictInstance(#[source] crate::RunLoopError),
}

/// Evicts instances that have been sleeping too long.
///
/// **Why this part exists:** Instances sleeping for extended durations consume
/// resources without progress. This part proactively removes them to reclaim
/// memory and allow the runloop to remain responsive.
///
/// **What it does:** Collects instances whose blocked_until time is in the future by
/// more than the eviction threshold, then:
/// - Batches them for efficient removal via the evict_instances operation
/// - Cleaned-up state mirrors eviction: executor removal, lock release in the backend
pub async fn handle<CoreBackend>(params: Params<'_, CoreBackend>) -> Result<(), Error>
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
        commit_barrier,
        core_backend,
        lock_uuid,
        evict_sleep_threshold,
    } = params;

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
        let params = super::ops::evict_instances::Params {
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
            instance_ids: &evict_ids,
        };
        super::ops::evict_instances::run(params)
            .await
            .map_err(Error::EvictInstance)?;

        for instance_id in evict_ids {
            commit_barrier.remove_instance(instance_id);
        }
    }

    Ok(())
}
