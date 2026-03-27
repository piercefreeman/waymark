use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use nonempty_collections::NEVec;
use tracing::{debug, warn};
use uuid::Uuid;
use waymark_core_backend::QueuedInstance;
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier, instance_lock_heartbeat, runloop::InflightActionDispatch, shard,
};

#[cfg(test)]
mod tests;

pub struct Params<'a, WorkflowRegistryBackend: ?Sized> {
    /// Maps each active instance/executor to the shard currently responsible for it.
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    /// Per-shard command channels used to assign hydrated instances to shard workers.
    pub shard_senders: &'a [std::sync::mpsc::Sender<waymark_timed::Opaque<shard::Command>>],
    /// Tracks which backend locks this runloop currently believes it owns.
    pub lock_tracker: &'a instance_lock_heartbeat::Tracker,
    /// Lock owner ID for this runloop, used here only for logging purposes.
    pub lock_uuid: Uuid,
    /// Counts how many action executions are still outstanding for each executor.
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    /// Tracks the currently valid dispatch token/attempt for each inflight action execution.
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    /// Active sleep requests keyed by execution node so reclaimed instances can clear stale sleeps.
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    /// Reverse index of sleeping node IDs by executor for cleanup when instances are reclaimed.
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    /// Earliest wake time currently blocking each executor from making progress.
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    /// Tracks deferred instance events so reclaimed instances can discard stale barriers.
    pub commit_barrier: &'a mut CommitBarrier<shard::Step>,

    /// Cache of workflow DAGs keyed by workflow version ID to avoid repeated hydration work.
    pub workflow_cache: &'a mut HashMap<Uuid, Arc<waymark_dag::DAG>>,
    /// Backend used to fetch workflow definitions that are missing from the local cache.
    pub registry_backend: &'a WorkflowRegistryBackend,

    /// Round-robin cursor used to spread newly claimed instances across shards.
    pub next_shard: &'a mut usize,
    /// Total number of available shards used with the round-robin cursor.
    pub shard_count: NonZeroUsize,
    /// Newly claimed instances collected during the current coordinator tick.
    pub all_instances: NEVec<QueuedInstance>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("hydrate: {0}")]
    Hydrate(#[source] super::ops::hydrate_instances::Error),
}

/// Registers new instances into the runloop and initializes their state.
///
/// **Why this part exists:** New instances must be loaded from the backend, hydrated
/// with workflow definitions, assigned to shards, locked in the backend, and tracked
/// in the runloop's bookkeeping structures before they can execute.
///
/// **What it does:**
/// - Hydrates instances with their workflow DAGs (fetching from backend if needed)
/// - Determines initial "blocked_until" (when instance can first run)
/// - Distributes instances across shards in round-robin for load balancing
/// - Acquires backend locks to prevent concurrent execution of the same instance
/// - Initializes state mappings: executor-to-shard, inflight tracking, sleep tracking
/// - Registers in commit barrier for state coordination
/// - Sends instances to assigned shards for execution
pub async fn handle<WorkflowRegistryBackend>(
    params: Params<'_, WorkflowRegistryBackend>,
) -> Result<(), Error>
where
    WorkflowRegistryBackend: ?Sized + waymark_workflow_registry_backend::WorkflowRegistryBackend,
{
    let Params {
        executor_shards,
        shard_senders,
        lock_tracker,
        lock_uuid,
        inflight_actions,
        inflight_dispatches,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
        workflow_cache,
        registry_backend,
        next_shard,
        shard_count,
        mut all_instances,
    } = params;

    let params = super::ops::hydrate_instances::Params {
        workflow_cache,
        registry_backend,
        instances: all_instances.as_mut(),
    };

    super::ops::hydrate_instances::run(params)
        .await
        .map_err(Error::Hydrate)?;
    debug!(count = all_instances.len(), "hydrated queued instances");

    let mut by_shard: HashMap<usize, Vec<QueuedInstance>> = HashMap::new();
    let mut claimed_instance_ids = Vec::with_capacity(all_instances.len().get());
    let mut replaced_instance_ids = Vec::new();
    for instance in all_instances {
        let shard_idx =
            if let Some(existing_shard_idx) = executor_shards.get(&instance.instance_id).copied() {
                // If an already-active instance reappears from the queue, treat
                // the prior in-memory executor as stale and replace it.
                replaced_instance_ids.push(instance.instance_id);
                inflight_actions.insert(instance.instance_id, 0);
                if let Some(nodes) = sleeping_by_instance.remove(&instance.instance_id) {
                    for node_id in nodes {
                        sleeping_nodes.remove(&node_id);
                    }
                }
                blocked_until_by_instance.remove(&instance.instance_id);
                existing_shard_idx
            } else {
                let shard_idx = *next_shard % shard_count;
                *next_shard = next_shard.wrapping_add(1);
                executor_shards.insert(instance.instance_id, shard_idx);
                inflight_actions.insert(instance.instance_id, 0);
                shard_idx
            };
        claimed_instance_ids.push(instance.instance_id);
        by_shard.entry(shard_idx).or_default().push(instance);
    }

    if !replaced_instance_ids.is_empty() {
        warn!(
            replaced = replaced_instance_ids.len(),
            "replacing active executors for reclaimed queued instances"
        );
        let replaced_set: HashSet<Uuid> = replaced_instance_ids.iter().copied().collect();
        inflight_dispatches.retain(|_, dispatch| !replaced_set.contains(&dispatch.executor_id));
        for instance_id in &replaced_set {
            commit_barrier.remove_instance(*instance_id);
        }
    }

    let claimed_count = claimed_instance_ids.len();
    lock_tracker.insert_all(claimed_instance_ids);
    debug!(
        count = claimed_count,
        %lock_uuid,
        "tracked instance locks"
    );

    for (shard_idx, batch) in by_shard {
        if let Some(sender) = shard_senders.get(shard_idx) {
            let _ = sender.send(shard::Command::AssignInstances(batch).into());
        }
    }

    Ok(())
}
