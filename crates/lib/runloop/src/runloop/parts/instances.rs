use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use tracing::{debug, warn};
use uuid::Uuid;
use waymark_core_backend::QueuedInstance;
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier,
    lock::InstanceLockTracker,
    runloop::{InflightActionDispatch, ShardCommand, ShardStep},
};

pub struct Params<'a, WorkflowRegistryBackend: ?Sized> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    pub lock_tracker: &'a InstanceLockTracker,
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,

    pub workflow_cache: &'a mut HashMap<Uuid, Arc<waymark_dag::DAG>>,
    pub registry_backend: &'a WorkflowRegistryBackend,

    pub instances_idle: &'a mut bool,
    pub next_shard: &'a mut usize,
    pub shard_count: usize,
    pub all_instances: Vec<QueuedInstance>,
    pub saw_empty_instances: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("hydrate: {0}")]
    Hydrate(#[source] crate::RunLoopError),
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
        inflight_actions,
        inflight_dispatches,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
        workflow_cache,
        registry_backend,
        instances_idle,
        next_shard,
        shard_count,
        mut all_instances,
        saw_empty_instances,
    } = params;

    if all_instances.is_empty() {
        if saw_empty_instances {
            *instances_idle = true;
        }
        return Ok(());
    }

    *instances_idle = false;

    let params = super::ops::hydrate_instances::Params {
        workflow_cache,
        registry_backend,
        instances: &mut all_instances,
    };

    super::ops::hydrate_instances::run(params)
        .await
        .map_err(Error::Hydrate)?;
    debug!(count = all_instances.len(), "hydrated queued instances");

    let mut by_shard: HashMap<usize, Vec<QueuedInstance>> = HashMap::new();
    let mut claimed_instance_ids = Vec::with_capacity(all_instances.len());
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
        lock_uuid = %lock_tracker.lock_uuid(),
        "tracked instance locks"
    );
    for (shard_idx, batch) in by_shard {
        if let Some(sender) = shard_senders.get(shard_idx) {
            let _ = sender.send(ShardCommand::AssignInstances(batch));
        }
    }

    Ok(())
}
