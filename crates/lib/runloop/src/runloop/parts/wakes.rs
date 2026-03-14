#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_runner::SleepRequest;

use crate::{commit_barrier::CommitBarrier, runloop::SleepWake, shard};

pub struct Params<'a> {
    /// Maps each active instance/executor to the shard currently responsible for it.
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    /// Per-shard command channels used to forward wake events back to executors.
    pub shard_senders: &'a [std::sync::mpsc::Sender<shard::Command>],
    /// Active sleep requests keyed by execution node so wake handling can validate deadlines.
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    /// Reverse index of sleeping node IDs by executor for bulk cleanup and blocked-until recomputation.
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    /// Earliest wake time currently blocking each executor from making progress.
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    /// Defers wakes for instances that cannot resume until a persisted batch is acknowledged.
    pub commit_barrier: &'a mut CommitBarrier<shard::Step>,
    /// Wake notifications collected during the current coordinator tick.
    pub all_wakes: Vec<SleepWake>,
}

/// Routes sleep wake events and recomputes instance blocking times.
///
/// **Why this part exists:** Instances blocked by sleep nodes become ready when
/// their sleep deadline arrives. The runloop must route these ready nodes back to
/// shards for continued execution and update blocking timestamps to reflect the
/// next sleep deadline (if multiple nodes are sleeping).
///
/// **What it does:**
/// - Filters wakes for nodes that are actually sleeping and have reached their deadline
/// - Removes woken nodes from sleep tracking
/// - For instances with multiple sleeping nodes, recomputes blocked_until to the
///   earliest remaining wake time
/// - Routes through commit barrier to check if instance can accept the wake
/// - Groups woken nodes by shard and sends to shard workers
/// - Cleans up empty instance entries from sleep tracking
pub fn handle(params: Params<'_>) {
    let Params {
        executor_shards,
        shard_senders,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
        all_wakes,
    } = params;

    if all_wakes.is_empty() {
        return;
    }

    let now = Utc::now();
    let mut by_shard: HashMap<usize, Vec<Uuid>> = HashMap::new();
    for wake in all_wakes {
        let Some(request) = sleeping_nodes.get(&wake.node_id) else {
            continue;
        };
        if request.wake_at > now {
            continue;
        }
        sleeping_nodes.remove(&wake.node_id);

        if let Some(nodes) = sleeping_by_instance.get_mut(&wake.executor_id) {
            nodes.remove(&wake.node_id);
            if nodes.is_empty() {
                sleeping_by_instance.remove(&wake.executor_id);
                blocked_until_by_instance.remove(&wake.executor_id);
            } else {
                let mut next_wake: Option<DateTime<Utc>> = None;
                for node_id in nodes.iter() {
                    if let Some(entry) = sleeping_nodes.get(node_id) {
                        next_wake = Some(match next_wake {
                            Some(existing) => existing.min(entry.wake_at),
                            None => entry.wake_at,
                        });
                    }
                }
                if let Some(next_wake) = next_wake {
                    blocked_until_by_instance.insert(wake.executor_id, next_wake);
                }
            }
        }
        if let Some(shard_idx) = executor_shards.get(&wake.executor_id).copied() {
            let Some(node_id) = commit_barrier.route_wake(wake.executor_id, wake.node_id) else {
                continue;
            };
            by_shard.entry(shard_idx).or_default().push(node_id);
        }
    }

    for (shard_idx, nodes) in by_shard {
        if let Some(sender) = shard_senders.get(shard_idx) {
            let _ = sender.send(shard::Command::Wake(nodes));
        }
    }
}
