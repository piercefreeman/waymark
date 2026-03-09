use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_runner::SleepRequest;

use crate::{
    commit_barrier::CommitBarrier,
    runloop::{ShardCommand, ShardStep, SleepWake},
};

pub struct Context<'a> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    pub sleeping_nodes: &'a mut HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: &'a mut HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: &'a mut HashMap<Uuid, DateTime<Utc>>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
}

pub fn handle(ctx: Context<'_>, all_wakes: Vec<SleepWake>) {
    let Context {
        executor_shards,
        shard_senders,
        sleeping_nodes,
        sleeping_by_instance,
        blocked_until_by_instance,
        commit_barrier,
    } = ctx;

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
            let _ = sender.send(ShardCommand::Wake(nodes));
        }
    }
}
