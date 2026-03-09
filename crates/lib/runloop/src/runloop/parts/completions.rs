use std::collections::HashMap;

use tracing::{debug, warn};
use uuid::Uuid;
use waymark_worker_core::ActionCompletion;

use crate::{
    commit_barrier::CommitBarrier,
    runloop::{InflightActionDispatch, ShardCommand, ShardStep},
};

pub struct Context<'a> {
    pub executor_shards: &'a mut HashMap<Uuid, usize>,
    pub shard_senders: &'a [std::sync::mpsc::Sender<ShardCommand>],
    pub inflight_actions: &'a mut HashMap<Uuid, usize>,
    pub inflight_dispatches: &'a mut HashMap<Uuid, InflightActionDispatch>,
    pub commit_barrier: &'a mut CommitBarrier<ShardStep>,
}

pub fn handle(ctx: Context<'_>, all_completions: Vec<ActionCompletion>) {
    let Context {
        executor_shards,
        shard_senders,
        inflight_actions,
        inflight_dispatches,
        commit_barrier,
    } = ctx;

    if all_completions.is_empty() {
        return;
    }

    let mut accepted = Vec::new();
    for completion in all_completions {
        let Some(expected) = inflight_dispatches.get(&completion.execution_id) else {
            debug!(
                executor_id = %completion.executor_id,
                execution_id = %completion.execution_id,
                "dropping completion for unknown inflight action"
            );
            continue;
        };
        if expected.executor_id != completion.executor_id {
            debug!(
                expected_executor_id = %expected.executor_id,
                completion_executor_id = %completion.executor_id,
                execution_id = %completion.execution_id,
                "dropping completion with mismatched executor ownership"
            );
            continue;
        }
        if expected.dispatch_token != completion.dispatch_token
            || expected.attempt_number != completion.attempt_number
        {
            debug!(
                execution_id = %completion.execution_id,
                expected_attempt = expected.attempt_number,
                completion_attempt = completion.attempt_number,
                expected_dispatch_token = %expected.dispatch_token,
                completion_dispatch_token = %completion.dispatch_token,
                "dropping stale completion for prior attempt"
            );
            continue;
        }
        inflight_dispatches.remove(&completion.execution_id);
        if let Some(count) = inflight_actions.get_mut(&completion.executor_id) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                inflight_actions.remove(&completion.executor_id);
            }
        }
        accepted.push(completion);
    }
    if accepted.is_empty() {
        return;
    }

    let mut by_shard: HashMap<usize, Vec<ActionCompletion>> = HashMap::new();
    for completion in accepted {
        let Some(completion) = commit_barrier.route_completion(completion) else {
            continue;
        };
        if let Some(shard_idx) = executor_shards.get(&completion.executor_id).copied() {
            by_shard.entry(shard_idx).or_default().push(completion);
        } else {
            warn!(
                executor_id = %completion.executor_id,
                "completion for unknown executor"
            );
        }
    }
    for (shard_idx, batch) in by_shard {
        if let Some(sender) = shard_senders.get(shard_idx) {
            let _ = sender.send(ShardCommand::ActionCompletions(batch));
        }
    }
}
