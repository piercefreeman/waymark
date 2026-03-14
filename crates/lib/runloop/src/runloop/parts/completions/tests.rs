use std::collections::{HashMap, HashSet};
use std::sync::mpsc;

use uuid::Uuid;

use crate::commit_barrier::CommitBarrier;
use crate::runloop::{InflightActionDispatch, ShardCommand, ShardStep};

#[test]
fn drops_unknown_execution_id() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let (tx, _rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut inflight_actions = HashMap::from([(executor_id, 1usize)]);
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        commit_barrier: &mut barrier,
        all_completions: vec![waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id,
            attempt_number: 1,
            dispatch_token,
            result: serde_json::json!(null),
        }],
    });

    assert_eq!(
        inflight_actions.get(&executor_id),
        Some(&1),
        "inflight count unchanged when completion is dropped"
    );
}

#[test]
fn drops_mismatched_executor_id() {
    let executor_id = Uuid::new_v4();
    let other_executor = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let (tx, _rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut inflight_actions = HashMap::from([(executor_id, 1usize)]);
    let mut inflight_dispatches = HashMap::from([(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    )]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        commit_barrier: &mut barrier,
        all_completions: vec![waymark_worker_core::ActionCompletion {
            executor_id: other_executor,
            execution_id,
            attempt_number: 1,
            dispatch_token,
            result: serde_json::json!(null),
        }],
    });

    assert!(
        inflight_dispatches.contains_key(&execution_id),
        "dispatch not consumed when executor id mismatches"
    );
}

#[test]
fn drops_stale_dispatch_token() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let stale_token = Uuid::new_v4();
    let (tx, _rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut inflight_actions = HashMap::from([(executor_id, 1usize)]);
    let mut inflight_dispatches = HashMap::from([(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    )]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        commit_barrier: &mut barrier,
        all_completions: vec![waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id,
            attempt_number: 1,
            dispatch_token: stale_token,
            result: serde_json::json!(null),
        }],
    });

    assert!(
        inflight_dispatches.contains_key(&execution_id),
        "dispatch not consumed on stale token"
    );
    assert_eq!(inflight_actions.get(&executor_id), Some(&1));
}

#[test]
fn drops_stale_attempt_number() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let (tx, _rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut inflight_actions = HashMap::from([(executor_id, 1usize)]);
    let mut inflight_dispatches = HashMap::from([(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 2,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    )]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        commit_barrier: &mut barrier,
        all_completions: vec![waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id,
            attempt_number: 1,
            dispatch_token,
            result: serde_json::json!(null),
        }],
    });

    assert!(
        inflight_dispatches.contains_key(&execution_id),
        "dispatch not consumed on stale attempt"
    );
    assert_eq!(inflight_actions.get(&executor_id), Some(&1));
}

#[test]
fn valid_decrements_inflight_and_routes_to_shard() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let (tx, rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut inflight_actions = HashMap::from([(executor_id, 1usize)]);
    let mut inflight_dispatches = HashMap::from([(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    )]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        commit_barrier: &mut barrier,
        all_completions: vec![waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id,
            attempt_number: 1,
            dispatch_token,
            result: serde_json::json!(null),
        }],
    });

    assert!(
        !inflight_dispatches.contains_key(&execution_id),
        "dispatch should be removed on success"
    );
    assert!(
        !inflight_actions.contains_key(&executor_id),
        "inflight counter should be removed when it reaches zero"
    );

    let cmd = rx.try_recv().expect("shard should receive a command");
    let ShardCommand::ActionCompletions(batch) = cmd else {
        panic!("expected ActionCompletions command");
    };
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].execution_id, execution_id);
}

#[test]
fn blocked_instance_defers_completion_until_unblock() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let (tx, rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut inflight_actions = HashMap::from([(executor_id, 1usize)]);
    let mut inflight_dispatches = HashMap::from([(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    )]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    barrier.register_batch(HashSet::from([executor_id]), vec![]);

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        commit_barrier: &mut barrier,
        all_completions: vec![waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id,
            attempt_number: 1,
            dispatch_token,
            result: serde_json::json!(null),
        }],
    });

    assert!(
        !inflight_dispatches.contains_key(&execution_id),
        "accepted completion should consume inflight dispatch"
    );
    assert!(
        !inflight_actions.contains_key(&executor_id),
        "accepted completion should decrement inflight action count"
    );
    assert!(
        rx.try_recv().is_err(),
        "completion should be deferred while persist batch blocks the instance"
    );

    let deferred = barrier.unblock_instance(executor_id);
    assert_eq!(deferred.len(), 1, "one completion should be deferred");
}

#[test]
fn accepted_completion_for_unknown_shard_is_dropped_after_accounting() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();
    let (tx, rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_actions = HashMap::from([(executor_id, 1usize)]);
    let mut inflight_dispatches = HashMap::from([(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    )]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        commit_barrier: &mut barrier,
        all_completions: vec![waymark_worker_core::ActionCompletion {
            executor_id,
            execution_id,
            attempt_number: 1,
            dispatch_token,
            result: serde_json::json!(null),
        }],
    });

    assert!(
        !inflight_dispatches.contains_key(&execution_id),
        "valid completion should still consume inflight dispatch"
    );
    assert!(
        !inflight_actions.contains_key(&executor_id),
        "valid completion should still decrement inflight action count"
    );
    assert!(
        rx.try_recv().is_err(),
        "unknown shard ownership should prevent send"
    );
}
