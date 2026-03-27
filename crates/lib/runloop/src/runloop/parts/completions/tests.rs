use std::collections::{HashMap, HashSet};
use std::sync::mpsc;

use uuid::Uuid;
use waymark_ids::{ExecutionId, InstanceId};

use crate::commit_barrier::CommitBarrier;
use crate::runloop::InflightActionDispatch;
use crate::shard;

struct TestHarness {
    pub executor_shards: HashMap<InstanceId, usize>,
    pub inflight_actions: HashMap<InstanceId, usize>,
    pub inflight_dispatches: HashMap<ExecutionId, InflightActionDispatch>,
    pub commit_barrier: CommitBarrier<shard::Step>,
    pub shard_senders: Vec<mpsc::Sender<waymark_timed::Opaque<shard::Command>>>,
}

impl Default for TestHarness {
    fn default() -> Self {
        Self {
            executor_shards: HashMap::new(),
            inflight_actions: HashMap::new(),
            inflight_dispatches: HashMap::new(),
            commit_barrier: CommitBarrier::new(),
            shard_senders: Vec::new(),
        }
    }
}

impl TestHarness {
    fn params<'a>(
        &'a mut self,
        all_completions: Vec<waymark_worker_core::ActionCompletion>,
    ) -> super::Params<'a> {
        super::Params {
            executor_shards: &mut self.executor_shards,
            shard_senders: &self.shard_senders,
            inflight_actions: &mut self.inflight_actions,
            inflight_dispatches: &mut self.inflight_dispatches,
            commit_barrier: &mut self.commit_barrier,
            all_completions,
        }
    }
}

#[test]
fn drops_unknown_execution_id() {
    let executor_id = InstanceId::new_uuid_v4();
    let execution_id = ExecutionId::new_uuid_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, _shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.inflight_actions.insert(executor_id, 1);

    super::handle(harness.params(vec![waymark_worker_core::ActionCompletion {
        executor_id,
        execution_id,
        attempt_number: 1,
        dispatch_token,
        result: serde_json::json!(null),
    }]));

    assert_eq!(
        harness.inflight_actions.get(&executor_id),
        Some(&1),
        "inflight count unchanged when completion is dropped"
    );
}

#[test]
fn drops_mismatched_executor_id() {
    let executor_id = InstanceId::new_uuid_v4();
    let other_executor = InstanceId::new_uuid_v4();
    let execution_id = ExecutionId::new_uuid_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, _shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.inflight_actions.insert(executor_id, 1);
    harness.inflight_dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    );

    super::handle(harness.params(vec![waymark_worker_core::ActionCompletion {
        executor_id: other_executor,
        execution_id,
        attempt_number: 1,
        dispatch_token,
        result: serde_json::json!(null),
    }]));

    assert!(
        harness.inflight_dispatches.contains_key(&execution_id),
        "dispatch not consumed when executor id mismatches"
    );
}

#[test]
fn drops_stale_dispatch_token() {
    let executor_id = InstanceId::new_uuid_v4();
    let execution_id = ExecutionId::new_uuid_v4();
    let dispatch_token = Uuid::new_v4();
    let stale_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, _shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.inflight_actions.insert(executor_id, 1);
    harness.inflight_dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    );

    super::handle(harness.params(vec![waymark_worker_core::ActionCompletion {
        executor_id,
        execution_id,
        attempt_number: 1,
        dispatch_token: stale_token,
        result: serde_json::json!(null),
    }]));

    assert!(
        harness.inflight_dispatches.contains_key(&execution_id),
        "dispatch not consumed on stale token"
    );
    assert_eq!(harness.inflight_actions.get(&executor_id), Some(&1));
}

#[test]
fn drops_stale_attempt_number() {
    let executor_id = InstanceId::new_uuid_v4();
    let execution_id = ExecutionId::new_uuid_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, _shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.inflight_actions.insert(executor_id, 1);
    harness.inflight_dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 2,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    );

    super::handle(harness.params(vec![waymark_worker_core::ActionCompletion {
        executor_id,
        execution_id,
        attempt_number: 1,
        dispatch_token,
        result: serde_json::json!(null),
    }]));

    assert!(
        harness.inflight_dispatches.contains_key(&execution_id),
        "dispatch not consumed on stale attempt"
    );
    assert_eq!(harness.inflight_actions.get(&executor_id), Some(&1));
}

#[test]
fn valid_decrements_inflight_and_routes_to_shard() {
    let executor_id = InstanceId::new_uuid_v4();
    let execution_id = ExecutionId::new_uuid_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.inflight_actions.insert(executor_id, 1);
    harness.inflight_dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    );

    super::handle(harness.params(vec![waymark_worker_core::ActionCompletion {
        executor_id,
        execution_id,
        attempt_number: 1,
        dispatch_token,
        result: serde_json::json!(null),
    }]));

    assert!(
        !harness.inflight_dispatches.contains_key(&execution_id),
        "dispatch should be removed on success"
    );
    assert!(
        !harness.inflight_actions.contains_key(&executor_id),
        "inflight counter should be removed when it reaches zero"
    );

    let cmd = shard_rx.try_recv().expect("shard should receive a command");
    let cmd = cmd.into_inner();
    let shard::Command::ActionCompletions(batch) = cmd else {
        panic!("expected ActionCompletions command");
    };
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].execution_id, execution_id);
}

#[test]
fn blocked_instance_defers_completion_until_unblock() {
    let executor_id = InstanceId::new_uuid_v4();
    let execution_id = ExecutionId::new_uuid_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.inflight_actions.insert(executor_id, 1);
    harness.inflight_dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    );
    harness
        .commit_barrier
        .register_batch(HashSet::from([executor_id]), vec![]);

    super::handle(harness.params(vec![waymark_worker_core::ActionCompletion {
        executor_id,
        execution_id,
        attempt_number: 1,
        dispatch_token,
        result: serde_json::json!(null),
    }]));

    assert!(
        !harness.inflight_dispatches.contains_key(&execution_id),
        "accepted completion should consume inflight dispatch"
    );
    assert!(
        !harness.inflight_actions.contains_key(&executor_id),
        "accepted completion should decrement inflight action count"
    );
    assert!(
        shard_rx.try_recv().is_err(),
        "completion should be deferred while persist batch blocks the instance"
    );

    let deferred = harness.commit_barrier.unblock_instance(executor_id);
    assert_eq!(deferred.len(), 1, "one completion should be deferred");
}

#[test]
fn accepted_completion_for_unknown_shard_is_dropped_after_accounting() {
    let executor_id = InstanceId::new_uuid_v4();
    let execution_id = ExecutionId::new_uuid_v4();
    let dispatch_token = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.inflight_actions.insert(executor_id, 1);
    harness.inflight_dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token,
            timeout_seconds: 0,
            deadline_at: None,
        },
    );

    super::handle(harness.params(vec![waymark_worker_core::ActionCompletion {
        executor_id,
        execution_id,
        attempt_number: 1,
        dispatch_token,
        result: serde_json::json!(null),
    }]));

    assert!(
        !harness.inflight_dispatches.contains_key(&execution_id),
        "valid completion should still consume inflight dispatch"
    );
    assert!(
        !harness.inflight_actions.contains_key(&executor_id),
        "valid completion should still decrement inflight action count"
    );
    assert!(
        shard_rx.try_recv().is_err(),
        "unknown shard ownership should prevent send"
    );
}
