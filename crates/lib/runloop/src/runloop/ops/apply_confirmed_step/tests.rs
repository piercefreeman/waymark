use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_core_backend::InstanceDone;
use waymark_runner::SleepRequest;
use waymark_worker_core::{ActionRequest, WorkerPoolError};

use crate::commit_barrier::CommitBarrier;
use crate::lock::InstanceLockTracker;
use crate::runloop::test_support::{MockWorkerPool, empty_kwargs};
use crate::runloop::{InflightActionDispatch, RunLoopError, ShardStep, SleepWake};

#[tokio::test]
async fn records_action_dispatch() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    let mut inflight_actions: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let step = ShardStep {
        executor_id,
        actions: vec![ActionRequest {
            executor_id,
            execution_id,
            action_name: "my_action".to_string(),
            module_name: None,
            kwargs: empty_kwargs(),
            timeout_seconds: 0,
            attempt_number: 1,
            dispatch_token,
        }],
        sleep_requests: vec![],
        updates: None,
        instance_done: None,
    };

    let mut worker_pool = MockWorkerPool::new();
    worker_pool
        .expect_queue()
        .times(1)
        .withf(move |request| {
            request.executor_id == executor_id
                && request.execution_id == execution_id
                && request.dispatch_token == dispatch_token
                && request.timeout_seconds == 0
                && request.attempt_number == 1
        })
        .returning(|_| Ok(()));

    let params = super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        worker_pool: &worker_pool,
        skip_sleep: false,
        step,
    };
    super::run(params).expect("apply step");

    assert_eq!(inflight_actions.get(&executor_id), Some(&1));
    let dispatch = inflight_dispatches
        .get(&execution_id)
        .expect("dispatch recorded");
    assert_eq!(dispatch.executor_id, executor_id);
    assert_eq!(dispatch.attempt_number, 1);
    assert_eq!(dispatch.dispatch_token, dispatch_token);
    assert!(dispatch.deadline_at.is_none());
}

#[tokio::test]
async fn queue_error_is_returned() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    let mut inflight_actions: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let step = ShardStep {
        executor_id,
        actions: vec![ActionRequest {
            executor_id,
            execution_id,
            action_name: "failing_action".to_string(),
            module_name: None,
            kwargs: empty_kwargs(),
            timeout_seconds: 0,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
        }],
        sleep_requests: vec![],
        updates: None,
        instance_done: None,
    };

    let mut worker_pool = MockWorkerPool::new();
    worker_pool
        .expect_queue()
        .times(1)
        .returning(|_| Err(WorkerPoolError::new("MockQueueError", "mock queue failure")));

    let params = super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        worker_pool: &worker_pool,
        skip_sleep: false,
        step,
    };

    let err = super::run(params).expect_err("queue should fail");
    match err {
        RunLoopError::WorkerPool(pool_err) => {
            assert_eq!(pool_err.kind, "MockQueueError");
            assert_eq!(pool_err.message, "mock queue failure");
        }
        _ => panic!("expected worker pool error"),
    }
}

#[tokio::test]
async fn timeout_sets_deadline() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    let mut inflight_actions: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let step = ShardStep {
        executor_id,
        actions: vec![ActionRequest {
            executor_id,
            execution_id,
            action_name: "timed_action".to_string(),
            module_name: None,
            kwargs: empty_kwargs(),
            timeout_seconds: 30,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
        }],
        sleep_requests: vec![],
        updates: None,
        instance_done: None,
    };

    let mut worker_pool = MockWorkerPool::new();
    worker_pool
        .expect_queue()
        .times(1)
        .withf(move |request| {
            request.executor_id == executor_id
                && request.execution_id == execution_id
                && request.timeout_seconds == 30
                && request.attempt_number == 1
        })
        .returning(|_| Ok(()));

    let before = Utc::now();
    let params = super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        worker_pool: &worker_pool,
        skip_sleep: false,
        step,
    };
    super::run(params).expect("apply step");

    let dispatch = inflight_dispatches
        .get(&execution_id)
        .expect("dispatch recorded");
    let deadline = dispatch.deadline_at.expect("deadline should be set");
    let expected_min = before + chrono::Duration::seconds(30);
    let expected_max = Utc::now() + chrono::Duration::seconds(30);
    assert!(
        deadline >= expected_min && deadline <= expected_max,
        "deadline should be ~30s from dispatch time"
    );
}

#[tokio::test]
async fn instance_done_removes_executor_state() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();

    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    lock_tracker.insert_all([executor_id]);

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut inflight_actions = HashMap::from([(executor_id, 2usize)]);
    let mut inflight_dispatches = HashMap::from([(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            timeout_seconds: 0,
            deadline_at: None,
        },
    )]);
    let mut sleeping_nodes = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: Utc::now() + chrono::Duration::seconds(60),
        },
    )]);
    let mut sleeping_by_instance = HashMap::from([(executor_id, HashSet::from([node_id]))]);
    let mut blocked_until =
        HashMap::from([(executor_id, Utc::now() + chrono::Duration::seconds(60))]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let step = ShardStep {
        executor_id,
        actions: vec![],
        sleep_requests: vec![],
        updates: None,
        instance_done: Some(InstanceDone {
            executor_id,
            entry_node: Uuid::new_v4(),
            result: Some(serde_json::json!("done")),
            error: None,
        }),
    };

    let mut worker_pool = MockWorkerPool::new();
    worker_pool.expect_queue().never();

    let params = super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        worker_pool: &worker_pool,
        skip_sleep: false,
        step,
    };
    super::run(params).expect("apply step");

    assert!(!executor_shards.contains_key(&executor_id));
    assert!(!inflight_actions.contains_key(&executor_id));
    assert!(!inflight_dispatches.contains_key(&execution_id));
    assert!(!sleeping_nodes.contains_key(&node_id));
    assert!(!sleeping_by_instance.contains_key(&executor_id));
    assert!(!blocked_until.contains_key(&executor_id));
    assert_eq!(instances_done_pending.len(), 1);
    assert_eq!(instances_done_pending[0].executor_id, executor_id);
}

#[tokio::test]
async fn sleep_request_registers_node() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let wake_at = Utc::now() + chrono::Duration::seconds(120);

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    let mut inflight_actions: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let step = ShardStep {
        executor_id,
        actions: vec![],
        sleep_requests: vec![SleepRequest { node_id, wake_at }],
        updates: None,
        instance_done: None,
    };

    let mut worker_pool = MockWorkerPool::new();
    worker_pool.expect_queue().never();

    let params = super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        worker_pool: &worker_pool,
        skip_sleep: false,
        step,
    };
    super::run(params).expect("apply step");

    let registered = sleeping_nodes
        .get(&node_id)
        .expect("sleeping node registered");
    assert_eq!(registered.wake_at, wake_at);
    assert!(
        sleeping_by_instance
            .get(&executor_id)
            .is_some_and(|nodes| nodes.contains(&node_id)),
        "instance should track its sleeping node"
    );
    assert_eq!(blocked_until.get(&executor_id), Some(&wake_at));
}

#[tokio::test]
async fn skip_sleep_overrides_wake_to_now() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let requested_wake_at = Utc::now() + chrono::Duration::seconds(120);

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    let mut inflight_actions: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let step = ShardStep {
        executor_id,
        actions: vec![],
        sleep_requests: vec![SleepRequest {
            node_id,
            wake_at: requested_wake_at,
        }],
        updates: None,
        instance_done: None,
    };

    let mut worker_pool = MockWorkerPool::new();
    worker_pool.expect_queue().never();

    let before = Utc::now();
    let params = super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        worker_pool: &worker_pool,
        skip_sleep: true,
        step,
    };
    super::run(params).expect("apply step");

    let recorded_wake = sleeping_nodes
        .get(&node_id)
        .expect("sleeping node registered")
        .wake_at;
    assert!(
        recorded_wake >= before && recorded_wake <= Utc::now(),
        "skip_sleep should clamp wake_at to now"
    );
    assert_eq!(blocked_until.get(&executor_id), Some(&recorded_wake));
}

#[tokio::test]
async fn later_duplicate_sleep_request_keeps_existing_earlier_wake() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let existing_wake = Utc::now() + chrono::Duration::seconds(20);
    let later_wake = Utc::now() + chrono::Duration::seconds(90);

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    let mut inflight_actions: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: existing_wake,
        },
    )]);
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> =
        HashMap::from([(executor_id, HashSet::from([node_id]))]);
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> =
        HashMap::from([(executor_id, existing_wake)]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let step = ShardStep {
        executor_id,
        actions: vec![],
        sleep_requests: vec![SleepRequest {
            node_id,
            wake_at: later_wake,
        }],
        updates: None,
        instance_done: None,
    };

    let mut worker_pool = MockWorkerPool::new();
    worker_pool.expect_queue().never();

    let params = super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        worker_pool: &worker_pool,
        skip_sleep: false,
        step,
    };
    super::run(params).expect("apply step");

    assert_eq!(
        sleeping_nodes.get(&node_id).map(|value| value.wake_at),
        Some(existing_wake),
        "existing earlier wake should be preserved"
    );
    assert_eq!(blocked_until.get(&executor_id), Some(&existing_wake));
}
