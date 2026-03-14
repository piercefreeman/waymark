use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_core_backend::InstanceDone;
use waymark_runner::SleepRequest;
use waymark_worker_core::{ActionRequest, WorkerPoolError};

use crate::commit_barrier::CommitBarrier;
use crate::instance_lock_heartbeat;
use crate::runloop::test_support::{MockWorkerPool, assert_no_extra_worker_pool_calls};
use crate::runloop::{InflightActionDispatch, RunLoopError, SleepWake};
use crate::shard;

struct TestHarness {
    pub executor_shards: HashMap<Uuid, usize>,
    pub lock_tracker: instance_lock_heartbeat::Tracker,
    pub inflight_actions: HashMap<Uuid, usize>,
    pub inflight_dispatches: HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until: HashMap<Uuid, DateTime<Utc>>,
    pub barrier: CommitBarrier<shard::Step>,
    pub instances_done_pending: Vec<InstanceDone>,
    pub worker_pool: MockWorkerPool,
    pub skip_sleep: bool,
    pub sleep_tx: tokio::sync::mpsc::UnboundedSender<SleepWake>,
    pub _sleep_rx: tokio::sync::mpsc::UnboundedReceiver<SleepWake>,
}

impl Default for TestHarness {
    fn default() -> Self {
        let (sleep_tx, sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();
        Self {
            executor_shards: HashMap::new(),
            lock_tracker: instance_lock_heartbeat::Tracker::default(),
            inflight_actions: HashMap::new(),
            inflight_dispatches: HashMap::new(),
            sleeping_nodes: HashMap::new(),
            sleeping_by_instance: HashMap::new(),
            blocked_until: HashMap::new(),
            barrier: CommitBarrier::new(),
            instances_done_pending: Vec::new(),
            worker_pool: MockWorkerPool::new(),
            skip_sleep: false,
            sleep_tx,
            _sleep_rx: sleep_rx,
        }
    }
}

impl TestHarness {
    fn params<'a>(&'a mut self, step: shard::Step) -> super::Params<'a, MockWorkerPool> {
        super::Params {
            executor_shards: &mut self.executor_shards,
            lock_tracker: &self.lock_tracker,
            inflight_actions: &mut self.inflight_actions,
            inflight_dispatches: &mut self.inflight_dispatches,
            sleeping_nodes: &mut self.sleeping_nodes,
            sleeping_by_instance: &mut self.sleeping_by_instance,
            blocked_until_by_instance: &mut self.blocked_until,
            commit_barrier: &mut self.barrier,
            instances_done_pending: &mut self.instances_done_pending,
            sleep_tx: &self.sleep_tx,
            worker_pool: &self.worker_pool,
            skip_sleep: self.skip_sleep,
            step,
        }
    }
}

#[tokio::test]
async fn records_action_dispatch() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let dispatch_token = Uuid::new_v4();

    let mut harness = TestHarness::default();
    harness.executor_shards.insert(executor_id, 0);

    let step = shard::Step {
        executor_id,
        actions: vec![ActionRequest {
            executor_id,
            execution_id,
            action_name: "my_action".to_string(),
            module_name: None,
            kwargs: HashMap::new(),
            timeout_seconds: 0,
            attempt_number: 1,
            dispatch_token,
        }],
        sleep_requests: vec![],
        updates: None,
        instance_done: None,
    };

    harness
        .worker_pool
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

    super::run(harness.params(step)).expect("apply step");

    assert_eq!(harness.inflight_actions.get(&executor_id), Some(&1));
    let dispatch = harness
        .inflight_dispatches
        .get(&execution_id)
        .expect("dispatch recorded");
    assert_eq!(dispatch.executor_id, executor_id);
    assert_eq!(dispatch.attempt_number, 1);
    assert_eq!(dispatch.dispatch_token, dispatch_token);
    assert!(dispatch.deadline_at.is_none());

    assert_no_extra_worker_pool_calls(&mut harness.worker_pool);
}

#[tokio::test]
async fn queue_error_is_returned() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();

    let mut harness = TestHarness::default();
    harness.executor_shards.insert(executor_id, 0);

    let step = shard::Step {
        executor_id,
        actions: vec![ActionRequest {
            executor_id,
            execution_id,
            action_name: "failing_action".to_string(),
            module_name: None,
            kwargs: HashMap::new(),
            timeout_seconds: 0,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
        }],
        sleep_requests: vec![],
        updates: None,
        instance_done: None,
    };

    harness
        .worker_pool
        .expect_queue()
        .times(1)
        .returning(|_| Err(WorkerPoolError::new("MockQueueError", "mock queue failure")));

    let err = super::run(harness.params(step)).expect_err("queue should fail");
    match err {
        RunLoopError::WorkerPool(pool_err) => {
            assert_eq!(pool_err.kind, "MockQueueError");
            assert_eq!(pool_err.message, "mock queue failure");
        }
        _ => panic!("expected worker pool error"),
    }

    assert_no_extra_worker_pool_calls(&mut harness.worker_pool);
}

#[tokio::test]
async fn timeout_sets_deadline() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();

    let mut harness = TestHarness::default();
    harness.executor_shards.insert(executor_id, 0);

    let step = shard::Step {
        executor_id,
        actions: vec![ActionRequest {
            executor_id,
            execution_id,
            action_name: "timed_action".to_string(),
            module_name: None,
            kwargs: HashMap::new(),
            timeout_seconds: 30,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
        }],
        sleep_requests: vec![],
        updates: None,
        instance_done: None,
    };

    harness
        .worker_pool
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
    super::run(harness.params(step)).expect("apply step");

    let dispatch = harness
        .inflight_dispatches
        .get(&execution_id)
        .expect("dispatch recorded");
    let deadline = dispatch.deadline_at.expect("deadline should be set");
    let expected_min = before + chrono::Duration::seconds(30);
    let expected_max = Utc::now() + chrono::Duration::seconds(30);
    assert!(
        deadline >= expected_min && deadline <= expected_max,
        "deadline should be ~30s from dispatch time"
    );

    assert_no_extra_worker_pool_calls(&mut harness.worker_pool);
}

#[tokio::test]
async fn instance_done_removes_executor_state() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();

    let mut harness = TestHarness::default();
    harness.executor_shards.insert(executor_id, 0);
    harness.lock_tracker.insert_all([executor_id]);
    harness.inflight_actions = HashMap::from([(executor_id, 2usize)]);
    harness.inflight_dispatches = HashMap::from([(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            timeout_seconds: 0,
            deadline_at: None,
        },
    )]);
    harness.sleeping_nodes = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: Utc::now() + chrono::Duration::seconds(60),
        },
    )]);
    harness.sleeping_by_instance = HashMap::from([(executor_id, HashSet::from([node_id]))]);
    harness.blocked_until =
        HashMap::from([(executor_id, Utc::now() + chrono::Duration::seconds(60))]);

    let step = shard::Step {
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

    harness.worker_pool.expect_queue().never();

    super::run(harness.params(step)).expect("apply step");

    assert!(!harness.executor_shards.contains_key(&executor_id));
    assert!(!harness.inflight_actions.contains_key(&executor_id));
    assert!(!harness.inflight_dispatches.contains_key(&execution_id));
    assert!(!harness.sleeping_nodes.contains_key(&node_id));
    assert!(!harness.sleeping_by_instance.contains_key(&executor_id));
    assert!(!harness.blocked_until.contains_key(&executor_id));
    assert_eq!(harness.instances_done_pending.len(), 1);
    assert_eq!(harness.instances_done_pending[0].executor_id, executor_id);

    assert_no_extra_worker_pool_calls(&mut harness.worker_pool);
}

#[tokio::test]
async fn sleep_request_registers_node() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let wake_at = Utc::now() + chrono::Duration::seconds(120);

    let mut harness = TestHarness::default();
    harness.executor_shards.insert(executor_id, 0);

    let step = shard::Step {
        executor_id,
        actions: vec![],
        sleep_requests: vec![SleepRequest { node_id, wake_at }],
        updates: None,
        instance_done: None,
    };

    harness.worker_pool.expect_queue().never();

    super::run(harness.params(step)).expect("apply step");

    let registered = harness
        .sleeping_nodes
        .get(&node_id)
        .expect("sleeping node registered");
    assert_eq!(registered.wake_at, wake_at);
    assert!(
        harness
            .sleeping_by_instance
            .get(&executor_id)
            .is_some_and(|nodes| nodes.contains(&node_id)),
        "instance should track its sleeping node"
    );
    assert_eq!(harness.blocked_until.get(&executor_id), Some(&wake_at));

    assert_no_extra_worker_pool_calls(&mut harness.worker_pool);
}

#[tokio::test]
async fn skip_sleep_overrides_wake_to_now() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let requested_wake_at = Utc::now() + chrono::Duration::seconds(120);

    let mut harness = TestHarness::default();
    harness.executor_shards.insert(executor_id, 0);

    let step = shard::Step {
        executor_id,
        actions: vec![],
        sleep_requests: vec![SleepRequest {
            node_id,
            wake_at: requested_wake_at,
        }],
        updates: None,
        instance_done: None,
    };

    harness.worker_pool.expect_queue().never();

    let before = Utc::now();
    harness.skip_sleep = true;
    super::run(harness.params(step)).expect("apply step");

    let recorded_wake = harness
        .sleeping_nodes
        .get(&node_id)
        .expect("sleeping node registered")
        .wake_at;
    assert!(
        recorded_wake >= before && recorded_wake <= Utc::now(),
        "skip_sleep should clamp wake_at to now"
    );
    assert_eq!(
        harness.blocked_until.get(&executor_id),
        Some(&recorded_wake)
    );

    assert_no_extra_worker_pool_calls(&mut harness.worker_pool);
}

#[tokio::test]
async fn later_duplicate_sleep_request_keeps_existing_earlier_wake() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let existing_wake = Utc::now() + chrono::Duration::seconds(20);
    let later_wake = Utc::now() + chrono::Duration::seconds(90);

    let mut harness = TestHarness::default();
    harness.executor_shards.insert(executor_id, 0);
    harness.sleeping_nodes = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: existing_wake,
        },
    )]);
    harness.sleeping_by_instance = HashMap::from([(executor_id, HashSet::from([node_id]))]);
    harness.blocked_until = HashMap::from([(executor_id, existing_wake)]);

    let step = shard::Step {
        executor_id,
        actions: vec![],
        sleep_requests: vec![SleepRequest {
            node_id,
            wake_at: later_wake,
        }],
        updates: None,
        instance_done: None,
    };

    harness.worker_pool.expect_queue().never();

    super::run(harness.params(step)).expect("apply step");

    assert_eq!(
        harness
            .sleeping_nodes
            .get(&node_id)
            .map(|value| value.wake_at),
        Some(existing_wake),
        "existing earlier wake should be preserved"
    );
    assert_eq!(
        harness.blocked_until.get(&executor_id),
        Some(&existing_wake)
    );

    assert_no_extra_worker_pool_calls(&mut harness.worker_pool);
}
