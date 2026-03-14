use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_core_backend::InstanceDone;
use waymark_runner::SleepRequest;

use crate::commit_barrier::CommitBarrier;
use crate::instance_lock_heartbeat;
use crate::runloop::InflightActionDispatch;
use crate::shard;

struct TestHarness {
    pub lock_tracker: instance_lock_heartbeat::Tracker,
    pub executor_shards: HashMap<Uuid, usize>,
    pub inflight_actions: HashMap<Uuid, usize>,
    pub inflight_dispatches: HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until: HashMap<Uuid, DateTime<Utc>>,
    pub barrier: CommitBarrier<shard::Step>,
    pub instances_done_pending: Vec<InstanceDone>,
}

impl Default for TestHarness {
    fn default() -> Self {
        Self {
            lock_tracker: instance_lock_heartbeat::Tracker::default(),
            executor_shards: HashMap::new(),
            inflight_actions: HashMap::new(),
            inflight_dispatches: HashMap::new(),
            sleeping_nodes: HashMap::new(),
            sleeping_by_instance: HashMap::new(),
            blocked_until: HashMap::new(),
            barrier: CommitBarrier::new(),
            instances_done_pending: Vec::new(),
        }
    }
}

impl TestHarness {
    fn params<'a>(&'a mut self, all_failed_instances: Vec<InstanceDone>) -> super::Params<'a> {
        super::Params {
            executor_shards: &mut self.executor_shards,
            lock_tracker: &self.lock_tracker,
            inflight_actions: &mut self.inflight_actions,
            inflight_dispatches: &mut self.inflight_dispatches,
            sleeping_nodes: &mut self.sleeping_nodes,
            sleeping_by_instance: &mut self.sleeping_by_instance,
            blocked_until_by_instance: &mut self.blocked_until,
            commit_barrier: &mut self.barrier,
            all_failed_instances,
            instances_done_pending: &mut self.instances_done_pending,
        }
    }
}

#[test]
fn cleans_up_all_state() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();

    let mut harness = TestHarness::default();
    harness.lock_tracker.insert_all([executor_id]);
    harness.executor_shards.insert(executor_id, 0usize);
    harness.inflight_actions.insert(executor_id, 1usize);
    harness.inflight_dispatches.insert(
        execution_id,
        InflightActionDispatch {
            executor_id,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            timeout_seconds: 0,
            deadline_at: None,
        },
    );
    harness.sleeping_nodes.insert(
        node_id,
        SleepRequest {
            node_id,
            wake_at: Utc::now() + chrono::Duration::seconds(60),
        },
    );
    harness
        .sleeping_by_instance
        .insert(executor_id, HashSet::from([node_id]));
    harness
        .blocked_until
        .insert(executor_id, Utc::now() + chrono::Duration::seconds(60));

    super::handle(harness.params(vec![InstanceDone {
        executor_id,
        entry_node: Uuid::new_v4(),
        result: None,
        error: Some(serde_json::json!({"type": "ExecutionError", "message": "boom"})),
    }]));

    assert!(!harness.executor_shards.contains_key(&executor_id));
    assert!(!harness.inflight_actions.contains_key(&executor_id));
    assert!(!harness.inflight_dispatches.contains_key(&execution_id));
    assert!(!harness.sleeping_nodes.contains_key(&node_id));
    assert!(!harness.sleeping_by_instance.contains_key(&executor_id));
    assert!(!harness.blocked_until.contains_key(&executor_id));
    assert_eq!(harness.instances_done_pending.len(), 1);
    assert_eq!(harness.instances_done_pending[0].executor_id, executor_id);
}

#[test]
fn empty_list_is_noop() {
    let mut harness = TestHarness::default();

    super::handle(harness.params(vec![]));

    assert!(harness.instances_done_pending.is_empty());
}
