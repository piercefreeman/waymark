use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_core_backend::InstanceDone;
use waymark_runner::SleepRequest;

use crate::commit_barrier::CommitBarrier;
use crate::lock::InstanceLockTracker;
use crate::runloop::{InflightActionDispatch, ShardStep};

#[test]
fn cleans_up_all_state() {
    let executor_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();

    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    lock_tracker.insert_all([executor_id]);

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut inflight_actions = HashMap::from([(executor_id, 1usize)]);
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
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> =
        HashMap::from([(executor_id, Utc::now() + chrono::Duration::seconds(60))]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        all_failed_instances: vec![InstanceDone {
            executor_id,
            entry_node: Uuid::new_v4(),
            result: None,
            error: Some(serde_json::json!({"type": "ExecutionError", "message": "boom"})),
        }],
        instances_done_pending: &mut instances_done_pending,
    });

    assert!(!executor_shards.contains_key(&executor_id));
    assert!(!inflight_actions.contains_key(&executor_id));
    assert!(!inflight_dispatches.contains_key(&execution_id));
    assert!(!sleeping_nodes.contains_key(&node_id));
    assert!(!sleeping_by_instance.contains_key(&executor_id));
    assert!(!blocked_until.contains_key(&executor_id));
    assert_eq!(instances_done_pending.len(), 1);
    assert_eq!(instances_done_pending[0].executor_id, executor_id);
}

#[test]
fn empty_list_is_noop() {
    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    let mut executor_shards: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_actions: HashMap<Uuid, usize> = HashMap::new();
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending: Vec<InstanceDone> = Vec::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        all_failed_instances: vec![],
        instances_done_pending: &mut instances_done_pending,
    });

    assert!(instances_done_pending.is_empty());
}
