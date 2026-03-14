use std::collections::{HashMap, HashSet};
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;
use waymark_backend_memory::MemoryBackend;
use waymark_runner::SleepRequest;

use crate::commit_barrier::CommitBarrier;
use crate::lock::InstanceLockTracker;
use crate::runloop::{InflightActionDispatch, ShardCommand, ShardStep};

#[tokio::test]
async fn evicts_instance_over_threshold_without_inflight_actions() {
    let lock_uuid = Uuid::new_v4();
    let instance_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();

    let mut executor_shards = HashMap::from([(instance_id, 0usize)]);
    let (shard_tx, shard_rx) = std_mpsc::channel::<ShardCommand>();
    let shard_senders = [shard_tx];

    let lock_tracker = InstanceLockTracker::new(lock_uuid);
    lock_tracker.insert_all([instance_id]);
    let mut inflight_actions = HashMap::from([(instance_id, 0usize)]);
    let mut inflight_dispatches = HashMap::from([(
        Uuid::new_v4(),
        InflightActionDispatch {
            executor_id: instance_id,
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
            wake_at: Utc::now() + chrono::Duration::seconds(30),
        },
    )]);
    let mut sleeping_by_instance = HashMap::from([(instance_id, HashSet::from([node_id]))]);
    let mut blocked_until_by_instance =
        HashMap::from([(instance_id, Utc::now() + chrono::Duration::seconds(30))]);
    let mut commit_barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    let backend = MemoryBackend::new();
    let result = super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &shard_senders,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until_by_instance,
        commit_barrier: &mut commit_barrier,
        core_backend: &backend,
        lock_uuid,
        evict_sleep_threshold: Duration::from_secs(10),
    })
    .await;

    assert!(result.is_ok());
    assert!(!executor_shards.contains_key(&instance_id));
    assert!(!inflight_actions.contains_key(&instance_id));
    assert!(
        inflight_dispatches
            .values()
            .all(|dispatch| dispatch.executor_id != instance_id),
        "evicted instance dispatches should be removed"
    );
    assert!(!sleeping_nodes.contains_key(&node_id));
    assert!(!sleeping_by_instance.contains_key(&instance_id));
    assert!(!blocked_until_by_instance.contains_key(&instance_id));

    let cmd = shard_rx.try_recv().expect("evict command should be sent");
    let ShardCommand::Evict(ids) = cmd else {
        panic!("expected Evict command");
    };
    assert_eq!(ids, vec![instance_id]);
}

#[tokio::test]
async fn does_not_evict_when_inflight_actions_exist() {
    let lock_uuid = Uuid::new_v4();
    let instance_id = Uuid::new_v4();

    let mut executor_shards = HashMap::from([(instance_id, 0usize)]);
    let (shard_tx, shard_rx) = std_mpsc::channel::<ShardCommand>();
    let shard_senders = [shard_tx];

    let lock_tracker = InstanceLockTracker::new(lock_uuid);
    let mut inflight_actions = HashMap::from([(instance_id, 2usize)]);
    let mut inflight_dispatches: HashMap<Uuid, InflightActionDispatch> = HashMap::new();
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut blocked_until_by_instance =
        HashMap::from([(instance_id, Utc::now() + chrono::Duration::seconds(30))]);
    let mut commit_barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    let backend = MemoryBackend::new();
    let result = super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &shard_senders,
        lock_tracker: &lock_tracker,
        inflight_actions: &mut inflight_actions,
        inflight_dispatches: &mut inflight_dispatches,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until_by_instance,
        commit_barrier: &mut commit_barrier,
        core_backend: &backend,
        lock_uuid,
        evict_sleep_threshold: Duration::from_secs(10),
    })
    .await;

    assert!(result.is_ok());
    assert!(executor_shards.contains_key(&instance_id));
    assert_eq!(inflight_actions.get(&instance_id), Some(&2));
    assert!(blocked_until_by_instance.contains_key(&instance_id));
    assert!(shard_rx.try_recv().is_err(), "no evict command expected");
}
