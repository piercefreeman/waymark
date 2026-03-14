use std::collections::{HashMap, HashSet};
use std::sync::mpsc as std_mpsc;

use chrono::Utc;
use uuid::Uuid;
use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::InstanceLockStatus;
use waymark_runner::SleepRequest;
use waymark_worker_inline::InlineWorkerPool;

use crate::commit_barrier::CommitBarrier;
use crate::lock::InstanceLockTracker;
use crate::runloop::{
    InflightActionDispatch, PersistAck, RunLoopError, ShardCommand, ShardStep, SleepWake,
};

#[tokio::test]
async fn returns_failed_ack_error_and_preserves_state() {
    let lock_uuid = Uuid::new_v4();
    let mut executor_shards = HashMap::from([(Uuid::new_v4(), 0usize)]);
    let (shard_tx, shard_rx) = std_mpsc::channel::<ShardCommand>();
    let shard_senders = [shard_tx];

    let lock_tracker = InstanceLockTracker::new(lock_uuid);
    let mut inflight_actions = HashMap::new();
    let mut inflight_dispatches = HashMap::new();
    let mut sleeping_nodes = HashMap::new();
    let mut sleeping_by_instance = HashMap::new();
    let mut blocked_until_by_instance = HashMap::new();
    let mut commit_barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let core_backend = MemoryBackend::new();
    let worker_pool = InlineWorkerPool::new(HashMap::new());

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
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        core_backend: &core_backend,
        worker_pool: &worker_pool,
        lock_uuid,
        skip_sleep: false,
        all_persist_acks: vec![PersistAck::StepsPersistFailed {
            batch_id: 7,
            error: RunLoopError::Message("persist boom".to_string()),
        }],
    })
    .await;

    let Err(super::Error::StepsPersistFailed(RunLoopError::Message(msg))) = result else {
        panic!("expected steps persist failed error, got {result:?}");
    };
    assert_eq!(msg, "persist boom");
    assert_eq!(
        executor_shards.len(),
        1,
        "state should be preserved on failed ack"
    );
    assert!(
        shard_rx.try_recv().is_err(),
        "no shard commands should be emitted"
    );
}

#[tokio::test]
async fn ignores_unknown_persist_batch_ack() {
    let lock_uuid = Uuid::new_v4();
    let instance_id = Uuid::new_v4();
    let mut executor_shards = HashMap::from([(instance_id, 0usize)]);
    let (shard_tx, shard_rx) = std_mpsc::channel::<ShardCommand>();
    let shard_senders = [shard_tx];

    let lock_tracker = InstanceLockTracker::new(lock_uuid);
    let mut inflight_actions = HashMap::new();
    let mut inflight_dispatches = HashMap::new();
    let mut sleeping_nodes = HashMap::new();
    let mut sleeping_by_instance = HashMap::new();
    let mut blocked_until_by_instance = HashMap::new();
    let mut commit_barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let mut instances_done_pending = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let core_backend = MemoryBackend::new();
    let worker_pool = InlineWorkerPool::new(HashMap::new());

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
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        core_backend: &core_backend,
        worker_pool: &worker_pool,
        lock_uuid,
        skip_sleep: false,
        all_persist_acks: vec![PersistAck::StepsPersisted {
            batch_id: 999,
            lock_statuses: Vec::new(),
        }],
    })
    .await;

    assert!(result.is_ok(), "unknown ack should be ignored");
    assert_eq!(commit_barrier.pending_batch_count(), 0);
    assert_eq!(executor_shards.get(&instance_id), Some(&0));
    assert!(instances_done_pending.is_empty());
    assert!(
        shard_rx.try_recv().is_err(),
        "unknown batch should not emit shard commands"
    );
}

#[tokio::test]
async fn evicts_only_lock_mismatch_instances_from_persisted_batch() {
    let lock_uuid = Uuid::new_v4();
    let keep_instance = Uuid::new_v4();
    let evict_instance = Uuid::new_v4();

    let keep_execution = Uuid::new_v4();
    let evict_execution = Uuid::new_v4();
    let keep_node = Uuid::new_v4();
    let evict_node = Uuid::new_v4();

    let mut executor_shards = HashMap::from([(keep_instance, 0usize), (evict_instance, 0usize)]);
    let (shard_tx, shard_rx) = std_mpsc::channel::<ShardCommand>();
    let shard_senders = [shard_tx];

    let lock_tracker = InstanceLockTracker::new(lock_uuid);
    lock_tracker.insert_all([keep_instance, evict_instance]);

    let mut inflight_actions = HashMap::from([(keep_instance, 1usize), (evict_instance, 1usize)]);
    let mut inflight_dispatches = HashMap::from([
        (
            keep_execution,
            InflightActionDispatch {
                executor_id: keep_instance,
                attempt_number: 1,
                dispatch_token: Uuid::new_v4(),
                timeout_seconds: 0,
                deadline_at: None,
            },
        ),
        (
            evict_execution,
            InflightActionDispatch {
                executor_id: evict_instance,
                attempt_number: 1,
                dispatch_token: Uuid::new_v4(),
                timeout_seconds: 0,
                deadline_at: None,
            },
        ),
    ]);
    let mut sleeping_nodes = HashMap::from([
        (
            keep_node,
            SleepRequest {
                node_id: keep_node,
                wake_at: Utc::now() + chrono::Duration::seconds(30),
            },
        ),
        (
            evict_node,
            SleepRequest {
                node_id: evict_node,
                wake_at: Utc::now() + chrono::Duration::seconds(30),
            },
        ),
    ]);
    let mut sleeping_by_instance = HashMap::from([
        (keep_instance, HashSet::from([keep_node])),
        (evict_instance, HashSet::from([evict_node])),
    ]);
    let mut blocked_until_by_instance = HashMap::from([
        (keep_instance, Utc::now() + chrono::Duration::seconds(30)),
        (evict_instance, Utc::now() + chrono::Duration::seconds(30)),
    ]);
    let mut commit_barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let batch_id =
        commit_barrier.register_batch(HashSet::from([keep_instance, evict_instance]), vec![]);

    let mut instances_done_pending = Vec::new();
    let (sleep_tx, _sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();

    let core_backend = MemoryBackend::new();
    let worker_pool = InlineWorkerPool::new(HashMap::new());

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
        instances_done_pending: &mut instances_done_pending,
        sleep_tx: &sleep_tx,
        core_backend: &core_backend,
        worker_pool: &worker_pool,
        lock_uuid,
        skip_sleep: false,
        all_persist_acks: vec![PersistAck::StepsPersisted {
            batch_id,
            lock_statuses: vec![
                InstanceLockStatus {
                    instance_id: keep_instance,
                    lock_uuid: Some(lock_uuid),
                    lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
                },
                InstanceLockStatus {
                    instance_id: evict_instance,
                    lock_uuid: Some(Uuid::new_v4()),
                    lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
                },
            ],
        }],
    })
    .await;

    assert!(result.is_ok());
    assert!(executor_shards.contains_key(&keep_instance));
    assert!(!executor_shards.contains_key(&evict_instance));

    assert!(inflight_actions.contains_key(&keep_instance));
    assert!(!inflight_actions.contains_key(&evict_instance));

    assert!(
        inflight_dispatches
            .values()
            .any(|dispatch| dispatch.executor_id == keep_instance),
        "keep instance dispatch should remain"
    );
    assert!(
        inflight_dispatches
            .values()
            .all(|dispatch| dispatch.executor_id != evict_instance),
        "evicted instance dispatches should be pruned"
    );

    assert!(sleeping_nodes.contains_key(&keep_node));
    assert!(!sleeping_nodes.contains_key(&evict_node));
    assert!(sleeping_by_instance.contains_key(&keep_instance));
    assert!(!sleeping_by_instance.contains_key(&evict_instance));
    assert!(blocked_until_by_instance.contains_key(&keep_instance));
    assert!(!blocked_until_by_instance.contains_key(&evict_instance));

    assert_eq!(commit_barrier.pending_batch_count(), 0);
    assert!(instances_done_pending.is_empty());

    let cmd = shard_rx
        .try_recv()
        .expect("eviction command should be sent");
    let ShardCommand::Evict(ids) = cmd else {
        panic!("expected Evict command");
    };
    assert_eq!(ids, vec![evict_instance]);
}
