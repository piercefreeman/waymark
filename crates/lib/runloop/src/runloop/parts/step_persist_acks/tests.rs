use std::collections::{HashMap, HashSet};
use std::sync::mpsc as std_mpsc;

use chrono::Utc;
use uuid::Uuid;
use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::InstanceLockStatus;
use waymark_runner::SleepRequest;
use waymark_worker_inline::InlineWorkerPool;

use crate::commit_barrier::CommitBarrier;
use crate::instance_lock_heartbeat;
use crate::runloop::{InflightActionDispatch, SleepWake};
use crate::{persist, shard};

struct TestHarness {
    pub lock_uuid: Uuid,
    pub executor_shards: HashMap<Uuid, usize>,
    pub shard_senders: Vec<std_mpsc::Sender<shard::Command>>,
    pub lock_tracker: instance_lock_heartbeat::Tracker,
    pub inflight_actions: HashMap<Uuid, usize>,
    pub inflight_dispatches: HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: HashMap<Uuid, chrono::DateTime<Utc>>,
    pub commit_barrier: CommitBarrier<shard::Step>,
    pub instances_done_pending: Vec<waymark_core_backend::InstanceDone>,
    pub sleep_tx: tokio::sync::mpsc::UnboundedSender<SleepWake>,
    pub _sleep_rx: tokio::sync::mpsc::UnboundedReceiver<SleepWake>,
    pub core_backend: MemoryBackend,
    pub worker_pool: InlineWorkerPool,
}

impl Default for TestHarness {
    fn default() -> Self {
        let lock_uuid = Uuid::new_v4();
        let (sleep_tx, sleep_rx) = tokio::sync::mpsc::unbounded_channel::<SleepWake>();
        Self {
            lock_uuid,
            executor_shards: HashMap::new(),
            shard_senders: Vec::new(),
            lock_tracker: instance_lock_heartbeat::Tracker::default(),
            inflight_actions: HashMap::new(),
            inflight_dispatches: HashMap::new(),
            sleeping_nodes: HashMap::new(),
            sleeping_by_instance: HashMap::new(),
            blocked_until_by_instance: HashMap::new(),
            commit_barrier: CommitBarrier::new(),
            instances_done_pending: Vec::new(),
            sleep_tx,
            _sleep_rx: sleep_rx,
            core_backend: MemoryBackend::new(),
            worker_pool: InlineWorkerPool::new(HashMap::new()),
        }
    }
}

impl TestHarness {
    fn params<'a>(
        &'a mut self,
        all_persist_acks: Vec<persist::Ack>,
    ) -> super::Params<'a, MemoryBackend, InlineWorkerPool> {
        super::Params {
            executor_shards: &mut self.executor_shards,
            shard_senders: &self.shard_senders,
            lock_tracker: &self.lock_tracker,
            inflight_actions: &mut self.inflight_actions,
            inflight_dispatches: &mut self.inflight_dispatches,
            sleeping_nodes: &mut self.sleeping_nodes,
            sleeping_by_instance: &mut self.sleeping_by_instance,
            blocked_until_by_instance: &mut self.blocked_until_by_instance,
            commit_barrier: &mut self.commit_barrier,
            instances_done_pending: &mut self.instances_done_pending,
            sleep_tx: &self.sleep_tx,
            core_backend: &self.core_backend,
            worker_pool: &self.worker_pool,
            lock_uuid: self.lock_uuid,
            skip_sleep: false,
            all_persist_acks,
        }
    }
}

#[tokio::test]
async fn returns_failed_ack_error_and_preserves_state() {
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = std_mpsc::channel::<shard::Command>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(Uuid::new_v4(), 0);

    let result = super::handle(harness.params(vec![persist::Ack::StepsPersistFailed {
        batch_id: 7,
        error: "persist boom".to_string(),
    }]))
    .await;

    let Err(super::Error::StepsPersistFailed(msg)) = result else {
        panic!("expected steps persist failed error, got {result:?}");
    };
    assert_eq!(msg, "persist boom");
    assert_eq!(
        harness.executor_shards.len(),
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
    let instance_id = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = std_mpsc::channel::<shard::Command>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(instance_id, 0);

    let result = super::handle(harness.params(vec![persist::Ack::StepsPersisted {
        batch_id: 999,
        lock_statuses: Vec::new(),
    }]))
    .await;

    assert!(result.is_ok(), "unknown ack should be ignored");
    assert_eq!(harness.commit_barrier.pending_batch_count(), 0);
    assert_eq!(harness.executor_shards.get(&instance_id), Some(&0));
    assert!(harness.instances_done_pending.is_empty());
    assert!(
        shard_rx.try_recv().is_err(),
        "unknown batch should not emit shard commands"
    );
}

#[tokio::test]
async fn evicts_only_lock_mismatch_instances_from_persisted_batch() {
    let keep_instance = Uuid::new_v4();
    let evict_instance = Uuid::new_v4();

    let keep_execution = Uuid::new_v4();
    let evict_execution = Uuid::new_v4();
    let keep_node = Uuid::new_v4();
    let evict_node = Uuid::new_v4();

    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = std_mpsc::channel::<shard::Command>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(keep_instance, 0);
    harness.executor_shards.insert(evict_instance, 0);
    harness
        .lock_tracker
        .insert_all([keep_instance, evict_instance]);
    harness.inflight_actions.insert(keep_instance, 1);
    harness.inflight_actions.insert(evict_instance, 1);
    harness.inflight_dispatches.insert(
        keep_execution,
        InflightActionDispatch {
            executor_id: keep_instance,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            timeout_seconds: 0,
            deadline_at: None,
        },
    );
    harness.inflight_dispatches.insert(
        evict_execution,
        InflightActionDispatch {
            executor_id: evict_instance,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            timeout_seconds: 0,
            deadline_at: None,
        },
    );
    harness.sleeping_nodes.insert(
        keep_node,
        SleepRequest {
            node_id: keep_node,
            wake_at: Utc::now() + chrono::Duration::seconds(30),
        },
    );
    harness.sleeping_nodes.insert(
        evict_node,
        SleepRequest {
            node_id: evict_node,
            wake_at: Utc::now() + chrono::Duration::seconds(30),
        },
    );
    harness
        .sleeping_by_instance
        .insert(keep_instance, HashSet::from([keep_node]));
    harness
        .sleeping_by_instance
        .insert(evict_instance, HashSet::from([evict_node]));
    harness
        .blocked_until_by_instance
        .insert(keep_instance, Utc::now() + chrono::Duration::seconds(30));
    harness
        .blocked_until_by_instance
        .insert(evict_instance, Utc::now() + chrono::Duration::seconds(30));
    let batch_id = harness
        .commit_barrier
        .register_batch(HashSet::from([keep_instance, evict_instance]), vec![]);

    let result = super::handle(harness.params(vec![persist::Ack::StepsPersisted {
        batch_id,
        lock_statuses: vec![
            InstanceLockStatus {
                instance_id: keep_instance,
                lock_uuid: Some(harness.lock_uuid),
                lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
            },
            InstanceLockStatus {
                instance_id: evict_instance,
                lock_uuid: Some(Uuid::new_v4()),
                lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
            },
        ],
    }]))
    .await;

    assert!(result.is_ok());
    assert!(harness.executor_shards.contains_key(&keep_instance));
    assert!(!harness.executor_shards.contains_key(&evict_instance));

    assert!(harness.inflight_actions.contains_key(&keep_instance));
    assert!(!harness.inflight_actions.contains_key(&evict_instance));

    assert!(
        harness
            .inflight_dispatches
            .values()
            .any(|dispatch| dispatch.executor_id == keep_instance),
        "keep instance dispatch should remain"
    );
    assert!(
        harness
            .inflight_dispatches
            .values()
            .all(|dispatch| dispatch.executor_id != evict_instance),
        "evicted instance dispatches should be pruned"
    );

    assert!(harness.sleeping_nodes.contains_key(&keep_node));
    assert!(!harness.sleeping_nodes.contains_key(&evict_node));
    assert!(harness.sleeping_by_instance.contains_key(&keep_instance));
    assert!(!harness.sleeping_by_instance.contains_key(&evict_instance));
    assert!(
        harness
            .blocked_until_by_instance
            .contains_key(&keep_instance)
    );
    assert!(
        !harness
            .blocked_until_by_instance
            .contains_key(&evict_instance)
    );

    assert_eq!(harness.commit_barrier.pending_batch_count(), 0);
    assert!(harness.instances_done_pending.is_empty());

    let cmd = shard_rx
        .try_recv()
        .expect("eviction command should be sent");
    let shard::Command::Evict(ids) = cmd else {
        panic!("expected Evict command");
    };
    assert_eq!(ids, vec![evict_instance]);
}
