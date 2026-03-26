use std::collections::{HashMap, HashSet};
use std::sync::mpsc as std_mpsc;

use chrono::Utc;
use uuid::Uuid;
use waymark_backend_memory::MemoryBackend;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_runner::SleepRequest;

use crate::commit_barrier::CommitBarrier;
use crate::instance_lock_heartbeat;
use crate::runloop::InflightActionDispatch;
use crate::shard;

struct TestHarness {
    pub lock_uuid: Uuid,
    pub backend: MemoryBackend,
    pub lock_tracker: instance_lock_heartbeat::Tracker,
    pub executor_shards: HashMap<Uuid, usize>,
    pub inflight_actions: HashMap<Uuid, usize>,
    pub inflight_dispatches: HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: HashMap<Uuid, chrono::DateTime<Utc>>,
    pub commit_barrier: CommitBarrier<shard::Step>,
    pub shard_senders: Vec<std_mpsc::Sender<shard::Command>>,
    pub evict_sleep_threshold: NonZeroDuration,
}

impl Default for TestHarness {
    fn default() -> Self {
        let lock_uuid = Uuid::new_v4();
        Self {
            lock_uuid,
            backend: MemoryBackend::new(),
            lock_tracker: instance_lock_heartbeat::Tracker::default(),
            executor_shards: HashMap::new(),
            inflight_actions: HashMap::new(),
            inflight_dispatches: HashMap::new(),
            sleeping_nodes: HashMap::new(),
            sleeping_by_instance: HashMap::new(),
            blocked_until_by_instance: HashMap::new(),
            commit_barrier: CommitBarrier::new(),
            shard_senders: Vec::new(),
            evict_sleep_threshold: NonZeroDuration::from_secs(10).unwrap(),
        }
    }
}

impl TestHarness {
    fn params<'a>(&'a mut self) -> super::Params<'a, MemoryBackend> {
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
            core_backend: &self.backend,
            lock_uuid: self.lock_uuid,
            evict_sleep_threshold: self.evict_sleep_threshold,
        }
    }
}

#[tokio::test]
async fn evicts_instance_over_threshold_without_inflight_actions() {
    let instance_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();

    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = std_mpsc::channel::<shard::Command>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(instance_id, 0);
    harness.lock_tracker.insert_all([instance_id]);
    harness.inflight_actions.insert(instance_id, 0);
    harness.inflight_dispatches.insert(
        Uuid::new_v4(),
        InflightActionDispatch {
            executor_id: instance_id,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            timeout_seconds: 0,
            deadline_at: None,
        },
    );
    harness.sleeping_nodes = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: Utc::now() + chrono::Duration::seconds(30),
        },
    )]);
    harness.sleeping_by_instance = HashMap::from([(instance_id, HashSet::from([node_id]))]);
    harness.blocked_until_by_instance =
        HashMap::from([(instance_id, Utc::now() + chrono::Duration::seconds(30))]);

    let result = super::handle(harness.params()).await;

    assert!(result.is_ok());
    assert!(!harness.executor_shards.contains_key(&instance_id));
    assert!(!harness.inflight_actions.contains_key(&instance_id));
    assert!(
        harness
            .inflight_dispatches
            .values()
            .all(|dispatch| dispatch.executor_id != instance_id),
        "evicted instance dispatches should be removed"
    );
    assert!(!harness.sleeping_nodes.contains_key(&node_id));
    assert!(!harness.sleeping_by_instance.contains_key(&instance_id));
    assert!(!harness.blocked_until_by_instance.contains_key(&instance_id));

    let cmd = shard_rx.try_recv().expect("evict command should be sent");
    let shard::Command::Evict(ids) = cmd else {
        panic!("expected Evict command");
    };
    assert_eq!(ids, vec![instance_id]);
}

#[tokio::test]
async fn does_not_evict_when_inflight_actions_exist() {
    let instance_id = Uuid::new_v4();

    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = std_mpsc::channel::<shard::Command>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(instance_id, 0);
    harness.inflight_actions.insert(instance_id, 2);
    harness.blocked_until_by_instance =
        HashMap::from([(instance_id, Utc::now() + chrono::Duration::seconds(30))]);

    let result = super::handle(harness.params()).await;

    assert!(result.is_ok());
    assert!(harness.executor_shards.contains_key(&instance_id));
    assert_eq!(harness.inflight_actions.get(&instance_id), Some(&2));
    assert!(harness.blocked_until_by_instance.contains_key(&instance_id));
    assert!(shard_rx.try_recv().is_err(), "no evict command expected");
}
