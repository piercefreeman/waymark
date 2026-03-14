use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::mpsc as std_mpsc;

use prost::Message;
use sha2::{Digest, Sha256};
use uuid::Uuid;
use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::QueuedInstance;
use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowRegistryBackend};

use crate::commit_barrier::CommitBarrier;
use crate::lock::InstanceLockTracker;
use crate::runloop::InflightActionDispatch;
use crate::shard;

struct TestHarness {
    pub backend: MemoryBackend,
    pub lock_tracker: InstanceLockTracker,
    pub executor_shards: HashMap<Uuid, usize>,
    pub shard_senders: Vec<std_mpsc::Sender<shard::Command>>,
    pub inflight_actions: HashMap<Uuid, usize>,
    pub inflight_dispatches: HashMap<Uuid, InflightActionDispatch>,
    pub sleeping_nodes: HashMap<Uuid, waymark_runner::SleepRequest>,
    pub sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until_by_instance: HashMap<Uuid, chrono::DateTime<chrono::Utc>>,
    pub commit_barrier: CommitBarrier<shard::Step>,
    pub workflow_cache: HashMap<Uuid, Arc<waymark_dag::DAG>>,
    pub instances_idle: bool,
    pub next_shard: usize,
}

impl Default for TestHarness {
    fn default() -> Self {
        Self {
            backend: MemoryBackend::new(),
            lock_tracker: InstanceLockTracker::new(Uuid::new_v4()),
            executor_shards: HashMap::new(),
            shard_senders: Vec::new(),
            inflight_actions: HashMap::new(),
            inflight_dispatches: HashMap::new(),
            sleeping_nodes: HashMap::new(),
            sleeping_by_instance: HashMap::new(),
            blocked_until_by_instance: HashMap::new(),
            commit_barrier: CommitBarrier::new(),
            workflow_cache: HashMap::new(),
            instances_idle: false,
            next_shard: 0,
        }
    }
}

impl TestHarness {
    fn params<'a>(
        &'a mut self,
        all_instances: Vec<QueuedInstance>,
    ) -> super::Params<'a, MemoryBackend> {
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
            workflow_cache: &mut self.workflow_cache,
            registry_backend: &self.backend,
            instances_idle: &mut self.instances_idle,
            next_shard: &mut self.next_shard,
            shard_count: 2,
            all_instances,
            saw_empty_instances: false,
        }
    }
}

#[tokio::test]
async fn reclaimed_active_instance_prunes_stale_state_and_reuses_shard() {
    let source = r#"
fn main(input: [x], output: [y]):
    y = @tests.fixtures.test_actions.double(value=x)
    return y
"#;
    let program = waymark_ir_parser::parse_program(source.trim()).expect("parse program");
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));

    let mut harness = TestHarness::default();
    let workflow_version_id = harness
        .backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: "instances_reclaim".to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .expect("register workflow version");

    let instance_id = Uuid::new_v4();
    let other_instance_id = Uuid::new_v4();
    let stale_node = Uuid::new_v4();

    let (shard_tx0, shard_rx0) = std_mpsc::channel::<shard::Command>();
    let (shard_tx1, _shard_rx1) = std_mpsc::channel::<shard::Command>();
    harness.shard_senders = vec![shard_tx0, shard_tx1];
    harness.executor_shards = HashMap::from([(instance_id, 0usize), (other_instance_id, 1usize)]);
    harness.inflight_actions = HashMap::from([(instance_id, 3usize)]);
    harness.inflight_dispatches = HashMap::from([
        (
            Uuid::new_v4(),
            InflightActionDispatch {
                executor_id: instance_id,
                attempt_number: 1,
                dispatch_token: Uuid::new_v4(),
                timeout_seconds: 0,
                deadline_at: None,
            },
        ),
        (
            Uuid::new_v4(),
            InflightActionDispatch {
                executor_id: other_instance_id,
                attempt_number: 1,
                dispatch_token: Uuid::new_v4(),
                timeout_seconds: 0,
                deadline_at: None,
            },
        ),
    ]);
    harness.sleeping_nodes = HashMap::from([(
        stale_node,
        waymark_runner::SleepRequest {
            node_id: stale_node,
            wake_at: chrono::Utc::now() + chrono::Duration::seconds(30),
        },
    )]);
    harness.sleeping_by_instance = HashMap::from([(instance_id, HashSet::from([stale_node]))]);
    harness.blocked_until_by_instance = HashMap::from([(
        instance_id,
        chrono::Utc::now() + chrono::Duration::seconds(30),
    )]);
    let batch_id = harness
        .commit_barrier
        .register_batch(HashSet::from([instance_id, other_instance_id]), vec![]);

    let result = super::handle(harness.params(vec![QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: Uuid::new_v4(),
        state: None,
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    }]))
    .await;

    assert!(result.is_ok());
    assert_eq!(
        harness.executor_shards.get(&instance_id),
        Some(&0usize),
        "reclaimed instance should keep prior shard"
    );
    assert_eq!(
        harness.inflight_actions.get(&instance_id),
        Some(&0usize),
        "reclaimed instance resets inflight count"
    );
    assert!(!harness.sleeping_by_instance.contains_key(&instance_id));
    assert!(!harness.sleeping_nodes.contains_key(&stale_node));
    assert!(!harness.blocked_until_by_instance.contains_key(&instance_id));

    assert!(
        harness
            .inflight_dispatches
            .values()
            .all(|dispatch| dispatch.executor_id != instance_id),
        "stale dispatches should be pruned for reclaimed instance"
    );
    assert!(
        harness
            .inflight_dispatches
            .values()
            .any(|dispatch| dispatch.executor_id == other_instance_id),
        "other executor dispatches should remain"
    );

    let batch = harness
        .commit_barrier
        .take_batch(batch_id)
        .expect("batch should exist");
    assert!(
        !batch.instance_ids.contains(&instance_id),
        "reclaimed instance should be removed from pending barrier membership"
    );
    assert!(batch.instance_ids.contains(&other_instance_id));

    let cmd = shard_rx0
        .try_recv()
        .expect("instance assignment should be sent");
    let shard::Command::AssignInstances(batch) = cmd else {
        panic!("expected AssignInstances command");
    };
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].instance_id, instance_id);
}
