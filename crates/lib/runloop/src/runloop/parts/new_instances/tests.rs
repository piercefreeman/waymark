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
use crate::runloop::{InflightActionDispatch, ShardCommand, ShardStep};

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

    let backend = MemoryBackend::new();
    let workflow_version_id = backend
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

    let mut executor_shards = HashMap::from([(instance_id, 0usize), (other_instance_id, 1usize)]);
    let (shard_tx0, shard_rx0) = std_mpsc::channel::<ShardCommand>();
    let (shard_tx1, _shard_rx1) = std_mpsc::channel::<ShardCommand>();
    let shard_senders = [shard_tx0, shard_tx1];

    let lock_tracker = InstanceLockTracker::new(Uuid::new_v4());
    let mut inflight_actions = HashMap::from([(instance_id, 3usize)]);
    let mut inflight_dispatches = HashMap::from([
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
    let mut sleeping_nodes = HashMap::from([(
        stale_node,
        waymark_runner::SleepRequest {
            node_id: stale_node,
            wake_at: chrono::Utc::now() + chrono::Duration::seconds(30),
        },
    )]);
    let mut sleeping_by_instance = HashMap::from([(instance_id, HashSet::from([stale_node]))]);
    let mut blocked_until_by_instance = HashMap::from([(
        instance_id,
        chrono::Utc::now() + chrono::Duration::seconds(30),
    )]);
    let mut commit_barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let batch_id =
        commit_barrier.register_batch(HashSet::from([instance_id, other_instance_id]), vec![]);

    let mut workflow_cache: HashMap<Uuid, Arc<waymark_dag::DAG>> = HashMap::new();
    let mut instances_idle = false;
    let mut next_shard = 0usize;

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
        workflow_cache: &mut workflow_cache,
        registry_backend: &backend,
        instances_idle: &mut instances_idle,
        next_shard: &mut next_shard,
        shard_count: 2,
        all_instances: vec![QueuedInstance {
            workflow_version_id,
            schedule_id: None,
            dag: None,
            entry_node: Uuid::new_v4(),
            state: None,
            action_results: HashMap::new(),
            instance_id,
            scheduled_at: None,
        }],
        saw_empty_instances: false,
    })
    .await;

    assert!(result.is_ok());
    assert_eq!(
        executor_shards.get(&instance_id),
        Some(&0usize),
        "reclaimed instance should keep prior shard"
    );
    assert_eq!(
        inflight_actions.get(&instance_id),
        Some(&0usize),
        "reclaimed instance resets inflight count"
    );
    assert!(!sleeping_by_instance.contains_key(&instance_id));
    assert!(!sleeping_nodes.contains_key(&stale_node));
    assert!(!blocked_until_by_instance.contains_key(&instance_id));

    assert!(
        inflight_dispatches
            .values()
            .all(|dispatch| dispatch.executor_id != instance_id),
        "stale dispatches should be pruned for reclaimed instance"
    );
    assert!(
        inflight_dispatches
            .values()
            .any(|dispatch| dispatch.executor_id == other_instance_id),
        "other executor dispatches should remain"
    );

    let batch = commit_barrier
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
    let ShardCommand::AssignInstances(batch) = cmd else {
        panic!("expected AssignInstances command");
    };
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].instance_id, instance_id);
}
