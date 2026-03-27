use std::collections::{HashMap, HashSet};
use std::sync::mpsc;

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_runner::SleepRequest;

use crate::commit_barrier::CommitBarrier;
use crate::runloop::SleepWake;
use crate::shard;

struct TestHarness {
    pub executor_shards: HashMap<Uuid, usize>,
    pub sleeping_nodes: HashMap<Uuid, SleepRequest>,
    pub sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>>,
    pub blocked_until: HashMap<Uuid, DateTime<Utc>>,
    pub barrier: CommitBarrier<shard::Step>,
    pub shard_senders: Vec<mpsc::Sender<waymark_timed::Opaque<shard::Command>>>,
}

impl Default for TestHarness {
    fn default() -> Self {
        Self {
            executor_shards: HashMap::new(),
            sleeping_nodes: HashMap::new(),
            sleeping_by_instance: HashMap::new(),
            blocked_until: HashMap::new(),
            barrier: CommitBarrier::new(),
            shard_senders: Vec::new(),
        }
    }
}

impl TestHarness {
    fn params<'a>(&'a mut self, all_wakes: Vec<SleepWake>) -> super::Params<'a> {
        super::Params {
            executor_shards: &mut self.executor_shards,
            shard_senders: &self.shard_senders,
            sleeping_nodes: &mut self.sleeping_nodes,
            sleeping_by_instance: &mut self.sleeping_by_instance,
            blocked_until_by_instance: &mut self.blocked_until,
            commit_barrier: &mut self.barrier,
            all_wakes,
        }
    }
}

#[test]
fn ignores_unknown_node() {
    let executor_id = Uuid::new_v4();
    let unknown_node = Uuid::new_v4();
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);

    super::handle(harness.params(vec![SleepWake {
        executor_id,
        node_id: unknown_node,
    }]));

    assert!(
        shard_rx.try_recv().is_err(),
        "no command should reach the shard"
    );
}

#[test]
fn ignores_node_with_future_wake_at() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let future_wake = Utc::now() + chrono::Duration::seconds(60);
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.sleeping_nodes = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: future_wake,
        },
    )]);
    harness.sleeping_by_instance = HashMap::from([(executor_id, HashSet::from([node_id]))]);
    harness.blocked_until = HashMap::from([(executor_id, future_wake)]);

    super::handle(harness.params(vec![SleepWake {
        executor_id,
        node_id,
    }]));

    assert!(
        harness.sleeping_nodes.contains_key(&node_id),
        "node should still be sleeping"
    );
    assert!(
        shard_rx.try_recv().is_err(),
        "no command should reach the shard"
    );
}

#[test]
fn routes_ready_node_to_shard() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let past_wake = Utc::now() - chrono::Duration::seconds(5);
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.sleeping_nodes = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: past_wake,
        },
    )]);
    harness.sleeping_by_instance = HashMap::from([(executor_id, HashSet::from([node_id]))]);
    harness.blocked_until = HashMap::from([(executor_id, past_wake)]);

    super::handle(harness.params(vec![SleepWake {
        executor_id,
        node_id,
    }]));

    assert!(
        !harness.sleeping_nodes.contains_key(&node_id),
        "node should be removed from sleeping set"
    );
    assert!(
        !harness.sleeping_by_instance.contains_key(&executor_id),
        "instance tracking should be cleared"
    );
    assert!(
        !harness.blocked_until.contains_key(&executor_id),
        "blocked-until should be cleared"
    );
    let cmd = shard_rx.try_recv().expect("shard should receive a command");
    let cmd = cmd.into_inner();
    let shard::Command::Wake(nodes) = cmd else {
        panic!("expected Wake command");
    };
    assert_eq!(nodes, vec![node_id]);
}

#[test]
fn waking_one_of_multiple_nodes_recomputes_blocked_until() {
    let executor_id = Uuid::new_v4();
    let first_node = Uuid::new_v4();
    let second_node = Uuid::new_v4();
    let first_wake = Utc::now() - chrono::Duration::seconds(5);
    let second_wake = Utc::now() + chrono::Duration::seconds(45);
    let mut harness = TestHarness::default();
    let (shard_tx, shard_rx) = mpsc::channel::<waymark_timed::Opaque<shard::Command>>();
    harness.shard_senders.push(shard_tx);
    harness.executor_shards.insert(executor_id, 0);
    harness.sleeping_nodes = HashMap::from([
        (
            first_node,
            SleepRequest {
                node_id: first_node,
                wake_at: first_wake,
            },
        ),
        (
            second_node,
            SleepRequest {
                node_id: second_node,
                wake_at: second_wake,
            },
        ),
    ]);
    harness.sleeping_by_instance =
        HashMap::from([(executor_id, HashSet::from([first_node, second_node]))]);
    harness.blocked_until = HashMap::from([(executor_id, first_wake)]);

    super::handle(harness.params(vec![SleepWake {
        executor_id,
        node_id: first_node,
    }]));

    assert!(!harness.sleeping_nodes.contains_key(&first_node));
    assert!(harness.sleeping_nodes.contains_key(&second_node));
    assert_eq!(harness.blocked_until.get(&executor_id), Some(&second_wake));
    let cmd = shard_rx
        .try_recv()
        .expect("shard should receive wake command");
    let cmd = cmd.into_inner();
    let shard::Command::Wake(nodes) = cmd else {
        panic!("expected Wake command");
    };
    assert_eq!(nodes, vec![first_node]);
}
