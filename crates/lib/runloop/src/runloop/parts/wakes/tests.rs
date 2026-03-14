use std::collections::{HashMap, HashSet};
use std::sync::mpsc;

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_runner::SleepRequest;

use crate::commit_barrier::CommitBarrier;
use crate::runloop::{ShardCommand, ShardStep, SleepWake};

#[test]
fn ignores_unknown_node() {
    let executor_id = Uuid::new_v4();
    let unknown_node = Uuid::new_v4();
    let (tx, rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut sleeping_nodes: HashMap<Uuid, SleepRequest> = HashMap::new();
    let mut sleeping_by_instance: HashMap<Uuid, HashSet<Uuid>> = HashMap::new();
    let mut blocked_until: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        all_wakes: vec![SleepWake {
            executor_id,
            node_id: unknown_node,
        }],
    });

    assert!(rx.try_recv().is_err(), "no command should reach the shard");
}

#[test]
fn ignores_node_with_future_wake_at() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let future_wake = Utc::now() + chrono::Duration::seconds(60);
    let (tx, rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut sleeping_nodes = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: future_wake,
        },
    )]);
    let mut sleeping_by_instance = HashMap::from([(executor_id, HashSet::from([node_id]))]);
    let mut blocked_until = HashMap::from([(executor_id, future_wake)]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        all_wakes: vec![SleepWake {
            executor_id,
            node_id,
        }],
    });

    assert!(
        sleeping_nodes.contains_key(&node_id),
        "node should still be sleeping"
    );
    assert!(rx.try_recv().is_err(), "no command should reach the shard");
}

#[test]
fn routes_ready_node_to_shard() {
    let executor_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let past_wake = Utc::now() - chrono::Duration::seconds(5);
    let (tx, rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut sleeping_nodes = HashMap::from([(
        node_id,
        SleepRequest {
            node_id,
            wake_at: past_wake,
        },
    )]);
    let mut sleeping_by_instance = HashMap::from([(executor_id, HashSet::from([node_id]))]);
    let mut blocked_until = HashMap::from([(executor_id, past_wake)]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        all_wakes: vec![SleepWake {
            executor_id,
            node_id,
        }],
    });

    assert!(
        !sleeping_nodes.contains_key(&node_id),
        "node should be removed from sleeping set"
    );
    assert!(
        !sleeping_by_instance.contains_key(&executor_id),
        "instance tracking should be cleared"
    );
    assert!(
        !blocked_until.contains_key(&executor_id),
        "blocked-until should be cleared"
    );
    let cmd = rx.try_recv().expect("shard should receive a command");
    let ShardCommand::Wake(nodes) = cmd else {
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
    let (tx, rx) = mpsc::channel::<ShardCommand>();
    let senders = [tx];

    let mut executor_shards = HashMap::from([(executor_id, 0usize)]);
    let mut sleeping_nodes = HashMap::from([
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
    let mut sleeping_by_instance =
        HashMap::from([(executor_id, HashSet::from([first_node, second_node]))]);
    let mut blocked_until = HashMap::from([(executor_id, first_wake)]);
    let mut barrier: CommitBarrier<ShardStep> = CommitBarrier::new();

    super::handle(super::Params {
        executor_shards: &mut executor_shards,
        shard_senders: &senders,
        sleeping_nodes: &mut sleeping_nodes,
        sleeping_by_instance: &mut sleeping_by_instance,
        blocked_until_by_instance: &mut blocked_until,
        commit_barrier: &mut barrier,
        all_wakes: vec![SleepWake {
            executor_id,
            node_id: first_node,
        }],
    });

    assert!(!sleeping_nodes.contains_key(&first_node));
    assert!(sleeping_nodes.contains_key(&second_node));
    assert_eq!(blocked_until.get(&executor_id), Some(&second_wake));
    let cmd = rx.try_recv().expect("shard should receive wake command");
    let ShardCommand::Wake(nodes) = cmd else {
        panic!("expected Wake command");
    };
    assert_eq!(nodes, vec![first_node]);
}
