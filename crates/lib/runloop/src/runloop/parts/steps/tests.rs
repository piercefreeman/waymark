use std::collections::HashSet;

use uuid::Uuid;

use crate::commit_barrier::CommitBarrier;
use crate::runloop::ShardStep;

#[tokio::test]
async fn submit_failure_rolls_back_batch_membership() {
    let instance_id = Uuid::new_v4();
    let step = ShardStep {
        executor_id: instance_id,
        actions: Vec::new(),
        sleep_requests: Vec::new(),
        updates: None,
        instance_done: None,
    };

    let shutdown = tokio_util::sync::CancellationToken::new();
    let (persist_tx, persist_rx) = tokio::sync::mpsc::channel(1);
    drop(persist_rx);

    let mut commit_barrier: CommitBarrier<ShardStep> = CommitBarrier::new();
    let result = super::handle(super::Params {
        shutdown_signal: shutdown.cancelled(),
        persist_tx: &persist_tx,
        commit_barrier: &mut commit_barrier,
        all_steps: vec![step],
    })
    .await;

    assert!(matches!(result, Err(super::Error::SubmittingPersistBatch)));
    assert_eq!(commit_barrier.pending_batch_count(), 0);

    let batch_id = commit_barrier.register_batch(HashSet::from([instance_id]), Vec::new());
    let batch = commit_barrier
        .take_batch(batch_id)
        .expect("batch should exist");
    assert!(
        batch.instance_ids.contains(&instance_id),
        "batch membership should not stay pruned after rollback"
    );
}
