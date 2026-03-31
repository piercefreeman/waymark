use std::collections::HashSet;

use waymark_ids::InstanceId;

use crate::commit_barrier::CommitBarrier;
use crate::{persist, shard};

struct TestHarness {
    pub shutdown: tokio_util::sync::CancellationToken,
    pub persist_tx: tokio::sync::mpsc::Sender<persist::Command>,
    pub commit_barrier: CommitBarrier<shard::Step>,
}

impl Default for TestHarness {
    fn default() -> Self {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (persist_tx, persist_rx) = tokio::sync::mpsc::channel(1);
        drop(persist_rx);
        Self {
            shutdown,
            persist_tx,
            commit_barrier: CommitBarrier::new(),
        }
    }
}

impl TestHarness {
    fn params<'a>(&'a mut self, all_steps: Vec<shard::Step>) -> super::Params<'a> {
        super::Params {
            shutdown_signal: self.shutdown.cancelled(),
            persist_tx: &self.persist_tx,
            commit_barrier: &mut self.commit_barrier,
            all_steps,
        }
    }
}

#[tokio::test]
async fn submit_failure_rolls_back_batch_membership() {
    let instance_id = InstanceId::new_uuid_v4();
    let step = shard::Step {
        executor_id: instance_id,
        actions: Vec::new(),
        sleep_requests: Vec::new(),
        updates: None,
        instance_done: None,
    };

    let mut harness = TestHarness::default();
    let result = super::handle(harness.params(vec![step])).await;

    assert!(matches!(result, Err(super::Error::SubmittingPersistBatch)));
    assert_eq!(harness.commit_barrier.pending_batch_count(), 0);

    let batch_id = harness
        .commit_barrier
        .register_batch(HashSet::from([instance_id]), Vec::new());
    let batch = harness
        .commit_barrier
        .take_batch(batch_id)
        .expect("batch should exist");
    assert!(
        batch.instance_ids.contains(&instance_id),
        "batch membership should not stay pruned after rollback"
    );
}
