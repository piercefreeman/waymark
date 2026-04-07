use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::InstanceDone;
use waymark_ids::{ExecutionId, InstanceId};
use waymark_runner_executor_core::{ExecutionException, ExecutionSuccess};

struct TestHarness {
    pub backend: MemoryBackend,
    pub pending: Vec<InstanceDone>,
}

impl Default for TestHarness {
    fn default() -> Self {
        Self {
            backend: MemoryBackend::new(),
            pending: Vec::new(),
        }
    }
}

impl TestHarness {
    fn params<'a>(&'a mut self) -> super::Params<'a, MemoryBackend> {
        super::Params {
            core_backend: &self.backend,
            pending: &mut self.pending,
        }
    }
}

#[tokio::test]
async fn empty_pending_is_noop() {
    let mut harness = TestHarness::default();

    let result = super::run(harness.params()).await;

    assert!(result.is_ok());
    assert!(harness.pending.is_empty());
    assert!(harness.backend.instances_done().is_empty());
}

#[tokio::test]
async fn non_empty_pending_is_flushed_and_drained() {
    let mut harness = TestHarness::default();
    let first = InstanceDone {
        executor_id: InstanceId::new_uuid_v4(),
        entry_node: ExecutionId::new_uuid_v4(),
        result: Some(ExecutionSuccess(serde_json::json!(1))),
        error: None,
    };
    let second = InstanceDone {
        executor_id: InstanceId::new_uuid_v4(),
        entry_node: ExecutionId::new_uuid_v4(),
        result: None,
        error: Some(ExecutionException(
            serde_json::json!({"type": "ExecutionError"}),
        )),
    };
    harness.pending = vec![first.clone(), second.clone()];

    let result = super::run(harness.params()).await;

    assert!(result.is_ok());
    assert!(
        harness.pending.is_empty(),
        "pending buffer should be drained"
    );

    let persisted = harness.backend.instances_done();
    assert_eq!(persisted.len(), 2);
    assert_eq!(persisted[0].executor_id, first.executor_id);
    assert_eq!(persisted[1].executor_id, second.executor_id);
}
