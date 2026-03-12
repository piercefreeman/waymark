use uuid::Uuid;
use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::InstanceDone;

#[tokio::test]
async fn empty_pending_is_noop() {
    let backend = MemoryBackend::new();
    let mut pending = Vec::new();

    let result = super::run(super::Params {
        core_backend: &backend,
        pending: &mut pending,
    })
    .await;

    assert!(result.is_ok());
    assert!(pending.is_empty());
    assert!(backend.instances_done().is_empty());
}

#[tokio::test]
async fn non_empty_pending_is_flushed_and_drained() {
    let backend = MemoryBackend::new();
    let first = InstanceDone {
        executor_id: Uuid::new_v4(),
        entry_node: Uuid::new_v4(),
        result: Some(serde_json::json!(1)),
        error: None,
    };
    let second = InstanceDone {
        executor_id: Uuid::new_v4(),
        entry_node: Uuid::new_v4(),
        result: None,
        error: Some(serde_json::json!({"type": "ExecutionError"})),
    };
    let mut pending = vec![first.clone(), second.clone()];

    let result = super::run(super::Params {
        core_backend: &backend,
        pending: &mut pending,
    })
    .await;

    assert!(result.is_ok());
    assert!(pending.is_empty(), "pending buffer should be drained");

    let persisted = backend.instances_done();
    assert_eq!(persisted.len(), 2);
    assert_eq!(persisted[0].executor_id, first.executor_id);
    assert_eq!(persisted[1].executor_id, second.executor_id);
}
