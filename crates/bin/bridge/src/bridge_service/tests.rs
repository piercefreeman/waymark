use uuid::Uuid;

use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::{CoreBackend as _, InstanceDone};

use super::find_latest_instance_done;

#[tokio::test]
async fn find_latest_instance_done_returns_most_recent_match() {
    let backend = MemoryBackend::new();

    let target_instance = Uuid::new_v4();
    let other_instance = Uuid::new_v4();
    let shared_entry = Uuid::new_v4();

    let first_target = InstanceDone {
        executor_id: target_instance,
        entry_node: shared_entry,
        result: Some(serde_json::json!({"value": 1})),
        error: None,
    };
    let other = InstanceDone {
        executor_id: other_instance,
        entry_node: shared_entry,
        result: Some(serde_json::json!({"value": 2})),
        error: None,
    };
    let latest_target = InstanceDone {
        executor_id: target_instance,
        entry_node: shared_entry,
        result: Some(serde_json::json!({"value": 3})),
        error: None,
    };

    backend
        .save_instances_done(&[first_target, other, latest_target.clone()])
        .await
        .expect("save_instances_done should succeed");

    let selected = find_latest_instance_done(&backend, target_instance)
        .expect("target instance should have a done record");

    assert_eq!(selected.executor_id, target_instance);
    assert_eq!(selected.result, latest_target.result);
}

#[tokio::test]
async fn find_latest_instance_done_returns_none_when_missing() {
    let backend = MemoryBackend::new();

    backend
        .save_instances_done(&[InstanceDone {
            executor_id: Uuid::new_v4(),
            entry_node: Uuid::new_v4(),
            result: Some(serde_json::json!({"value": "other"})),
            error: None,
        }])
        .await
        .expect("save_instances_done should succeed");

    let missing = find_latest_instance_done(&backend, Uuid::new_v4());

    assert!(missing.is_none());
}
