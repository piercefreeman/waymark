use serial_test::serial;
use waymark_state_vm_executables_backend::LoadExecutable as _;
use waymark_workflow_service_vm_executables_backend::UpsertExecutable as _;

use crate::test_helpers::setup_backend;

#[serial(postgres)]
#[tokio::test]
async fn load_missing_executable_returns_not_found() {
    let backend = setup_backend().await;
    let id = waymark_ids::WorkflowVersionId::new_uuid_v4();

    let result = backend.load_executable(&id).await;

    assert!(matches!(result, Err(super::error::LoadError::NotFound(_))));
}

#[serial(postgres)]
#[tokio::test]
async fn load_after_upsert_returns_bytes() {
    let backend = setup_backend().await;
    let bytes = b"test bytecode";

    let id = backend
        .upsert_executable("test_wf", "v1", bytes)
        .await
        .expect("upsert");

    let loaded = backend.load_executable(&id).await.expect("load");

    assert_eq!(loaded, bytes);
}
