use serial_test::serial;

use super::super::test_helpers::setup_backend;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntime;

#[serial(postgres)]
#[tokio::test]
async fn register_duplicate_vm_returns_error() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();
    let executable_id = WorkflowVersionId::new_uuid_v4();

    backend
        .register_vm_runtime(&vm_id, &executable_id, b"first")
        .await
        .expect("first register");

    let result =
        RegisterVmRuntime::register_vm_runtime(&backend, &vm_id, &executable_id, b"second").await;
    assert!(matches!(
        result,
        Err(super::error::RegisterVmRuntimeError::AlreadyExists(_))
    ));
}
