use nonempty_collections::NESlice;
use serial_test::serial;

use super::super::test_helpers::setup_backend;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_workflow_service_vm_runtimes_backend::{FindExistingVmRuntimes, RegisterVmRuntime};

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

#[serial(postgres)]
#[tokio::test]
async fn find_existing_vm_runtimes_returns_only_registered() {
    let backend = setup_backend().await;
    let executable_id = WorkflowVersionId::new_uuid_v4();

    let registered = InstanceId::new_uuid_v4();
    let unregistered = InstanceId::new_uuid_v4();

    backend
        .register_vm_runtime(&registered, &executable_id, b"snapshot")
        .await
        .expect("register");

    let existing = backend
        .find_existing_vm_runtimes(
            NESlice::try_from_slice(&[registered, unregistered]).expect("non-empty"),
        )
        .await
        .expect("find existing");

    assert_eq!(existing, vec![registered]);
}

#[serial(postgres)]
#[tokio::test]
async fn find_existing_vm_runtimes_empty_when_none_registered() {
    let backend = setup_backend().await;

    let existing = backend
        .find_existing_vm_runtimes(
            NESlice::try_from_slice(&[InstanceId::new_uuid_v4()]).expect("non-empty"),
        )
        .await
        .expect("find existing");

    assert!(existing.is_empty());
}
