use serial_test::serial;
use waymark_workflow_service_vm_executables_backend::Error;
use waymark_workflow_service_vm_executables_backend::UpsertExecutable as _;

use crate::test_helpers::setup_backend;

#[serial(postgres)]
#[tokio::test]
async fn upsert_new_executable_returns_id() {
    let backend = setup_backend().await;

    let id = backend
        .upsert_executable("test_wf", "v1", b"bytecode")
        .await
        .expect("first upsert");

    // Verify the returned id is valid (not nil UUID).
    assert_ne!(id.to_string(), "00000000-0000-0000-0000-000000000000");
}

#[serial(postgres)]
#[tokio::test]
async fn upsert_same_bytecode_returns_existing_id() {
    let backend = setup_backend().await;

    let first_id = backend
        .upsert_executable("test_wf", "v1", b"bytecode")
        .await
        .expect("first upsert");

    let second_id = backend
        .upsert_executable("test_wf", "v1", b"bytecode")
        .await
        .expect("second upsert");

    assert_eq!(first_id, second_id);
}

#[serial(postgres)]
#[tokio::test]
async fn upsert_different_bytecode_returns_conflict() {
    let backend = setup_backend().await;

    backend
        .upsert_executable("test_wf", "v1", b"first")
        .await
        .expect("first upsert");

    let result = backend.upsert_executable("test_wf", "v1", b"second").await;

    assert!(matches!(
        result,
        Err(ref e) if matches!(e.kind(), waymark_workflow_service_vm_executables_backend::ErrorKind::Conflict)
    ));
}

#[serial(postgres)]
#[tokio::test]
async fn upsert_different_names_are_independent() {
    let backend = setup_backend().await;

    let id_a = backend
        .upsert_executable("wf_a", "v1", b"bytes")
        .await
        .expect("upsert wf_a");

    let id_b = backend
        .upsert_executable("wf_b", "v1", b"bytes")
        .await
        .expect("upsert wf_b");

    assert_ne!(id_a, id_b);
}

#[serial(postgres)]
#[tokio::test]
async fn upsert_different_versions_are_independent() {
    let backend = setup_backend().await;

    let id_v1 = backend
        .upsert_executable("test_wf", "v1", b"bytes")
        .await
        .expect("upsert v1");

    let id_v2 = backend
        .upsert_executable("test_wf", "v2", b"bytes")
        .await
        .expect("upsert v2");

    assert_ne!(id_v1, id_v2);
}
