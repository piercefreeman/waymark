use serial_test::serial;

use super::super::test_helpers::setup_backend;
use waymark_ids::{InstanceId, WorkflowVersionId};

#[serial(postgres)]
#[tokio::test]
async fn store_and_load_snapshot_happy_path() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();
    let executable_id = WorkflowVersionId::new_uuid_v4();
    let initial_snapshot = b"initial-snapshot";
    let updated_snapshot = b"updated-snapshot";

    // Insert the initial snapshot and workload pinning rows directly
    // for precise control over the test scenario.
    sqlx::query(
        "INSERT INTO vm_runtime_snapshots (vm_id, executable_id, snapshot) VALUES ($1, $2, $3)",
    )
    .bind(vm_id)
    .bind(executable_id)
    .bind(initial_snapshot)
    .execute(backend.pool())
    .await
    .expect("insert vm runtime snapshot");

    sqlx::query("INSERT INTO workload_pinnings (instance_id) VALUES ($1)")
        .bind(vm_id)
        .execute(backend.pool())
        .await
        .expect("insert workload pinning");

    let payload =
        waymark_state_vm_runtimes_backend::LoadForRevive::load_for_revive(&backend, &vm_id)
            .await
            .expect("load for revive");
    assert_eq!(payload.snapshot, initial_snapshot);
    assert_eq!(payload.executable_id, executable_id);

    waymark_state_vm_runtimes_backend::StoreSnapshot::store_snapshot(
        &backend,
        &vm_id,
        updated_snapshot,
    )
    .await
    .expect("store snapshot");

    let payload =
        waymark_state_vm_runtimes_backend::LoadForRevive::load_for_revive(&backend, &vm_id)
            .await
            .expect("load for revive");
    assert_eq!(payload.snapshot, updated_snapshot);
    assert_eq!(payload.executable_id, executable_id);
}

#[serial(postgres)]
#[tokio::test]
async fn store_snapshot_before_register_returns_error() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let result =
        waymark_state_vm_runtimes_backend::StoreSnapshot::store_snapshot(&backend, &vm_id, b"data")
            .await;
    assert!(matches!(
        result,
        Err(super::error::StoreSnapshotError::NotRegistered(_))
    ));
}

#[serial(postgres)]
#[tokio::test]
async fn load_for_revive_unknown_vm_returns_error() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let result =
        waymark_state_vm_runtimes_backend::LoadForRevive::load_for_revive(&backend, &vm_id).await;
    assert!(matches!(
        result,
        Err(super::error::LoadForReviveError::NotFound(_))
    ));
}
