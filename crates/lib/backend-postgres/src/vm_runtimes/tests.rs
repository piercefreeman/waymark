use serial_test::serial;

use super::super::test_helpers::{TEST_VM_SNAPSHOT, register_test_vm, setup_backend};
use waymark_ids::InstanceId;

#[serial(postgres)]
#[tokio::test]
async fn store_and_load_snapshot_happy_path() {
    let backend = setup_backend().await;
    let (vm_id, executable_id) = register_test_vm(&backend).await;
    let updated_snapshot = b"updated-snapshot";

    let payload =
        waymark_state_vm_runtimes_backend::LoadForRevive::load_for_revive(&backend, &vm_id)
            .await
            .expect("load for revive");
    assert_eq!(payload.snapshot, TEST_VM_SNAPSHOT);
    assert_eq!(payload.executable_id, executable_id);

    waymark_state_vm_runtimes_backend::StoreSnapshots::store_snapshots(
        &backend,
        &[waymark_state_vm_runtimes_backend::StoreSnapshotsItem {
            vm_id: &vm_id,
            snapshot: updated_snapshot,
        }],
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
async fn store_snapshots_multi_item_batch_updates_each_vm() {
    let backend = setup_backend().await;
    let (vm_a, executable_a) = register_test_vm(&backend).await;
    let (vm_b, executable_b) = register_test_vm(&backend).await;
    let unregistered = InstanceId::new_uuid_v4();

    // One batch spanning two registered VMs plus an unregistered one; the
    // unregistered item matches no row and must not disturb the others.
    waymark_state_vm_runtimes_backend::StoreSnapshots::store_snapshots(
        &backend,
        &[
            waymark_state_vm_runtimes_backend::StoreSnapshotsItem {
                vm_id: &vm_a,
                snapshot: b"snapshot-a",
            },
            waymark_state_vm_runtimes_backend::StoreSnapshotsItem {
                vm_id: &vm_b,
                snapshot: b"snapshot-b",
            },
            waymark_state_vm_runtimes_backend::StoreSnapshotsItem {
                vm_id: &unregistered,
                snapshot: b"snapshot-lost",
            },
        ],
    )
    .await
    .expect("store snapshots");

    let payload =
        waymark_state_vm_runtimes_backend::LoadForRevive::load_for_revive(&backend, &vm_a)
            .await
            .expect("load for revive");
    assert_eq!(payload.snapshot, b"snapshot-a");
    assert_eq!(payload.executable_id, executable_a);

    let payload =
        waymark_state_vm_runtimes_backend::LoadForRevive::load_for_revive(&backend, &vm_b)
            .await
            .expect("load for revive");
    assert_eq!(payload.snapshot, b"snapshot-b");
    assert_eq!(payload.executable_id, executable_b);

    let result =
        waymark_state_vm_runtimes_backend::LoadForRevive::load_for_revive(&backend, &unregistered)
            .await;
    assert!(matches!(
        result,
        Err(super::error::LoadForReviveError::NotFound(_))
    ));
}

#[serial(postgres)]
#[tokio::test]
async fn store_snapshots_before_register_is_a_noop() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    // An unregistered VM matches no row: the batch succeeds and stores
    // nothing, rather than erroring.
    waymark_state_vm_runtimes_backend::StoreSnapshots::store_snapshots(
        &backend,
        &[waymark_state_vm_runtimes_backend::StoreSnapshotsItem {
            vm_id: &vm_id,
            snapshot: b"data",
        }],
    )
    .await
    .expect("store snapshots is a no-op for an unregistered vm");

    let result =
        waymark_state_vm_runtimes_backend::LoadForRevive::load_for_revive(&backend, &vm_id).await;
    assert!(matches!(
        result,
        Err(super::error::LoadForReviveError::NotFound(_))
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
