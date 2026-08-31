use serial_test::serial;

use super::super::test_helpers::{
    TEST_VM_SNAPSHOT, deadlock, register_test_vm, register_test_vm_with_id, setup_backend,
};
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

/// One choreographed collision of two concurrent snapshot-store batches
/// over the same two VMs — the vm_runtime_snapshots member of the
/// multi-row-writer class behind the 2026-08-31 deadlock outage
/// (reachable cross-node in the post-steal dual-ownership window, and
/// this table's second writer arrives the day a batched snapshot delete
/// lands).  Opposite batch orders must queue, not cycle.
///
/// The staging, plan-shape seeding, and contract follow
/// `complete_and_renew_across_a_staged_row` in
/// `action_call_requests::tests`: the leader's first VM row is pre-held
/// so it blocks holding nothing, the follower locks the other row and
/// queues behind it.  Both batches run the same statement text, so the
/// follower's blocked state is staged by waiter count.
async fn store_against_store_across_a_staged_row(leader_first: bool) {
    let backend = setup_backend().await;
    let mut vm_ids = [InstanceId::new_uuid_v4(), InstanceId::new_uuid_v4()];
    vm_ids.sort();
    let [vm_lo, vm_hi] = vm_ids;
    register_test_vm_with_id(&backend, vm_lo).await;
    register_test_vm_with_id(&backend, vm_hi).await;

    // Plan shape, as in the swept-table choreographies: enough analyzed
    // rows that the store statement probes the primary key in input
    // order instead of seq-scanning both contenders into one order.
    sqlx::query(
        r#"
        INSERT INTO vm_runtime_snapshots (vm_id, executable_id, snapshot)
        SELECT gen_random_uuid(), gen_random_uuid(), 'filler'
        FROM generate_series(1, 10000) AS n
        "#,
    )
    .execute(backend.pool())
    .await
    .expect("seed filler snapshots");
    sqlx::query("ANALYZE vm_runtime_snapshots")
        .execute(backend.pool())
        .await
        .expect("analyze");

    let (leader_order, follower_order) = if leader_first {
        ([vm_hi, vm_lo], [vm_lo, vm_hi])
    } else {
        ([vm_lo, vm_hi], [vm_hi, vm_lo])
    };

    // Pre-hold the leader batch's first VM row.
    let mut staging = backend.pool().begin().await.expect("begin staging");
    let held = sqlx::query("SELECT 1 FROM vm_runtime_snapshots WHERE vm_id = $1 FOR UPDATE")
        .bind(leader_order[0])
        .execute(&mut *staging)
        .await
        .expect("hold staged row");
    assert_eq!(
        held.rows_affected(),
        1,
        "the staged snapshot row must exist"
    );

    let spawn_store = |order: [InstanceId; 2]| {
        let backend = backend.clone();
        tokio::spawn(async move {
            waymark_state_vm_runtimes_backend::StoreSnapshots::store_snapshots(
                &backend,
                &[
                    waymark_state_vm_runtimes_backend::StoreSnapshotsItem {
                        vm_id: &order[0],
                        snapshot: b"stored",
                    },
                    waymark_state_vm_runtimes_backend::StoreSnapshotsItem {
                        vm_id: &order[1],
                        snapshot: b"stored",
                    },
                ],
            )
            .await
        })
    };
    // `SET snapshot` appears only in the store statement.
    let store_pattern = "%SET snapshot = b.snapshot%";

    let leader = spawn_store(leader_order);
    deadlock::wait_until_lock_blocked(backend.pool(), store_pattern).await;
    let follower = spawn_store(follower_order);
    deadlock::wait_until_lock_blocked_at_least(
        backend.pool(),
        store_pattern,
        std::num::NonZeroI64::new(2).unwrap(),
    )
    .await;

    staging.rollback().await.expect("release staged row");

    leader
        .await
        .expect("join leader")
        .expect("a snapshot-store batch must not deadlock against a competing store");
    follower
        .await
        .expect("join follower")
        .expect("a snapshot-store batch must not deadlock against a competing store");
}

#[serial(postgres)]
#[tokio::test]
async fn concurrent_snapshot_store_batches_do_not_deadlock() {
    // Both leader orders, with the staged row tracking the leader's
    // first key, so an ordering discipline missing from the store
    // statement fails one of the runs.
    store_against_store_across_a_staged_row(true).await;
    store_against_store_across_a_staged_row(false).await;
}
