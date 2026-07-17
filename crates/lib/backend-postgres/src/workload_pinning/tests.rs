use std::num::NonZeroUsize;

use chrono::Duration;
use serial_test::serial;
use uuid::Uuid;
use waymark_ids::InstanceId;

use super::super::test_helpers::{register_test_vm, setup_backend};

fn test_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

fn test_pinning(
    node_id: Uuid,
) -> waymark_workload_pinning_backend::Pinning<Uuid, chrono::DateTime<chrono::Utc>> {
    waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() + Duration::seconds(30),
    }
}

fn test_max_items() -> NonZeroUsize {
    NonZeroUsize::new(10).expect("10 > 0")
}

#[serial(postgres)]
#[tokio::test]
async fn poll_pins_newly_registered_vm() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    let result = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await
    .expect("poll unpinned")
    .expect("workloads available");

    let ids: Vec<InstanceId> = result.into_iter().collect();
    assert_eq!(ids, vec![vm_id]);
}

#[serial(postgres)]
#[tokio::test]
async fn poll_skips_already_pinned_vm() {
    let backend = setup_backend().await;
    let (_vm_id, _executable_id) = register_test_vm(&backend).await;

    // First poll pins the VM.
    let result = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await
    .expect("first poll")
    .expect("workloads available");
    assert_eq!(result.len().get(), 1);

    // Second poll should find nothing.
    let result = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await;
    assert!(matches!(result, Ok(None)));
}

#[serial(postgres)]
#[tokio::test]
async fn poll_picks_up_expired_pinning() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Pin with a short expiry.
    let short_pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() - Duration::seconds(1),
    };
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        short_pinning,
        test_max_items(),
    )
    .await
    .expect("pin with short expiry")
    .expect("workloads available");

    // Now poll — the expired pinning should be picked up.
    let result = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(node_id),
        test_max_items(),
    )
    .await
    .expect("poll after expiry")
    .expect("workloads available");
    let ids: Vec<InstanceId> = result.into_iter().collect();
    assert_eq!(ids, vec![vm_id]);
}

#[serial(postgres)]
#[tokio::test]
async fn refresh_updates_expiry() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Pin the VM.
    let original_pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() + Duration::seconds(10),
    };
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        original_pinning,
        test_max_items(),
    )
    .await
    .expect("pin vm")
    .expect("workloads available");

    // Refresh with a later expiry.
    let new_pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() + Duration::seconds(60),
    };
    let statuses = waymark_workload_pinning_backend::KeepalivePinnings::refresh_pinnings(
        &backend,
        test_now(),
        new_pinning,
        nonempty_collections::NEVec::new(vm_id),
    )
    .await
    .expect("refresh pinnings");

    assert_eq!(statuses.len().get(), 1);
    assert!(statuses.first().pinning.is_some());
}

#[serial(postgres)]
#[tokio::test]
async fn refresh_re_fences_expired_but_still_owned_pinning() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Pin the VM with an already-lapsed expiry, but let nobody steal it —
    // the pinning is expired yet still owned by this node.
    let expired_pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() - Duration::seconds(1),
    };
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        expired_pinning,
        test_max_items(),
    )
    .await
    .expect("pin vm")
    .expect("workloads available");

    // A late heartbeat must still be able to re-fence it, since ownership was
    // never contested.
    let renewed_pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() + Duration::seconds(60),
    };
    let statuses = waymark_workload_pinning_backend::KeepalivePinnings::refresh_pinnings(
        &backend,
        test_now(),
        renewed_pinning,
        nonempty_collections::NEVec::new(vm_id),
    )
    .await
    .expect("refresh pinnings");
    assert!(statuses.first().pinning.is_some());

    // Re-fenced with the future expiry, so a subsequent poll can no longer
    // pin it.
    let poll = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await;
    assert!(matches!(poll, Ok(None)));
}

#[serial(postgres)]
#[tokio::test]
async fn refresh_returns_none_for_lost_pinning() {
    let backend = setup_backend().await;
    let node_a = Uuid::new_v4();
    let node_b = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Node A pins the VM with a short expiry.
    let short_pinning = waymark_workload_pinning_backend::Pinning {
        node_id: node_a,
        expires_at: test_now() - Duration::seconds(1),
    };
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        short_pinning,
        test_max_items(),
    )
    .await
    .expect("node a pin")
    .expect("workloads available");

    // Node B steals it.
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(node_b),
        test_max_items(),
    )
    .await
    .expect("node b pin")
    .expect("workloads available");

    // Node A tries to refresh — should get None.
    let statuses = waymark_workload_pinning_backend::KeepalivePinnings::refresh_pinnings(
        &backend,
        test_now(),
        test_pinning(node_a),
        nonempty_collections::NEVec::new(vm_id),
    )
    .await
    .expect("refresh pinnings");
    assert!(statuses.first().pinning.is_none());
}

#[serial(postgres)]
#[tokio::test]
async fn refresh_returns_mixed_statuses_for_refreshed_and_lost() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_a, _executable_a) = register_test_vm(&backend).await;
    let (vm_b, _executable_b) = register_test_vm(&backend).await;

    // Pin both VMs.
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(node_id),
        NonZeroUsize::new(5).unwrap(),
    )
    .await
    .expect("pin both vms")
    .expect("workloads available");

    // Unpin vm_b in release mode so it loses its pinning.
    waymark_workload_pinning_backend::UnpinWorkloads::unpin_workloads(
        &backend,
        node_id,
        nonempty_collections::NEVec::new((vm_b, waymark_workload_pinning_core::UnpinMode::Release)),
    )
    .await
    .expect("unpin vm_b");

    // Refresh both — vm_a should stay pinned, vm_b should be None.
    let mut ids = nonempty_collections::NEVec::new(vm_a);
    ids.push(vm_b);
    let statuses = waymark_workload_pinning_backend::KeepalivePinnings::refresh_pinnings(
        &backend,
        test_now(),
        test_pinning(node_id),
        ids,
    )
    .await
    .expect("refresh pinnings");

    assert_eq!(statuses.len().get(), 2);
    let by_id: std::collections::HashMap<_, _> = statuses
        .into_iter()
        .map(|s| (s.workload_id, s.pinning))
        .collect();
    assert!(by_id[&vm_a].is_some());
    assert!(by_id[&vm_b].is_none());
}

#[serial(postgres)]
#[tokio::test]
async fn unpin_release_keeps_workload_runnable() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Pin the VM.
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(node_id),
        test_max_items(),
    )
    .await
    .expect("pin vm")
    .expect("workloads available");

    // Unpin in release mode.
    waymark_workload_pinning_backend::UnpinWorkloads::unpin_workloads(
        &backend,
        node_id,
        nonempty_collections::NEVec::new((
            vm_id,
            waymark_workload_pinning_core::UnpinMode::Release,
        )),
    )
    .await
    .expect("unpin release");

    // Poll again — the workload stayed runnable and is pinnable again.
    let result = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await
    .expect("poll after release-unpin")
    .expect("workloads available");
    let ids: Vec<InstanceId> = result.into_iter().collect();
    assert_eq!(ids, vec![vm_id]);
}

#[serial(postgres)]
#[tokio::test]
async fn unpin_park_removes_workload_from_runnable_set() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Pin the VM.
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(node_id),
        test_max_items(),
    )
    .await
    .expect("pin vm")
    .expect("workloads available");

    // Unpin in park mode.
    waymark_workload_pinning_backend::UnpinWorkloads::unpin_workloads(
        &backend,
        node_id,
        nonempty_collections::NEVec::new((vm_id, waymark_workload_pinning_core::UnpinMode::Park)),
    )
    .await
    .expect("unpin park");

    // Poll again — the workload left the runnable set.
    let result = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await;
    assert!(matches!(result, Ok(None)));

    // The snapshot survives parking — only the runnable-workload row is gone.
    let snapshots: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM vm_runtime_snapshots WHERE vm_id = $1")
            .bind(vm_id)
            .fetch_one(backend.pool())
            .await
            .expect("count snapshots");
    assert_eq!(snapshots, 1);
}

#[serial(postgres)]
#[tokio::test]
async fn refresh_reports_parked_workload_as_lost() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Pin the VM.
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(node_id),
        test_max_items(),
    )
    .await
    .expect("pin vm")
    .expect("workloads available");

    // Park it — the runnable-workload row is gone.
    waymark_workload_pinning_backend::UnpinWorkloads::unpin_workloads(
        &backend,
        node_id,
        nonempty_collections::NEVec::new((vm_id, waymark_workload_pinning_core::UnpinMode::Park)),
    )
    .await
    .expect("unpin park");

    // A late heartbeat racing the park must report the pinning as lost.
    let statuses = waymark_workload_pinning_backend::KeepalivePinnings::refresh_pinnings(
        &backend,
        test_now(),
        test_pinning(node_id),
        nonempty_collections::NEVec::new(vm_id),
    )
    .await
    .expect("refresh pinnings");
    assert!(statuses.first().pinning.is_none());
}

#[serial(postgres)]
#[tokio::test]
async fn unpin_mixed_batch_releases_and_parks() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_a, _executable_a) = register_test_vm(&backend).await;
    let (vm_b, _executable_b) = register_test_vm(&backend).await;

    // Pin both VMs.
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(node_id),
        test_max_items(),
    )
    .await
    .expect("pin both vms")
    .expect("workloads available");

    // Release vm_a and park vm_b in a single batch.
    let mut workloads =
        nonempty_collections::NEVec::new((vm_a, waymark_workload_pinning_core::UnpinMode::Release));
    workloads.push((vm_b, waymark_workload_pinning_core::UnpinMode::Park));
    waymark_workload_pinning_backend::UnpinWorkloads::unpin_workloads(&backend, node_id, workloads)
        .await
        .expect("unpin mixed batch");

    // Poll again — only the released workload comes back.
    let result = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await
    .expect("poll after mixed unpin")
    .expect("workloads available");
    let ids: Vec<InstanceId> = result.into_iter().collect();
    assert_eq!(ids, vec![vm_a]);
}

#[serial(postgres)]
#[tokio::test]
async fn unpin_from_non_owner_is_noop() {
    let backend = setup_backend().await;
    let node_a = Uuid::new_v4();
    let node_b = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Node A pins the VM.
    waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(node_a),
        test_max_items(),
    )
    .await
    .expect("node a pin")
    .expect("workloads available");

    // Node B tries to park it — the ownership fence makes this a no-op.
    waymark_workload_pinning_backend::UnpinWorkloads::unpin_workloads(
        &backend,
        node_b,
        nonempty_collections::NEVec::new((vm_id, waymark_workload_pinning_core::UnpinMode::Park)),
    )
    .await
    .expect("unpin from non-owner");

    // Node A still owns the pinning.
    let statuses = waymark_workload_pinning_backend::KeepalivePinnings::refresh_pinnings(
        &backend,
        test_now(),
        test_pinning(node_a),
        nonempty_collections::NEVec::new(vm_id),
    )
    .await
    .expect("refresh pinnings");
    assert!(statuses.first().pinning.is_some());
}

#[serial(postgres)]
#[tokio::test]
async fn unpin_absent_workload_is_noop() {
    let backend = setup_backend().await;

    let result = waymark_workload_pinning_backend::UnpinWorkloads::unpin_workloads(
        &backend,
        Uuid::new_v4(),
        nonempty_collections::NEVec::new((
            InstanceId::new_uuid_v4(),
            waymark_workload_pinning_core::UnpinMode::Park,
        )),
    )
    .await;
    assert!(result.is_ok());
}

#[serial(postgres)]
#[tokio::test]
async fn deregister_removes_vm_from_poll() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    sqlx::query("DELETE FROM vm_runtime_snapshots WHERE vm_id = $1")
        .bind(vm_id)
        .execute(backend.pool())
        .await
        .expect("delete vm runtime snapshot");

    let result = waymark_workload_pinning_backend::PollUnpinnedWorkloads::poll_unpinned(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await;
    assert!(matches!(result, Ok(None)));
}
