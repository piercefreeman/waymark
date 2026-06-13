use std::num::NonZeroUsize;

use chrono::Duration;
use serial_test::serial;
use uuid::Uuid;
use waymark_ids::{InstanceId, WorkflowVersionId};

use super::super::test_helpers::setup_backend;

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

async fn register_test_vm(backend: &super::PostgresBackend) -> (InstanceId, WorkflowVersionId) {
    let vm_id = InstanceId::new_uuid_v4();
    let executable_id = WorkflowVersionId::new_uuid_v4();

    sqlx::query(
        "INSERT INTO vm_runtime_snapshots (vm_id, executable_id, snapshot) VALUES ($1, $2, $3)",
    )
    .bind(vm_id)
    .bind(executable_id)
    .bind(b"test-snapshot")
    .execute(backend.pool())
    .await
    .expect("insert vm runtime snapshot");

    sqlx::query("INSERT INTO workload_pinnings (instance_id) VALUES ($1)")
        .bind(vm_id)
        .execute(backend.pool())
        .await
        .expect("insert workload pinning");

    (vm_id, executable_id)
}

#[serial(postgres)]
#[tokio::test]
async fn poll_claims_newly_registered_vm() {
    let backend = setup_backend().await;
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    let result = waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await
    .expect("poll unlocked")
    .expect("instances available");

    let ids: Vec<InstanceId> = result.into_iter().collect();
    assert_eq!(ids, vec![vm_id]);
}

#[serial(postgres)]
#[tokio::test]
async fn poll_skips_already_pinned_vm() {
    let backend = setup_backend().await;
    let (_vm_id, _executable_id) = register_test_vm(&backend).await;

    // First poll claims the VM.
    let result = waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await
    .expect("first poll")
    .expect("instances available");
    assert_eq!(result.len().get(), 1);

    // Second poll should find nothing.
    let result = waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
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

    // Claim with a short expiry.
    let short_pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() - Duration::seconds(1),
    };
    waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        short_pinning,
        test_max_items(),
    )
    .await
    .expect("claim with short expiry")
    .expect("instances available");

    // Now poll — the expired pinning should be picked up.
    let result = waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        test_pinning(node_id),
        test_max_items(),
    )
    .await
    .expect("poll after expiry")
    .expect("instances available");
    let ids: Vec<InstanceId> = result.into_iter().collect();
    assert_eq!(ids, vec![vm_id]);
}

#[serial(postgres)]
#[tokio::test]
async fn refresh_updates_expiry() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Claim the VM.
    let original_pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() + Duration::seconds(10),
    };
    waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        original_pinning,
        test_max_items(),
    )
    .await
    .expect("claim vm")
    .expect("instances available");

    // Refresh with a later expiry.
    let new_pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at: test_now() + Duration::seconds(60),
    };
    let statuses = waymark_workload_pinning_backend::KeepaliveInstancePinnings::refresh_pinnings(
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
async fn refresh_returns_none_for_lost_pinning() {
    let backend = setup_backend().await;
    let node_a = Uuid::new_v4();
    let node_b = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Node A claims the VM with a short expiry.
    let short_pinning = waymark_workload_pinning_backend::Pinning {
        node_id: node_a,
        expires_at: test_now() - Duration::seconds(1),
    };
    waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        short_pinning,
        test_max_items(),
    )
    .await
    .expect("node a claim")
    .expect("instances available");

    // Node B steals it.
    waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        test_pinning(node_b),
        test_max_items(),
    )
    .await
    .expect("node b claim")
    .expect("instances available");

    // Node A tries to refresh — should get None.
    let statuses = waymark_workload_pinning_backend::KeepaliveInstancePinnings::refresh_pinnings(
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

    // Claim both VMs.
    waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        test_pinning(node_id),
        NonZeroUsize::new(5).unwrap(),
    )
    .await
    .expect("claim both vms")
    .expect("instances available");

    // Release vm_b so it loses its pinning.
    waymark_workload_pinning_backend::ReleasePinnings::release_pinnings(
        &backend,
        node_id,
        nonempty_collections::NEVec::new(vm_b),
    )
    .await
    .expect("release vm_b");

    // Refresh both — vm_a should stay pinned, vm_b should be None.
    let mut ids = nonempty_collections::NEVec::new(vm_a);
    ids.push(vm_b);
    let statuses = waymark_workload_pinning_backend::KeepaliveInstancePinnings::refresh_pinnings(
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
        .map(|s| (s.instance_id, s.pinning))
        .collect();
    assert!(by_id[&vm_a].is_some());
    assert!(by_id[&vm_b].is_none());
}

#[serial(postgres)]
#[tokio::test]
async fn release_clears_pinning() {
    let backend = setup_backend().await;
    let node_id = Uuid::new_v4();
    let (vm_id, _executable_id) = register_test_vm(&backend).await;

    // Claim the VM.
    waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        test_pinning(node_id),
        test_max_items(),
    )
    .await
    .expect("claim vm")
    .expect("instances available");

    // Release it.
    waymark_workload_pinning_backend::ReleasePinnings::release_pinnings(
        &backend,
        node_id,
        nonempty_collections::NEVec::new(vm_id),
    )
    .await
    .expect("release pinning");

    // Poll again — should pick it up since it's released.
    let result = waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        test_pinning(node_id),
        test_max_items(),
    )
    .await
    .expect("poll after release")
    .expect("instances available");
    let ids: Vec<InstanceId> = result.into_iter().collect();
    assert_eq!(ids, vec![vm_id]);
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

    let result = waymark_workload_pinning_backend::PollUnpinnedInstances::poll_unlocked(
        &backend,
        test_now(),
        test_pinning(Uuid::new_v4()),
        test_max_items(),
    )
    .await;
    assert!(matches!(result, Ok(None)));
}
