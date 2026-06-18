use sqlx::PgPool;

use super::PostgresBackend;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_support_test::postgres_setup;
use waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntime;

pub(super) async fn setup_backend() -> PostgresBackend {
    let pool = postgres_setup().await;
    reset_database(&pool).await;
    PostgresBackend::new(pool)
}

/// Snapshot bytes written by [`register_test_vm`].
pub(super) const TEST_VM_SNAPSHOT: &[u8] = b"test-snapshot";

/// Register a VM runtime (its snapshot and workload pinning rows) through the
/// production [`RegisterVmRuntime`] path and return its identifiers, so tests
/// share exactly the registration behavior they exercise.
pub(super) async fn register_test_vm(backend: &PostgresBackend) -> (InstanceId, WorkflowVersionId) {
    let vm_id = InstanceId::new_uuid_v4();
    let executable_id = WorkflowVersionId::new_uuid_v4();
    backend
        .register_vm_runtime(&vm_id, &executable_id, TEST_VM_SNAPSHOT)
        .await
        .expect("register test vm");
    (vm_id, executable_id)
}

pub(super) async fn reset_database(pool: &PgPool) {
    sqlx::query(
        r#"
        TRUNCATE runner_actions_done,
                 queued_instances,
                 runner_instances,
                 vm_executables,
                 vm_runtime_snapshots,
                 workflow_schedules,
                 workflow_versions,
                 workload_pinnings,
                 worker_status
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await
    .expect("truncate postgres tables");
}
