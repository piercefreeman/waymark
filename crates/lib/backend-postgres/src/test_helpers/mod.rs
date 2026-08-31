pub(super) mod deadlock;

use super::PostgresBackend;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_support_test::postgres_setup;
use waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntimes;

pub(super) async fn setup_backend() -> PostgresBackend {
    let pool = postgres_setup().await;
    crate::reset::truncate_all(&pool)
        .await
        .expect("truncate postgres tables");
    PostgresBackend::new(pool)
}

/// Snapshot bytes written by [`register_test_vm`].
pub(super) const TEST_VM_SNAPSHOT: &[u8] = b"test-snapshot";

/// Store a compiled-executable row through the production
/// [`waymark_workflow_service_vm_executables_backend::UpsertExecutable`]
/// path and return its id, so schedule tests get a real row behind the
/// executable foreign key.
pub(super) async fn upsert_test_executable(
    backend: &PostgresBackend,
    workflow_name: &str,
) -> WorkflowVersionId {
    waymark_workflow_service_vm_executables_backend::UpsertExecutable::upsert_executable(
        backend,
        workflow_name,
        "test-version",
        b"test-bytecode",
    )
    .await
    .expect("upsert test executable")
}

/// Register a VM runtime (its snapshot and runnable-workload rows) through the
/// production [`RegisterVmRuntimes`] path and return its identifiers, so tests
/// share exactly the registration behavior they exercise.
pub(super) async fn register_test_vm(backend: &PostgresBackend) -> (InstanceId, WorkflowVersionId) {
    let vm_id = InstanceId::new_uuid_v4();
    register_test_vm_with_id(backend, vm_id).await
}

/// [`register_test_vm`] with a caller-chosen VM id, for tests whose
/// choreography depends on the registration (heap) order of specific ids.
pub(super) async fn register_test_vm_with_id(
    backend: &PostgresBackend,
    vm_id: InstanceId,
) -> (InstanceId, WorkflowVersionId) {
    let executable_id = WorkflowVersionId::new_uuid_v4();
    let item = waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegisterVmRuntimesItem {
        vm_id: &vm_id,
        executable_id: &executable_id,
        snapshot: TEST_VM_SNAPSHOT,
    };
    backend
        .register_vm_runtimes(nonempty_collections::nev![item].as_nonempty_slice())
        .await
        .expect("register test vm");
    (vm_id, executable_id)
}
