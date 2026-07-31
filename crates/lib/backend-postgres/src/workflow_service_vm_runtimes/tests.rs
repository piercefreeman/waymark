use nonempty_collections::{NESlice, nev};
use serial_test::serial;

use super::super::test_helpers::setup_backend;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::{
    RegisterVmRuntimesItem, RegistrationSuccess,
};
use waymark_workflow_service_vm_runtimes_backend::{FindExistingVmRuntimes, RegisterVmRuntimes};

async fn register_one(
    backend: &crate::PostgresBackend,
    vm_id: &InstanceId,
    executable_id: &WorkflowVersionId,
    snapshot: &[u8],
) -> Result<RegistrationSuccess<InstanceId>, super::error::RegisterVmRuntimesError> {
    let item = RegisterVmRuntimesItem {
        vm_id,
        executable_id,
        snapshot,
    };
    backend
        .register_vm_runtimes(nev![item].as_nonempty_slice())
        .await
}

#[serial(postgres)]
#[tokio::test]
async fn register_duplicate_vm_runtime_reports_already_registered() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();
    let executable_id = WorkflowVersionId::new_uuid_v4();

    let success = register_one(&backend, &vm_id, &executable_id, b"first")
        .await
        .expect("first register");
    assert_eq!(success, RegistrationSuccess::AllRegistered);

    let success = register_one(&backend, &vm_id, &executable_id, b"second")
        .await
        .expect("registration succeeds; already-registered is per-row");
    assert_eq!(
        success,
        RegistrationSuccess::SomeAlreadyRegistered(nev![vm_id]),
    );
}

#[serial(postgres)]
#[tokio::test]
async fn mixed_batch_registers_only_fresh_vm_runtimes() {
    let backend = setup_backend().await;
    let executable_id = WorkflowVersionId::new_uuid_v4();
    let seeded = InstanceId::new_uuid_v4();
    let fresh = InstanceId::new_uuid_v4();

    register_one(&backend, &seeded, &executable_id, b"seeded")
        .await
        .expect("seed");

    // One batch spanning an already-registered id and a fresh one: the
    // conflict is named per-row while the fresh VM runtime is durably
    // registered by the same statement.
    let items = [
        RegisterVmRuntimesItem {
            vm_id: &seeded,
            executable_id: &executable_id,
            snapshot: b"rewrite-attempt",
        },
        RegisterVmRuntimesItem {
            vm_id: &fresh,
            executable_id: &executable_id,
            snapshot: b"fresh",
        },
    ];
    let success = backend
        .register_vm_runtimes(NESlice::try_from_slice(&items).expect("non-empty"))
        .await
        .expect("registration succeeds; already-registered is per-row");
    assert_eq!(
        success,
        RegistrationSuccess::SomeAlreadyRegistered(nev![seeded]),
    );

    let existing = backend
        .find_existing_vm_runtimes(NESlice::try_from_slice(&[seeded, fresh]).expect("non-empty"))
        .await
        .expect("find existing");
    assert_eq!(existing.len(), 2, "the fresh VM runtime was registered");
}

#[serial(postgres)]
#[tokio::test]
async fn find_existing_vm_runtimes_returns_only_registered() {
    let backend = setup_backend().await;
    let executable_id = WorkflowVersionId::new_uuid_v4();

    let registered = InstanceId::new_uuid_v4();
    let unregistered = InstanceId::new_uuid_v4();

    register_one(&backend, &registered, &executable_id, b"snapshot")
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
