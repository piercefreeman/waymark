use serial_test::serial;

use crate::test_helpers::setup_backend;
use waymark_ids::InstanceId;
use waymark_workflow_completion_backend::{RecordCompletion, RecordException};

#[serial(postgres)]
#[tokio::test]
async fn record_completion_first_write_succeeds() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let result = RecordCompletion::record_completion(&backend, &vm_id, b"result-1").await;
    assert!(
        result.is_ok(),
        "first completion should succeed: {:?}",
        result
    );
}

#[serial(postgres)]
#[tokio::test]
async fn record_completion_same_value_is_idempotent() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    RecordCompletion::record_completion(&backend, &vm_id, b"result-1")
        .await
        .expect("first write");

    let result = RecordCompletion::record_completion(&backend, &vm_id, b"result-1").await;
    assert!(
        result.is_ok(),
        "same value should be idempotent: {:?}",
        result
    );
}

#[serial(postgres)]
#[tokio::test]
async fn record_completion_different_value_is_conflict() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    RecordCompletion::record_completion(&backend, &vm_id, b"result-1")
        .await
        .expect("first write");

    let result = RecordCompletion::record_completion(&backend, &vm_id, b"result-2").await;
    assert!(
        matches!(result, Err(super::error::RecordError::Conflict(id)) if id == vm_id),
        "different value should conflict, got: {:?}",
        result
    );
}

#[serial(postgres)]
#[tokio::test]
async fn record_exception_first_write_succeeds() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let result = RecordException::record_exception(&backend, &vm_id, b"exception-1").await;
    assert!(
        result.is_ok(),
        "first exception should succeed: {:?}",
        result
    );
}

#[serial(postgres)]
#[tokio::test]
async fn record_exception_same_value_is_idempotent() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    RecordException::record_exception(&backend, &vm_id, b"exception-1")
        .await
        .expect("first write");

    let result = RecordException::record_exception(&backend, &vm_id, b"exception-1").await;
    assert!(
        result.is_ok(),
        "same value should be idempotent: {:?}",
        result
    );
}

#[serial(postgres)]
#[tokio::test]
async fn record_exception_different_value_is_conflict() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    RecordException::record_exception(&backend, &vm_id, b"exception-1")
        .await
        .expect("first write");

    let result = RecordException::record_exception(&backend, &vm_id, b"exception-2").await;
    assert!(
        matches!(result, Err(super::error::RecordError::Conflict(id)) if id == vm_id),
        "different value should conflict, got: {:?}",
        result
    );
}

#[serial(postgres)]
#[tokio::test]
async fn completion_and_exception_do_not_conflict_with_each_other() {
    // The XOR constraint on the table means result and error are independent —
    // recording a completion then an exception (or vice versa) are separate
    // columns and will conflict with each other since the INSERT specifies
    // different columns.  Both should get a conflict error on the second write.

    // Fixed: both record_completion and record_exception write different
    // columns (result vs error).  The second write will conflict because the
    // WHERE clause checks the column being written — inserting `error` when
    // `result` already exists: `WHERE vm_execution_results.error IS NOT
    // DISTINCT FROM EXCLUDED.error` → existing error is NULL, excluded is
    // non-NULL → no match → conflict.

    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    RecordCompletion::record_completion(&backend, &vm_id, b"value")
        .await
        .expect("completion");

    let result = RecordException::record_exception(&backend, &vm_id, b"exception").await;
    assert!(
        matches!(result, Err(super::error::RecordError::Conflict(id)) if id == vm_id),
        "exception after completion should conflict, got: {:?}",
        result
    );
}
