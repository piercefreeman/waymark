use serial_test::serial;

use crate::test_helpers::setup_backend;
use waymark_ids::InstanceId;
use waymark_workflow_completion_backend::{
    Outcome, RecordOutcomes as _, RecordOutcomesItem, RecordingSuccess,
};

async fn record_batch(
    backend: &crate::PostgresBackend,
    items: &[RecordOutcomesItem<'_, InstanceId>],
) -> Result<RecordingSuccess<InstanceId>, super::error::RecordOutcomesError> {
    backend
        .record_outcomes(
            nonempty_collections::NESlice::try_from_slice(items)
                .expect("test batches are non-empty"),
        )
        .await
}

async fn record_one(
    backend: &crate::PostgresBackend,
    vm_id: &InstanceId,
    outcome: &Outcome,
) -> Result<RecordingSuccess<InstanceId>, super::error::RecordOutcomesError> {
    record_batch(backend, &[RecordOutcomesItem { vm_id, outcome }]).await
}

fn completion(value: &[u8]) -> Outcome {
    Outcome::Completion(value.to_vec())
}

fn exception(value: &[u8]) -> Outcome {
    Outcome::Exception(value.to_vec())
}

fn assert_conflicted(result: &RecordingSuccess<InstanceId>, expected: &[InstanceId]) {
    match result {
        RecordingSuccess::SomeConflicted(ids) => {
            assert_eq!(
                ids.clone().into_iter().collect::<Vec<_>>(),
                expected,
                "conflicted keys should be named exactly",
            );
        }
        RecordingSuccess::AllRecorded => {
            panic!("expected conflicts {expected:?}, got AllRecorded")
        }
    }
}

#[serial(postgres)]
#[tokio::test]
async fn record_completion_first_write_succeeds() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let result = record_one(&backend, &vm_id, &completion(b"result-1"))
        .await
        .expect("recording succeeds");
    assert_eq!(result, RecordingSuccess::AllRecorded);
}

#[serial(postgres)]
#[tokio::test]
async fn record_completion_same_value_is_idempotent() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    record_one(&backend, &vm_id, &completion(b"result-1"))
        .await
        .expect("first write");

    let result = record_one(&backend, &vm_id, &completion(b"result-1"))
        .await
        .expect("identical re-record succeeds");
    assert_eq!(result, RecordingSuccess::AllRecorded);
}

#[serial(postgres)]
#[tokio::test]
async fn record_completion_different_value_is_conflict() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    record_one(&backend, &vm_id, &completion(b"result-1"))
        .await
        .expect("first write");

    let result = record_one(&backend, &vm_id, &completion(b"result-2"))
        .await
        .expect("recording succeeds; the conflict is per-row");
    assert_conflicted(&result, &[vm_id]);
}

#[serial(postgres)]
#[tokio::test]
async fn record_exception_first_write_succeeds() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let result = record_one(&backend, &vm_id, &exception(b"exception-1"))
        .await
        .expect("recording succeeds");
    assert_eq!(result, RecordingSuccess::AllRecorded);
}

#[serial(postgres)]
#[tokio::test]
async fn record_exception_same_value_is_idempotent() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    record_one(&backend, &vm_id, &exception(b"exception-1"))
        .await
        .expect("first write");

    let result = record_one(&backend, &vm_id, &exception(b"exception-1"))
        .await
        .expect("identical re-record succeeds");
    assert_eq!(result, RecordingSuccess::AllRecorded);
}

#[serial(postgres)]
#[tokio::test]
async fn record_exception_different_value_is_conflict() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    record_one(&backend, &vm_id, &exception(b"exception-1"))
        .await
        .expect("first write");

    let result = record_one(&backend, &vm_id, &exception(b"exception-2"))
        .await
        .expect("recording succeeds; the conflict is per-row");
    assert_conflicted(&result, &[vm_id]);
}

#[serial(postgres)]
#[tokio::test]
async fn completion_then_exception_conflicts() {
    // The outcome is exclusive: an exception for a VM that already has a
    // completion flips which column is NULL, so the upsert's two-column
    // `IS NOT DISTINCT FROM` check rejects the write as a per-row conflict.

    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    record_one(&backend, &vm_id, &completion(b"value"))
        .await
        .expect("completion");

    let result = record_one(&backend, &vm_id, &exception(b"exception"))
        .await
        .expect("recording succeeds; the conflict is per-row");
    assert_conflicted(&result, &[vm_id]);
}

#[serial(postgres)]
#[tokio::test]
async fn mixed_batch_records_both_variants() {
    let backend = setup_backend().await;
    let done_vm = InstanceId::new_uuid_v4();
    let failed_vm = InstanceId::new_uuid_v4();
    let done = completion(b"value");
    let failed = exception(b"boom");

    let result = record_batch(
        &backend,
        &[
            RecordOutcomesItem {
                vm_id: &done_vm,
                outcome: &done,
            },
            RecordOutcomesItem {
                vm_id: &failed_vm,
                outcome: &failed,
            },
        ],
    )
    .await
    .expect("mixed batch records in one statement");
    assert_eq!(result, RecordingSuccess::AllRecorded);

    use waymark_workflow_completion_backend::PollOutcome as _;
    let polled = backend.poll_outcome(&done_vm).await.expect("poll");
    assert_eq!(polled, Some(done));
    let polled = backend.poll_outcome(&failed_vm).await.expect("poll");
    assert_eq!(polled, Some(failed));
}

#[serial(postgres)]
#[tokio::test]
async fn batch_conflict_names_only_the_conflicted_keys() {
    let backend = setup_backend().await;
    let seeded_vm = InstanceId::new_uuid_v4();
    let fresh_vm = InstanceId::new_uuid_v4();

    record_one(&backend, &seeded_vm, &completion(b"original"))
        .await
        .expect("seed");

    // One batch holding a conflicting rewrite and an innocent fresh
    // outcome: the conflict is named per-row while the innocent outcome
    // is durably recorded by the same statement.
    let conflicting = completion(b"DIFFERENT");
    let fresh = completion(b"fresh");
    let result = record_batch(
        &backend,
        &[
            RecordOutcomesItem {
                vm_id: &seeded_vm,
                outcome: &conflicting,
            },
            RecordOutcomesItem {
                vm_id: &fresh_vm,
                outcome: &fresh,
            },
        ],
    )
    .await
    .expect("recording succeeds; the conflict is per-row");
    assert_conflicted(&result, &[seeded_vm]);

    use waymark_workflow_completion_backend::PollOutcome as _;
    let polled = backend.poll_outcome(&fresh_vm).await.expect("poll");
    assert_eq!(polled, Some(fresh), "the innocent outcome was recorded");
}

#[serial(postgres)]
#[tokio::test]
async fn duplicate_vm_ids_in_one_batch_error_as_invalid_batch() {
    use waymark_workflow_completion_backend::record_outcomes::{Error as _, ErrorKind};

    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    // The upsert cannot affect the same row twice (SQLSTATE 21000), even
    // for identical rows.  Callers dedupe; the classification makes the
    // failure deterministically fatal rather than endlessly retried.
    let outcome = completion(b"result");
    let error = record_batch(
        &backend,
        &[
            RecordOutcomesItem {
                vm_id: &vm_id,
                outcome: &outcome,
            },
            RecordOutcomesItem {
                vm_id: &vm_id,
                outcome: &outcome,
            },
        ],
    )
    .await
    .expect_err("a duplicate vm_id cannot be upserted in one statement");
    assert_eq!(error.kind(), ErrorKind::InvalidBatch);
}
