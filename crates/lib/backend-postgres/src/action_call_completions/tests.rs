use nonempty_collections::NEVec;
use serial_test::serial;
use waymark_action_completions_reconciler_backend::record_completions;
use waymark_action_completions_reconciler_backend::record_completions::Error as _;
use waymark_action_completions_reconciler_backend::{
    AckCompletions as _, CompletionKey, CompletionRecord, PollCompletions as _,
    PurgeVmCompletions as _, RecordCompletions as _,
};
use waymark_ids::InstanceId;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::super::test_helpers::setup_backend;
use super::error::RecordError;

fn record(
    vm_id: InstanceId,
    promise: usize,
    effect: usize,
    outcome: &[u8],
) -> CompletionRecord<InstanceId> {
    CompletionRecord {
        vm_id,
        promise_state_id: PromiseStateId(promise),
        effect_number: EffectNumber(effect),
        outcome: outcome.to_vec(),
    }
}

fn key(vm_id: InstanceId, promise: usize) -> CompletionKey<InstanceId> {
    CompletionKey {
        vm_id,
        promise_state_id: PromiseStateId(promise),
    }
}

#[serial(postgres)]
#[tokio::test]
async fn record_then_poll_returns_only_demanded() {
    let backend = setup_backend().await;
    let vm_a = InstanceId::new_uuid_v4();
    let vm_b = InstanceId::new_uuid_v4();

    let records = NEVec::try_from_vec(vec![
        record(vm_a, 1, 10, b"a1"),
        record(vm_a, 2, 11, b"a2"),
        record(vm_b, 1, 20, b"b1"),
    ])
    .unwrap();
    backend
        .record_completions(records.as_nonempty_slice())
        .await
        .expect("record");

    // Demand only one of vm_a's promises plus one that was never recorded.
    let demand = NEVec::try_from_vec(vec![key(vm_a, 1), key(vm_a, 99)]).unwrap();
    let polled = backend
        .poll_completions(demand.as_nonempty_slice())
        .await
        .expect("poll");

    assert_eq!(polled.len(), 1);
    assert_eq!(polled[0], record(vm_a, 1, 10, b"a1"));
}

#[serial(postgres)]
#[tokio::test]
async fn record_is_idempotent_for_identical_duplicates() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let records = NEVec::new(record(vm_id, 1, 10, b"same"));
    let success = backend
        .record_completions(records.as_nonempty_slice())
        .await
        .expect("first record");
    assert_eq!(success, record_completions::RecordingSuccess::AllRecorded);
    let success = backend
        .record_completions(records.as_nonempty_slice())
        .await
        .expect("identical re-record is idempotent");
    assert_eq!(success, record_completions::RecordingSuccess::AllRecorded);

    let demand = NEVec::new(key(vm_id, 1));
    let polled = backend
        .poll_completions(demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled.len(), 1);
}

#[serial(postgres)]
#[tokio::test]
async fn record_reports_conflicting_outcome_and_keeps_first_write() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let original = NEVec::new(record(vm_id, 1, 10, b"original"));
    backend
        .record_completions(original.as_nonempty_slice())
        .await
        .expect("first record");

    // Same key and effect number, conflicting outcome: a redelivered
    // non-deterministic retry.  First write wins, reported as success.
    let conflicting_outcome = NEVec::new(record(vm_id, 1, 10, b"DIFFERENT"));
    let success = backend
        .record_completions(conflicting_outcome.as_nonempty_slice())
        .await
        .expect("conflicting outcome is not an error");
    assert_eq!(
        success,
        record_completions::RecordingSuccess::SomeConflictingOutcomes(NEVec::new(key(vm_id, 1)))
    );

    // The stored row is unchanged.
    let polled = backend
        .poll_completions(NEVec::new(key(vm_id, 1)).as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [record(vm_id, 1, 10, b"original")]);
}

#[serial(postgres)]
#[tokio::test]
async fn record_fails_loudly_on_divergent_effect_number() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let original = NEVec::new(record(vm_id, 1, 10, b"original"));
    backend
        .record_completions(original.as_nonempty_slice())
        .await
        .expect("first record");

    // Same key, different effect number: broken correlation invariant.
    let divergent_effect = NEVec::new(record(vm_id, 1, 99, b"original"));
    let error = backend
        .record_completions(divergent_effect.as_nonempty_slice())
        .await
        .expect_err("divergent effect number must fail");
    assert_eq!(
        error.kind(),
        record_completions::ErrorKind::DivergentEffectNumber
    );
    let RecordError::DivergentEffectNumber(keys) = &error else {
        panic!("expected DivergentEffectNumber, got {error:?}");
    };
    assert_eq!(keys, &NEVec::new(key(vm_id, 1)));

    // The stored row is unchanged.
    let polled = backend
        .poll_completions(NEVec::new(key(vm_id, 1)).as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [record(vm_id, 1, 10, b"original")]);
}

#[serial(postgres)]
#[tokio::test]
async fn record_accepts_mixed_batch_of_new_and_identical_rows() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    backend
        .record_completions(NEVec::new(record(vm_id, 1, 10, b"one")).as_nonempty_slice())
        .await
        .expect("seed record");

    // One identical duplicate + one new row in the same batch.
    let batch = NEVec::try_from_vec(vec![
        record(vm_id, 1, 10, b"one"),
        record(vm_id, 2, 11, b"two"),
    ])
    .unwrap();
    let success = backend
        .record_completions(batch.as_nonempty_slice())
        .await
        .expect("mixed batch is accepted");
    assert_eq!(success, record_completions::RecordingSuccess::AllRecorded);

    let demand = NEVec::try_from_vec(vec![key(vm_id, 1), key(vm_id, 2)]).unwrap();
    let polled = backend
        .poll_completions(demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled.len(), 2);
}

#[serial(postgres)]
#[tokio::test]
async fn ack_removes_rows_and_is_idempotent() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();

    let records = NEVec::try_from_vec(vec![
        record(vm_id, 1, 10, b"one"),
        record(vm_id, 2, 11, b"two"),
    ])
    .unwrap();
    backend
        .record_completions(records.as_nonempty_slice())
        .await
        .expect("record");

    let acked = NEVec::new(key(vm_id, 1));
    backend
        .ack_completions(acked.as_nonempty_slice())
        .await
        .expect("ack");
    // Re-acking (crash recovery) and acking a never-recorded key are no-ops.
    let re_acked = NEVec::try_from_vec(vec![key(vm_id, 1), key(vm_id, 99)]).unwrap();
    backend
        .ack_completions(re_acked.as_nonempty_slice())
        .await
        .expect("re-ack is idempotent");

    let demand = NEVec::try_from_vec(vec![key(vm_id, 1), key(vm_id, 2)]).unwrap();
    let polled = backend
        .poll_completions(demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [record(vm_id, 2, 11, b"two")]);
}

#[serial(postgres)]
#[tokio::test]
async fn purge_removes_all_rows_of_the_vm_only() {
    let backend = setup_backend().await;
    let vm_a = InstanceId::new_uuid_v4();
    let vm_b = InstanceId::new_uuid_v4();

    let records = NEVec::try_from_vec(vec![
        record(vm_a, 1, 10, b"a1"),
        record(vm_a, 2, 11, b"a2"),
        record(vm_b, 1, 20, b"b1"),
    ])
    .unwrap();
    backend
        .record_completions(records.as_nonempty_slice())
        .await
        .expect("record");

    backend.purge_vm_completions(&vm_a).await.expect("purge");
    // Purging a VM with no rows is a no-op.
    backend
        .purge_vm_completions(&InstanceId::new_uuid_v4())
        .await
        .expect("purge of unknown vm");

    let demand = NEVec::try_from_vec(vec![key(vm_a, 1), key(vm_a, 2), key(vm_b, 1)]).unwrap();
    let polled = backend
        .poll_completions(demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [record(vm_b, 1, 20, b"b1")]);
}
