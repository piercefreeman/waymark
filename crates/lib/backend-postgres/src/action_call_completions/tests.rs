use nonempty_collections::NEVec;
use serial_test::serial;
use waymark_action_completions_reconciler_backend::record_completions;
use waymark_action_completions_reconciler_backend::record_completions::Error as _;
use waymark_action_completions_reconciler_backend::{
    AckCompletions as _, CompletionKey, CompletionRecord, PollCompletions as _,
    RecordCompletions as _,
};
use waymark_ids::InstanceId;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::super::test_helpers::{deadlock, register_test_vm, setup_backend};
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
async fn snapshot_delete_sweeps_all_rows_of_the_vm_only() {
    let backend = setup_backend().await;
    let (vm_a, _) = register_test_vm(&backend).await;
    let (vm_b, _) = register_test_vm(&backend).await;

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

    sqlx::query("DELETE FROM vm_runtime_snapshots WHERE vm_id = $1")
        .bind(vm_a)
        .execute(backend.pool())
        .await
        .expect("delete vm_a snapshot");

    let demand = NEVec::try_from_vec(vec![key(vm_a, 1), key(vm_a, 2), key(vm_b, 1)]).unwrap();
    let polled = backend
        .poll_completions(demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [record(vm_b, 1, 20, b"b1")]);
}

/// One choreographed collision of the completions-ack DELETE and the
/// snapshot-cleanup trigger's completions sweep over the same two
/// completion rows — the completions-table sweep pairing of the
/// multi-row-writer class behind the 2026-08-31 deadlock outage.
///
/// The staging, plan-shape seeding, and contract are those of
/// `complete_and_renew_across_a_staged_row` in
/// `action_call_requests::tests`; the sweep's lock order is not
/// input-driven — it walks the whole VM — so the mirroring axis is the
/// ack batch's order plus which row is staged.
async fn ack_and_sweep_across_a_staged_row(ack_order: [usize; 2]) {
    let backend = setup_backend().await;
    let (vm, _) = register_test_vm(&backend).await;

    let records = NEVec::try_from_vec(vec![
        record(vm, 1, 10, b"done-1"),
        record(vm, 2, 11, b"done-2"),
    ])
    .unwrap();
    backend
        .record_completions(records.as_nonempty_slice())
        .await
        .expect("record completions");

    deadlock::seed_filler_rows(backend.pool(), deadlock::SweptTable::ActionCallCompletions).await;

    // Pre-hold the ack batch's first row.
    let staged = ack_order[0];
    let staging = deadlock::hold_row_for_update(
        backend.pool(),
        deadlock::SweptTable::ActionCallCompletions,
        vm,
        staged,
    )
    .await;

    // The ack first: it must be the head of the staged row's wait queue,
    // so releasing hands the row to it and not to the sweep queued later.
    let ack_backend = backend.clone();
    let ack_task = tokio::spawn(async move {
        let keys = NEVec::try_from_vec(ack_order.iter().map(|&promise| key(vm, promise)).collect())
            .unwrap();
        ack_backend.ack_completions(keys.as_nonempty_slice()).await
    });
    deadlock::contend_op_with_snapshot_sweep(
        backend.pool(),
        staging,
        "completions ack",
        ack_task,
        "%DELETE FROM action_call_completions%",
        vm,
    )
    .await;
}

#[serial(postgres)]
#[tokio::test]
async fn concurrent_ack_and_snapshot_sweep_do_not_deadlock() {
    // Both ack orders, with the staged row tracking the ack's first key.
    // These runs pin the ACK side's discipline only: a single-VM sweep
    // locks its rows in primary-key order under every realizable plan,
    // so the trigger side cannot regress visibly here — the multi-VM
    // sweep test in `action_call_requests::tests` covers it.
    ack_and_sweep_across_a_staged_row([2, 1]).await;
    ack_and_sweep_across_a_staged_row([1, 2]).await;
}
