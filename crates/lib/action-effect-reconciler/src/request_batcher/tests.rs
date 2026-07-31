//! Flush-behavior tests for the shared request batcher: coalescing,
//! per-record outcome mapping, and divergence fanning.  Retry behavior is
//! covered through the effect-handler tests.

use std::sync::Arc;

use waymark_action_effect_reconciler_backend::ActionCallRequestRecord;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::request_batcher::{RecordError, RecordOutcome, RequestRecorderHandle, request_batcher};
use crate::test_support::{MockBackend, TestKey};

fn record(vm_id: u64, promise: usize, request: &[u8]) -> ActionCallRequestRecord<u64> {
    ActionCallRequestRecord {
        vm_id,
        promise_state_id: PromiseStateId(promise),
        effect_number: EffectNumber(1),
        request: request.to_vec(),
    }
}

/// A recorder whose batcher only ever flushes on the size trigger, so a
/// test that completes proves its submissions shared one batch.
fn spawn_size_gated_recorder(
    backend: &Arc<MockBackend>,
    max_batch: usize,
) -> RequestRecorderHandle<u64> {
    let (recorder, batcher) = request_batcher(
        Arc::clone(backend),
        7u32,
        std::time::Duration::from_secs(60).try_into().unwrap(),
        waymark_batcher::Policy {
            max_batch: max_batch.try_into().unwrap(),
            max_delay: NonZeroDuration::from_secs(3600).unwrap(),
        },
        std::future::pending(),
    );
    tokio::spawn(batcher);
    recorder
}

#[tokio::test]
async fn coalesces_submissions_into_one_statement() {
    let backend = Arc::new(MockBackend::default());
    let recorder = spawn_size_gated_recorder(&backend, 2);

    // The delay trigger is effectively off, so these can only resolve if
    // both records left in the same batch.
    let a = {
        let recorder = recorder.clone();
        tokio::spawn(async move { recorder.submit(record(1, 10, b"a")).await })
    };
    let b = {
        let recorder = recorder.clone();
        tokio::spawn(async move { recorder.submit(record(2, 20, b"b")).await })
    };

    let mut fence_bases = Vec::new();
    for submission in [a, b] {
        let outcome = submission.await.expect("join").expect("not closed");
        match outcome.expect("recorded") {
            RecordOutcome::Recorded { taken_at } => fence_bases.push(taken_at),
            outcome @ RecordOutcome::AlreadyRecorded => {
                panic!("expected a fresh record: {outcome:?}")
            }
        }
    }
    // One pre-send instant per flush attempt: same-batch locks share
    // their fence base.
    assert_eq!(fence_bases[0], fence_bases[1]);

    let rows = backend.rows.lock().unwrap();
    for (vm_id, promise) in [(1, 10), (2, 20)] {
        let row = rows
            .get(&TestKey {
                vm_id,
                promise_state_id: PromiseStateId(promise),
            })
            .expect("row recorded");
        assert_eq!(row.locked_by, Some(7), "born locked by the batch lock");
    }
}

#[tokio::test]
async fn replays_and_fresh_records_get_their_own_outcomes() {
    let backend = Arc::new(MockBackend::default());

    // Seed a row so a byte-identical resubmission is a replay.
    let seeder = spawn_size_gated_recorder(&backend, 1);
    seeder
        .submit(record(1, 10, b"a"))
        .await
        .expect("not closed")
        .expect("recorded");

    // One batch holding a replay and a fresh record: each waiter gets its
    // own verdict.
    let recorder = spawn_size_gated_recorder(&backend, 2);
    let replay = {
        let recorder = recorder.clone();
        tokio::spawn(async move { recorder.submit(record(1, 10, b"a")).await })
    };
    let fresh = {
        let recorder = recorder.clone();
        tokio::spawn(async move { recorder.submit(record(2, 20, b"b")).await })
    };

    let replay = replay.await.expect("join").expect("not closed");
    let fresh = fresh.await.expect("join").expect("not closed");
    assert_eq!(
        replay.expect("replay outcome"),
        RecordOutcome::AlreadyRecorded
    );
    assert!(matches!(
        fresh.expect("fresh outcome"),
        RecordOutcome::Recorded { .. }
    ));
}

#[tokio::test]
async fn divergence_fails_the_whole_batch() {
    let backend = Arc::new(MockBackend::default());

    let seeder = spawn_size_gated_recorder(&backend, 1);
    seeder
        .submit(record(1, 10, b"a"))
        .await
        .expect("not closed")
        .expect("recorded");

    // A divergent resubmission shares a batch with an innocent fresh
    // record: the fatal error fans to both waiters...
    let recorder = spawn_size_gated_recorder(&backend, 2);
    let divergent = {
        let recorder = recorder.clone();
        tokio::spawn(async move { recorder.submit(record(1, 10, b"DIFFERENT")).await })
    };
    let innocent = {
        let recorder = recorder.clone();
        tokio::spawn(async move { recorder.submit(record(2, 20, b"b")).await })
    };

    for submission in [divergent, innocent] {
        let outcome = submission.await.expect("join").expect("not closed");
        assert!(matches!(outcome, Err(RecordError::DivergentPayload)));
    }

    // ...but the innocent record was still durably recorded (the backend
    // inserts everything not named in the divergence), so the revival
    // reconcile redelivers it once the batch lock lapses.
    assert!(backend.rows.lock().unwrap().contains_key(&TestKey {
        vm_id: 2,
        promise_state_id: PromiseStateId(20),
    }));
}
