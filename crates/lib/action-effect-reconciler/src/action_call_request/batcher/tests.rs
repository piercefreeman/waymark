//! Flush-behavior tests for the shared request batcher: coalescing,
//! per-record outcomes, and the divergence failure fan.

use std::num::NonZeroUsize;
use std::sync::Arc;

use waymark_action_effect_reconciler_backend::ActionCallRequestRecord;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::{RecorderHandle, RecordingError, RecordingOutcome};
use crate::action_call_request::locks::Tracker;
use crate::test_support::{LOCK_OWNER, LOCK_TIME_TO_LIVE, MockBackend, TestKey, shared_tracker};

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
    tracker: &Arc<std::sync::Mutex<Tracker<u64>>>,
    max_batch: usize,
) -> RecorderHandle<u64> {
    let (recorder, batcher) = super::run(
        Arc::clone(backend),
        LOCK_OWNER,
        LOCK_TIME_TO_LIVE.try_into().unwrap(),
        Arc::clone(tracker),
        waymark_batcher::Policy {
            max_batch: NonZeroUsize::new(max_batch).expect("non-zero"),
            max_delay: NonZeroDuration::from_secs(3600).expect("non-zero"),
        },
        std::future::pending(),
    );
    tokio::spawn(batcher);
    recorder
}

#[tokio::test]
async fn coalesces_submissions_into_one_statement() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    let recorder = spawn_size_gated_recorder(&backend, &tracker, 2);

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

    for submission in [a, b] {
        let outcome = submission.await.expect("join").expect("not closed");
        match outcome.expect("recorded") {
            RecordingOutcome::Recorded {
                pending_call_request,
            } => {
                pending_call_request.confirm_requested();
            }
            outcome @ RecordingOutcome::AlreadyRecorded => {
                panic!("expected a fresh record: {outcome:?}")
            }
        }
    }
    // One pre-send instant and one nonce per flush attempt: same-batch
    // locks share their fence deadline and their nonce.
    {
        let tracker = tracker.lock().unwrap();
        let lock_ids = tracker.tracked_lock_ids();
        assert_eq!(lock_ids.len(), 2);
        assert_eq!(
            tracker.fence_deadline(&lock_ids[0]),
            tracker.fence_deadline(&lock_ids[1])
        );
        assert_eq!(lock_ids[0].nonce, lock_ids[1].nonce);
    }

    let rows = backend.rows.lock().unwrap();
    for (vm_id, promise) in [(1, 10), (2, 20)] {
        let row = rows
            .get(&TestKey {
                vm_id,
                promise_state_id: PromiseStateId(promise),
            })
            .expect("row recorded");
        assert_eq!(
            row.locked_by,
            Some(LOCK_OWNER),
            "born locked by the batch lock"
        );
    }
}

#[tokio::test]
async fn replays_and_fresh_records_get_their_own_outcomes() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();

    // Seed a row so a byte-identical resubmission is a replay.
    let seeder = spawn_size_gated_recorder(&backend, &tracker, 1);
    let seeded = seeder
        .submit(record(1, 10, b"a"))
        .await
        .expect("not closed")
        .expect("recorded");
    let RecordingOutcome::Recorded {
        pending_call_request,
    } = seeded
    else {
        panic!("expected a fresh record: {seeded:?}");
    };
    pending_call_request.confirm_requested();

    // One batch holding a replay and a fresh record: each waiter gets its
    // own verdict.
    let recorder = spawn_size_gated_recorder(&backend, &tracker, 2);
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
    assert!(matches!(
        replay.expect("replay outcome"),
        RecordingOutcome::AlreadyRecorded
    ));
    assert!(matches!(
        fresh.expect("fresh outcome"),
        RecordingOutcome::Recorded { .. }
    ));
}

#[tokio::test]
async fn divergence_fails_the_whole_batch() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();

    let seeder = spawn_size_gated_recorder(&backend, &tracker, 1);
    seeder
        .submit(record(1, 10, b"a"))
        .await
        .expect("not closed")
        .expect("recorded");

    // A divergent resubmission shares a batch with an innocent fresh
    // record: the fatal error fans to both waiters...
    let recorder = spawn_size_gated_recorder(&backend, &tracker, 2);
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
        assert!(matches!(outcome, Err(RecordingError::DivergentPayload)));
    }

    // ...but the innocent record was still durably recorded (the backend
    // inserts everything the divergence error does not refer to), so
    // adoption redelivers it once the batch lock lapses.
    assert!(backend.rows.lock().unwrap().contains_key(&TestKey {
        vm_id: 2,
        promise_state_id: PromiseStateId(20),
    }));
}
