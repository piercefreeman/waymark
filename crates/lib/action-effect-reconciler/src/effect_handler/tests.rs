use std::sync::Arc;

use waymark_extcall_reconciler_core::ActionEffectHandler as _;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::EffectHandler;
use crate::action_call_request_locks::Tracker;
use crate::adopting_action_settler::recheck;
use crate::effect_handler::Error;
use crate::request_batcher::RecordError;
use crate::test_support::{
    CapturingRequester, FailingRequester, LOCK_OWNER, MockBackend, TestKey, action_ref,
    shared_tracker, spawn_recorder, tracks_request,
};

type TestEffectHandler<Requester> = EffectHandler<u64, RmpCodec, Requester>;

struct Harness {
    backend: Arc<MockBackend>,
    tracker: Arc<std::sync::Mutex<Tracker<u64>>>,
    requester: CapturingRequester,
    handler: TestEffectHandler<CapturingRequester>,
    recheck: recheck::Rx,
}

fn harness() -> Harness {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    let requester = CapturingRequester::default();
    let (recheck_tx, recheck) = recheck::new();
    let handler = EffectHandler {
        recorder: spawn_recorder(&backend, &tracker),
        codec: RmpCodec,
        action_call_request_locks_tracker: Arc::clone(&tracker),
        vm_id: 42u64,
        requester: requester.clone(),
        recheck: recheck_tx,
    };
    Harness {
        backend,
        tracker,
        requester,
        handler,
        recheck,
    }
}

fn key(promise: usize) -> TestKey {
    TestKey {
        vm_id: 42,
        promise_state_id: PromiseStateId(promise),
    }
}

#[tokio::test]
async fn records_born_locked_then_delivers() {
    let mut h = harness();

    h.handler
        .request_action(
            EffectNumber(3),
            PromiseStateId(11),
            action_ref("greet"),
            vec![1i64, 2i64],
        )
        .await
        .expect("fresh effect");

    // A fresh recording gives the adopter nothing to recheck.
    assert!(!recheck_requested_now(&mut h.recheck).await);

    // The row exists, born locked by this process.
    let rows = h.backend.rows.lock().unwrap();
    let row = rows.get(&key(11)).expect("row recorded");
    assert_eq!(row.effect_number, EffectNumber(3));
    assert_eq!(row.locked_by, Some(LOCK_OWNER));
    assert!(row.lock_expires_at.expect("locked rows carry expiry") > chrono::Utc::now());
    drop(rows);

    // The call was delivered with its correlation.
    let requests = h.requester.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].action_ref, action_ref("greet"));
    assert_eq!(requests[0].arguments, vec![1, 2]);
    assert_eq!(requests[0].metadata.effect_number, EffectNumber(3));
    assert_eq!(requests[0].metadata.promise_state_id, PromiseStateId(11));
    drop(requests);

    // The requested call's lock is tracked for renewal.
    let tracker = h.tracker.lock().unwrap();
    assert!(tracks_request(&tracker, &key(11)));
    assert!(tracker.pending_unlocks().is_none());
}

#[tokio::test]
async fn replayed_effect_is_not_delivered_again() {
    let mut h = harness();

    for _ in 0..2 {
        h.handler
            .request_action(
                EffectNumber(3),
                PromiseStateId(11),
                action_ref("greet"),
                vec![1i64],
            )
            .await
            .expect("effect");
    }

    // The row found already recorded is left to the adopter to recheck.
    assert!(recheck_requested_now(&mut h.recheck).await);
    assert!(!recheck_requested_now(&mut h.recheck).await);

    assert_eq!(h.requester.requests.lock().unwrap().len(), 1);
    let tracker = h.tracker.lock().unwrap();
    assert_eq!(
        tracker
            .tracked_lock_ids()
            .iter()
            .map(|lock_id| lock_id.request)
            .collect::<Vec<_>>(),
        vec![key(11)]
    );
    assert!(tracker.pending_unlocks().is_none());
}

#[tokio::test]
async fn divergent_payload_is_fatal() {
    let mut h = harness();

    h.handler
        .request_action(
            EffectNumber(3),
            PromiseStateId(11),
            action_ref("greet"),
            vec![1i64],
        )
        .await
        .expect("fresh effect");

    let error = h
        .handler
        .request_action(
            EffectNumber(3),
            PromiseStateId(11),
            action_ref("greet"),
            vec![999i64],
        )
        .await
        .expect_err("divergent payload must fail");
    assert!(matches!(
        error,
        Error::Record(RecordError::DivergentPayload)
    ));
}

#[tokio::test(start_paused = true)]
async fn retries_retryable_record_failures() {
    let mut h = harness();
    *h.backend.fail_records.lock().unwrap() = 2;

    h.handler
        .request_action(
            EffectNumber(3),
            PromiseStateId(11),
            action_ref("greet"),
            vec![1i64],
        )
        .await
        .expect("retried to success");

    assert_eq!(h.requester.requests.lock().unwrap().len(), 1);
    let tracker = h.tracker.lock().unwrap();
    assert!(tracks_request(&tracker, &key(11)));
    assert!(tracker.pending_unlocks().is_none());
}

/// The statement committed but reported failure: the retry finds the row
/// already recorded, and the first attempt's lock is held here, tracked
/// by nobody — the adopter's recheck is the way to the call.
#[tokio::test(start_paused = true)]
async fn a_recording_that_committed_but_failed_is_left_to_the_recheck() {
    let mut h = harness();
    *h.backend.fail_records_after_insert.lock().unwrap() = 1;

    h.handler
        .request_action(
            EffectNumber(3),
            PromiseStateId(11),
            action_ref("greet"),
            vec![1i64],
        )
        .await
        .expect("the retry finds the row recorded");

    assert!(h.requester.requests.lock().unwrap().is_empty());
    assert!(recheck_requested_now(&mut h.recheck).await);

    let rows = h.backend.rows.lock().unwrap();
    assert_eq!(rows[&key(11)].locked_by, Some(LOCK_OWNER));
    drop(rows);

    let tracker = h.tracker.lock().unwrap();
    assert!(!tracks_request(&tracker, &key(11)));
    assert!(tracker.pending_unlocks().is_none());
}

/// Whether a recheck is requested right now: one poll of the request.
async fn recheck_requested_now(recheck: &mut recheck::Rx) -> bool {
    let requested =
        tokio::time::timeout(std::time::Duration::ZERO, recheck.recheck_requested()).await;
    matches!(requested, Ok(Ok(())))
}

#[tokio::test]
async fn delivery_failure_leaves_the_row_recorded_and_up_for_unlocking() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    let mut handler: TestEffectHandler<FailingRequester> = EffectHandler {
        recorder: spawn_recorder(&backend, &tracker),
        codec: RmpCodec,
        action_call_request_locks_tracker: Arc::clone(&tracker),
        vm_id: 42u64,
        requester: FailingRequester,
        recheck: recheck::new().0,
    };

    let error = handler
        .request_action(
            EffectNumber(3),
            PromiseStateId(11),
            action_ref("greet"),
            vec![1i64],
        )
        .await
        .expect_err("delivery failure must surface");
    assert!(matches!(error, Error::Deliver(_)));

    // The row was durably recorded before the failed delivery — it stays,
    // born locked.  Its lock is not tracked but up for unlocking: the
    // request never went through.
    assert!(backend.rows.lock().unwrap().contains_key(&key(11)));
    let tracker = tracker.lock().unwrap();
    assert!(!tracks_request(&tracker, &key(11)));
    let pending = tracker
        .pending_unlocks()
        .expect("the lock is up for unlocking");
    assert_eq!(
        pending
            .lock_ids()
            .iter()
            .map(|lock_id| lock_id.request)
            .collect::<Vec<_>>(),
        vec![key(11)]
    );
}
