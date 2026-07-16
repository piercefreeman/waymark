use std::sync::Arc;

use waymark_extcall_reconciler_core::ActionEffectHandler as _;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::EffectHandler;
use crate::effect_handler::Error;
use crate::renewal::HeldLock;
use crate::test_support::{
    CapturingRequester, FailingRequester, MockBackend, MockRecordError, TestKey, action_ref,
};

const LOCK_TIME_TO_LIVE: std::time::Duration = std::time::Duration::from_secs(60);

type TestEffectHandler = EffectHandler<MockBackend, RmpCodec, CapturingRequester>;

struct Harness {
    backend: Arc<MockBackend>,
    requester: CapturingRequester,
    handler: TestEffectHandler,
    held_locks_rx: tokio::sync::mpsc::UnboundedReceiver<HeldLock<u64>>,
}

fn harness() -> Harness {
    let backend = Arc::new(MockBackend::default());
    let requester = CapturingRequester::default();
    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    let handler = EffectHandler {
        backend: Arc::clone(&backend),
        codec: RmpCodec,
        lock_owner_id: 7u32,
        lock_time_to_live: LOCK_TIME_TO_LIVE.try_into().unwrap(),
        held_locks_tx,
        vm_id: 42u64,
        requester: requester.clone(),
    };
    Harness {
        backend,
        requester,
        handler,
        held_locks_rx,
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

    // The row exists, born locked by this process.
    let rows = h.backend.rows.lock().unwrap();
    let row = rows
        .get(&TestKey {
            vm_id: 42,
            promise_state_id: PromiseStateId(11),
        })
        .expect("row recorded");
    assert_eq!(row.effect_number, EffectNumber(3));
    assert_eq!(row.locked_by, Some(7));
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

    // The delivered call's lock is tracked for renewal.
    let held_lock = h.held_locks_rx.try_recv().expect("held lock");
    assert_eq!(held_lock.key.vm_id, 42);
    assert_eq!(held_lock.key.promise_state_id, PromiseStateId(11));
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

    assert_eq!(h.requester.requests.lock().unwrap().len(), 1);
    h.held_locks_rx.try_recv().expect("one held lock");
    assert!(h.held_locks_rx.try_recv().is_err());
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
        Error::Record(MockRecordError::DivergentPayload(_))
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
    h.held_locks_rx.try_recv().expect("held lock");
}

#[tokio::test]
async fn delivery_failure_leaves_the_row_recorded_and_untracked() {
    let backend = Arc::new(MockBackend::default());
    let (held_locks_tx, mut held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut handler: EffectHandler<MockBackend, RmpCodec, FailingRequester> = EffectHandler {
        backend: Arc::clone(&backend),
        codec: RmpCodec,
        lock_owner_id: 7u32,
        lock_time_to_live: LOCK_TIME_TO_LIVE.try_into().unwrap(),
        held_locks_tx,
        vm_id: 42u64,
        requester: FailingRequester,
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
    // born locked, and will be redelivered once the lock lapses.  Nothing
    // is tracked for renewal.
    assert!(backend.rows.lock().unwrap().contains_key(&TestKey {
        vm_id: 42,
        promise_state_id: PromiseStateId(11),
    }));
    assert!(held_locks_rx.try_recv().is_err());
}
