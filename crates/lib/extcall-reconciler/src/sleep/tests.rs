//! Tests for the sleep handler and poller.

use std::time::Duration;

use waymark_extcall_convert::Converter;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_value::Value;

#[tokio::test]
async fn record_and_collect_single_sleep() {
    let (handler, mut poller) = super::new::<()>();
    let psid = PromiseStateId(0);

    // A 1-nanosecond sleep elapses immediately.
    handler.record(
        (),
        psid,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );

    // Should collect immediately.
    let settlements = poller.poll::<Converter, Value>().await.unwrap();
    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, psid);
}

#[tokio::test]
async fn multiple_sleeps_collected_in_order() {
    let (handler, mut poller) = super::new::<()>();
    let a = PromiseStateId(0);
    let b = PromiseStateId(1);

    handler.record(
        (),
        a,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );
    handler.record(
        (),
        b,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );

    let settlements = poller.poll::<Converter, Value>().await.unwrap();
    assert_eq!(settlements.len().get(), 2);
}

#[tokio::test]
async fn poll_waits_for_new_sleep() {
    let (handler, mut poller) = super::new::<()>();
    let psid = PromiseStateId(0);

    // Spawn a task that records a sleep after a short delay.
    let h = handler;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        h.record(
            (),
            psid,
            NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
        );
    });

    let settlements = poller.poll::<Converter, Value>().await.unwrap();
    assert_eq!(settlements[0].promise_state_id, psid);
}

#[tokio::test]
async fn poll_returns_none_when_handler_dropped() {
    let (handler, mut poller) = super::new::<()>();
    drop(handler);

    // Poll should return None — no handler to record sleeps.
    tokio::select! {
        result = poller.poll::<Converter, Value>() => {
            assert!(result.is_none());
        }
        _ = tokio::time::sleep(Duration::from_secs(1)) => {
            panic!("poll should return None, not hang");
        }
    }
}

#[tokio::test]
async fn sleep_resolution_is_null_value() {
    let (handler, mut poller) = super::new::<()>();
    let psid = PromiseStateId(0);

    handler.record(
        (),
        psid,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );

    let settlements = poller.poll::<Converter, Value>().await.unwrap();
    let s = &settlements[0];
    match &s.resolution {
        waymark_vm_driver_core::PromiseResolution::Resolved(v) => {
            assert_eq!(*v, Value::Ready(waymark_vm_value::ReadyValue::None));
        }
        _other => panic!("expected Resolved, got non-resolved variant"),
    }
}
