//! Tests for the sleep handler and poller.

use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_value::ReadyValue;

#[tokio::test]
async fn record_and_collect_single_sleep() {
    let (handler, mut poller) = super::new(false);
    let psid = PromiseStateId(0);

    // A 1-nanosecond sleep elapses immediately.
    handler.record(
        psid,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );

    // Should collect immediately.
    let settlements = poller.poll::<ReadyValue, crate::Ack>().await.unwrap();
    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, psid);
}

#[tokio::test]
async fn multiple_sleeps_collected_in_order() {
    let (handler, mut poller) = super::new(false);
    let a = PromiseStateId(0);
    let b = PromiseStateId(1);

    handler.record(
        a,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );
    handler.record(
        b,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );

    let settlements = poller.poll::<ReadyValue, crate::Ack>().await.unwrap();
    assert_eq!(settlements.len().get(), 2);
}

#[tokio::test]
async fn poll_waits_for_new_sleep() {
    let (handler, mut poller) = super::new(false);
    let psid = PromiseStateId(0);

    // Spawn a task that records a sleep after a short delay.
    let h = handler;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        h.record(
            psid,
            NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
        );
    });

    let settlements = poller.poll::<ReadyValue, crate::Ack>().await.unwrap();
    assert_eq!(settlements[0].promise_state_id, psid);
}

#[tokio::test]
async fn poll_returns_none_when_handler_dropped() {
    let (handler, mut poller) = super::new(false);
    drop(handler);

    // Poll should return None — no handler to record sleeps.
    tokio::select! {
        result = poller.poll::<ReadyValue, crate::Ack>() => {
            assert!(result.is_none());
        }
        _ = tokio::time::sleep(Duration::from_secs(1)) => {
            panic!("poll should return None, not hang");
        }
    }
}

#[tokio::test]
async fn sleep_resolution_is_null_value() {
    let (handler, mut poller) = super::new(false);
    let psid = PromiseStateId(0);

    handler.record(
        psid,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );

    let settlements = poller.poll::<ReadyValue, crate::Ack>().await.unwrap();
    let s = &settlements[0];
    match &s.resolution {
        waymark_vm_driver_core::PromiseResolution::Resolved(v) => {
            assert_eq!(*v, ReadyValue::None);
        }
        _other => panic!("expected Resolved, got non-resolved variant"),
    }
}

#[tokio::test]
async fn skip_sleep_resolves_immediately() {
    let (handler, mut poller) = super::new(true);
    let psid = PromiseStateId(0);

    // A 5-second sleep should resolve immediately when skip_sleep is true.
    handler.record(
        psid,
        NonZeroDuration::try_from(Duration::from_secs(5)).unwrap(),
    );

    let settlements = poller.poll::<ReadyValue, crate::Ack>().await.unwrap();
    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, psid);
}

#[tokio::test]
async fn skip_sleep_multiple_resolve_immediately() {
    let (handler, mut poller) = super::new(true);
    let a = PromiseStateId(0);
    let b = PromiseStateId(1);

    handler.record(
        a,
        NonZeroDuration::try_from(Duration::from_secs(60)).unwrap(),
    );
    handler.record(
        b,
        NonZeroDuration::try_from(Duration::from_secs(120)).unwrap(),
    );

    let settlements = poller.poll::<ReadyValue, crate::Ack>().await.unwrap();
    assert_eq!(settlements.len().get(), 2);
}

#[tokio::test]
async fn skip_sleep_flag_is_independent_per_handler() {
    // One handler with skip, one without.
    let (skip_handler, mut skip_poller) = super::new(true);
    let (normal_handler, mut normal_poller) = super::new(false);

    let skip_psid = PromiseStateId(0);
    skip_handler.record(
        skip_psid,
        NonZeroDuration::try_from(Duration::from_secs(5)).unwrap(),
    );
    let skip_settlements = skip_poller.poll::<ReadyValue, crate::Ack>().await.unwrap();
    assert_eq!(skip_settlements[0].promise_state_id, skip_psid);

    // The normal handler should still work with actual durations.
    let normal_psid = PromiseStateId(1);
    normal_handler.record(
        normal_psid,
        NonZeroDuration::try_from(Duration::from_nanos(1)).unwrap(),
    );
    let normal_settlements = normal_poller
        .poll::<ReadyValue, crate::Ack>()
        .await
        .unwrap();
    assert_eq!(normal_settlements[0].promise_state_id, normal_psid);
}
