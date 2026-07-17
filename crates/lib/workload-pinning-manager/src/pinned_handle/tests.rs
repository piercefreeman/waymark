use waymark_workload_pinning_core::UnpinMode;

use super::PinnedHandle;

#[test]
fn unpin_park_sends_park_exactly_once() {
    let (evict_tx, mut evict_rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = PinnedHandle::new(1u64, evict_tx);

    handle.unpin(UnpinMode::Park);

    assert_eq!(
        evict_rx.try_recv().expect("eviction sent"),
        (1u64, UnpinMode::Park)
    );
    // The handle was consumed and its drop must not send again.
    assert!(evict_rx.try_recv().is_err());
}

#[test]
fn unpin_release_sends_release_exactly_once() {
    let (evict_tx, mut evict_rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = PinnedHandle::new(2u64, evict_tx);

    handle.unpin(UnpinMode::Release);

    assert_eq!(
        evict_rx.try_recv().expect("eviction sent"),
        (2u64, UnpinMode::Release)
    );
    assert!(evict_rx.try_recv().is_err());
}

#[test]
fn drop_sends_release() {
    let (evict_tx, mut evict_rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = PinnedHandle::new(3u64, evict_tx);

    drop(handle);

    assert_eq!(
        evict_rx.try_recv().expect("eviction sent"),
        (3u64, UnpinMode::Release)
    );
    assert!(evict_rx.try_recv().is_err());
}

#[test]
fn unpin_with_closed_receiver_does_not_panic() {
    let (evict_tx, evict_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let handle = PinnedHandle::new(4u64, evict_tx);

    drop(evict_rx);

    handle.unpin(UnpinMode::Park);
}

#[test]
fn id_returns_the_workload_id() {
    let (evict_tx, _evict_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let handle = PinnedHandle::new(5u64, evict_tx);

    assert_eq!(*handle.id(), 5u64);
}
