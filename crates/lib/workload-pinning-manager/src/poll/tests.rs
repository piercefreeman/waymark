use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use mockall::predicate;
use nonempty_collections::nev;
use tokio_util::sync::CancellationToken;

use super::{PollParams, poll_and_pin, run_poll_loop};
use crate::test_utils::helpers::{
    test_max_concurrent, test_node_id, test_pinning_ttl, test_poll_interval,
};
use crate::test_utils::mock::MockBackend;

#[tokio::test]
async fn manager_polls_and_pins_workloads() {
    let id1 = 1u64;
    let id2 = 2u64;

    let mut backend = MockBackend::new();
    backend
        .expect_poll_unpinned()
        .with(
            predicate::always(),
            predicate::always(),
            predicate::always(),
        )
        .return_once(move |_, _, _| Box::pin(std::future::ready(Ok(Some(nev![id1, id2])))));

    let pinning_ttl = test_pinning_ttl();

    let ids = poll_and_pin(&backend, test_node_id(), test_max_concurrent(), pinning_ttl)
        .await
        .expect("poll and pin")
        .expect("workloads available");

    let active_ids: HashSet<u64> = HashSet::from_iter(ids);
    assert_eq!(active_ids.len(), 2);
    assert!(active_ids.contains(&id1));
    assert!(active_ids.contains(&id2));
}

#[tokio::test]
async fn manager_polls_and_pins_workloads_none_when_no_work() {
    let mut backend = MockBackend::new();
    backend
        .expect_poll_unpinned()
        .with(
            predicate::always(),
            predicate::always(),
            predicate::always(),
        )
        .return_once(move |_, _, _| Box::pin(std::future::ready(Ok(None))));

    let pinning_ttl = test_pinning_ttl();

    let result = poll_and_pin(&backend, test_node_id(), test_max_concurrent(), pinning_ttl)
        .await
        .expect("poll and pin");

    assert!(result.is_none());
}

#[tokio::test]
async fn poll_and_pin_propagates_error() {
    let mut backend = MockBackend::new();
    backend
        .expect_poll_unpinned()
        .with(
            predicate::always(),
            predicate::always(),
            predicate::always(),
        )
        .return_once(move |_, _, _| {
            Box::pin(std::future::ready(Err(crate::test_utils::mock::MockError)))
        });

    let result = poll_and_pin(
        &backend,
        test_node_id(),
        test_max_concurrent(),
        test_pinning_ttl(),
    )
    .await;

    assert!(result.is_err());
}

/// Dropping all count_tx senders must not cause the poll loop to exit —
/// it should continue polling. The count channel is advisory, not a
/// liveness signal.
#[tokio::test]
async fn poll_loop_continues_when_count_channel_closes() {
    let mut backend = MockBackend::new();
    backend
        .expect_poll_unpinned()
        .returning(move |_, _, _| Box::pin(std::future::ready(Ok(None))));

    let backend = Arc::new(backend);

    let (_pinned_tx, _pinned_rx) = tokio::sync::mpsc::channel(1);
    let (_evict_tx, _evict_rx) = tokio::sync::mpsc::unbounded_channel();
    let (_batch_tx, _batch_rx) = tokio::sync::mpsc::channel(1);
    let (count_tx, count_rx) = tokio::sync::mpsc::unbounded_channel();

    // Close the count channel — all senders gone.
    drop(count_tx);

    let result = tokio::time::timeout(
        Duration::from_millis(200),
        run_poll_loop(PollParams {
            backend,
            node_id: test_node_id(),
            pinned_tx: _pinned_tx,
            evict_tx: _evict_tx,
            batch_tx: _batch_tx,
            count_rx,
            shutdown_token: CancellationToken::new(),
            max_pinned: test_max_concurrent(),
            pinning_ttl: test_pinning_ttl(),
            poll_interval: test_poll_interval(),
        }),
    )
    .await;

    assert!(
        result.is_err(),
        "poll loop should continue polling when count channel closes"
    );
}

/// When the pinned-handle receiver is closed, the poll loop must exit
/// with [`crate::PollLoopError::PinnedReceiverClosed`].
#[tokio::test]
async fn poll_loop_exits_on_pinned_receiver_closed() {
    let mut backend = MockBackend::new();
    backend
        .expect_poll_unpinned()
        .return_once(move |_, _, _| Box::pin(std::future::ready(Ok(Some(nev![1u64])))));

    let backend = Arc::new(backend);

    let (pinned_tx, pinned_rx) = tokio::sync::mpsc::channel(1);
    let (evict_tx, _evict_rx) = tokio::sync::mpsc::unbounded_channel();
    let (batch_tx, mut batch_rx) =
        tokio::sync::mpsc::channel::<crate::pinned_batch::PinnedBatch<u64>>(1);
    let (count_tx, count_rx) = tokio::sync::mpsc::unbounded_channel();

    // Drop the pinned-handle receiver before the loop dispatches.
    drop(pinned_rx);

    // Spawn a helper to consume the batch and reply — without this the
    // poll loop hangs waiting for the batch ack.
    let batch_acker = tokio::spawn(async move {
        while let Some(crate::pinned_batch::PinnedBatch { reply, .. }) = batch_rx.recv().await {
            let _ = reply.send(0);
        }
    });

    let result = run_poll_loop(PollParams {
        backend,
        node_id: test_node_id(),
        pinned_tx,
        evict_tx,
        batch_tx,
        count_rx,
        shutdown_token: CancellationToken::new(),
        max_pinned: test_max_concurrent(),
        pinning_ttl: test_pinning_ttl(),
        poll_interval: test_poll_interval(),
    })
    .await;

    batch_acker.abort();

    match result {
        Err(crate::PollLoopError::PinnedReceiverClosed) => {} // expected
        other => panic!("expected PinnedReceiverClosed, got {other:?}"),
    }

    drop(count_tx);
}
