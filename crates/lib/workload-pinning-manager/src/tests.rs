use std::sync::Arc;
use std::time::Duration;

use mockall::predicate;
use nonempty_collections::nev;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workload_pinning_backend::PinningStatus;

use crate::test_utils::helpers::{
    short_heartbeat, test_max_concurrent, test_node_id, test_pinning, test_pinning_ttl,
};
use crate::test_utils::mock::{MockBackend, MockError};
use crate::{Params, PinnedHandle, run};

/// A normal shutdown via cancellation should exit cleanly.
#[tokio::test]
async fn run_exits_on_cancellation() {
    let mut backend = MockBackend::new();
    backend
        .expect_poll_unlocked()
        .returning(move |_, _, _| Box::pin(std::future::ready(Ok(None))));

    let backend = Arc::new(backend);

    let (pinned_tx, _pinned_rx) =
        mpsc::channel::<nonempty_collections::NEVec<PinnedHandle<u64>>>(1);
    let cancel = CancellationToken::new();

    let run_future = run(Params {
        shutdown_token: cancel.clone(),
        force_shutdown_token: CancellationToken::new(),
        backend,
        node_id: test_node_id(),
        pinned_tx,
        max_pinned: test_max_concurrent(),
        pinning_ttl: test_pinning_ttl(),
        pinning_heartbeat: short_heartbeat(),
    });

    tokio::pin!(run_future);

    // Let the loops start up, then cancel.
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(5), &mut run_future)
        .await
        .expect("run should exit promptly after cancellation");

    assert!(
        outcome.poll_error.is_none(),
        "expected no poll error, got {:?}",
        outcome.poll_error
    );
    assert!(
        outcome.maintenance_error.is_none(),
        "expected no maintenance error, got {:?}",
        outcome.maintenance_error
    );
}

/// When poll hits an error, run should propagate it and still clean up.
#[tokio::test]
async fn run_propagates_poll_error() {
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_poll_unlocked()
        .with(
            predicate::always(),
            predicate::always(),
            predicate::always(),
        )
        .returning(move |_, _, _| Box::pin(std::future::ready(Err(MockError))));

    let backend = Arc::new(backend);

    let (pinned_tx, _pinned_rx) =
        mpsc::channel::<nonempty_collections::NEVec<PinnedHandle<u64>>>(1);
    let cancel = CancellationToken::new();

    let run_future = run(Params {
        shutdown_token: cancel,
        force_shutdown_token: CancellationToken::new(),
        backend,
        node_id,
        pinned_tx,
        max_pinned: test_max_concurrent(),
        pinning_ttl: test_pinning_ttl(),
        pinning_heartbeat: short_heartbeat(),
    });

    let outcome = tokio::time::timeout(Duration::from_secs(5), run_future)
        .await
        .expect("run should exit promptly on poll error");

    match outcome.poll_error {
        Some(_) => {} // expected
        other => panic!("expected Poll error on poll_error, got {other:?}"),
    }
}

/// When the poll loop fails, the maintenance loop must continue
/// heartbeating and processing evictions until all in-flight work
/// drains naturally. Poll errors never cancel maintenance.
#[tokio::test]
async fn maintenance_drains_after_poll_error() {
    let id = 1u64;
    let node_id = test_node_id();
    let pinning = test_pinning(node_id, 1);

    let mut backend = MockBackend::new();
    // First poll returns an instance; second poll errors.
    let mut poll_calls = 0;
    backend.expect_poll_unlocked().returning(move |_, _, _| {
        poll_calls += 1;
        if poll_calls == 1 {
            Box::pin(std::future::ready(Ok(Some(nev![id]))))
        } else {
            Box::pin(std::future::ready(Err(MockError)))
        }
    });
    // Heartbeat may fire while the instance is active, but the drain
    // cycle can complete before the first tick — optional expectation.
    backend
        .expect_refresh_pinnings()
        .times(0..)
        .returning(move |_, _, _| {
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                instance_id: id,
                pinning: Some(pinning.clone()),
            }])))
        });
    // Eviction releases the pinning.
    backend
        .expect_release_pinnings()
        .with(predicate::eq(node_id), predicate::eq(nev![id]))
        .return_once(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);

    let (pinned_tx, mut pinned_rx) =
        mpsc::channel::<nonempty_collections::NEVec<PinnedHandle<u64>>>(1);

    // Spawn a helper to receive the pinned handle and drop it,
    // triggering eviction in the maintenance loop.
    let handle_dropper = tokio::spawn(async move {
        let handles = pinned_rx
            .recv()
            .await
            .expect("should receive pinned handle");
        drop(handles);
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        run(Params {
            shutdown_token: CancellationToken::new(),
            force_shutdown_token: CancellationToken::new(),
            backend: Arc::clone(&backend),
            node_id,
            pinned_tx,
            max_pinned: test_max_concurrent(),
            pinning_ttl: test_pinning_ttl(),
            pinning_heartbeat: short_heartbeat(),
        }),
    )
    .await
    .expect("run should exit after maintenance drains");

    // Ensure the handle was received and dropped.
    handle_dropper.await.expect("handle dropper panicked");

    // Explicit: release_pinnings was called (and completed) before run() returned.
    let mut mock = Arc::into_inner(backend)
        .unwrap_or_else(|| panic!("backend should be exclusively owned after run() drops its Arc"));
    mock.checkpoint();

    match outcome.poll_error {
        Some(_) => {} // expected — poll errored
        other => panic!("expected Poll error on poll_error, got {other:?}"),
    }
}

/// After the poll loop dies, the maintenance loop must still execute
/// heartbeats for in-flight instances. Three heartbeats are observed:
/// 1. Before poll error — proves heartbeats work normally.
/// 2. After signalling poll error — may race with termination; discarded.
/// 3. After poll is definitively dead — this is the proof.
#[tokio::test]
async fn maintenance_heartbeats_after_poll_is_dead() {
    let id = 1u64;
    let node_id = test_node_id();
    let pinning = test_pinning(node_id, 1);

    // mpsc instead of Notify so every heartbeat is queued — no lost wakeups
    // when multiple heartbeats fire between consumer polls.
    let (heartbeat_tx, mut heartbeat_rx) = mpsc::unbounded_channel();
    // Async mutex so the monitor can signal poll termination without
    // atomics. try_lock() lets the mock check synchronously.
    let poll_error = Arc::new(tokio::sync::Mutex::new(false));

    let mut backend = MockBackend::new();
    let mut poll_calls = 0;
    {
        let poll_error = Arc::clone(&poll_error);
        backend.expect_poll_unlocked().returning(move |_, _, _| {
            poll_calls += 1;
            if poll_calls == 1 {
                // First call: hand out an instance so maintenance has
                // something to heartbeat.
                return Box::pin(std::future::ready(Ok(Some(nev![id]))));
            }
            let poll_error = Arc::clone(&poll_error);
            Box::pin(async move {
                if *poll_error.lock().await {
                    // After the monitor signals, fail so the poll loop dies.
                    return Err(MockError);
                }
                // Yield so the single-threaded runtime can schedule
                // spawned tasks between polls.
                tokio::task::yield_now().await;
                Ok(None)
            })
        });
    }
    {
        let heartbeat_tx = heartbeat_tx.clone();
        backend
            .expect_refresh_pinnings()
            .times(..)
            .returning(move |_, _, _| {
                // Record every heartbeat so the monitor can count them.
                heartbeat_tx.send(()).ok();
                Box::pin(std::future::ready(Ok(nev![PinningStatus {
                    instance_id: id,
                    pinning: Some(pinning.clone()),
                }])))
            });
    }
    backend
        .expect_release_pinnings()
        .with(predicate::eq(node_id), predicate::eq(nev![id]))
        .return_once(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);

    let (pinned_tx, mut pinned_rx) =
        mpsc::channel::<nonempty_collections::NEVec<PinnedHandle<u64>>>(1);

    let heartbeat = Duration::from_millis(10);

    // The monitor owns the pinned handle — it holds the instance
    // in-flight while counting three heartbeats, then drops the handle
    // to trigger eviction so maintenance can drain and run() can exit.
    let monitor = tokio::spawn(async move {
        let handles = pinned_rx
            .recv()
            .await
            .expect("should receive pinned handle");

        heartbeat_rx.recv().await.expect("first heartbeat");
        *poll_error.lock().await = true;
        heartbeat_rx.recv().await.expect("second heartbeat");
        heartbeat_rx.recv().await.expect("third heartbeat — proof");

        drop(handles);
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        run(Params {
            shutdown_token: CancellationToken::new(),
            force_shutdown_token: CancellationToken::new(),
            backend: Arc::clone(&backend),
            node_id,
            pinned_tx,
            max_pinned: test_max_concurrent(),
            pinning_ttl: test_pinning_ttl(),
            pinning_heartbeat: NonZeroDuration::new(heartbeat).unwrap(),
        }),
    )
    .await
    .expect("run should exit after maintenance drains");

    monitor.await.expect("monitor panicked");

    // Explicit: release_pinnings was called (and completed) before run() returned.
    let mut mock = Arc::into_inner(backend)
        .unwrap_or_else(|| panic!("backend should be exclusively owned after run() drops its Arc"));
    mock.checkpoint();

    match outcome.poll_error {
        Some(_) => {} // expected
        other => panic!("expected Poll error on poll_error, got {other:?}"),
    }
}
