use std::sync::Arc;
use std::time::Duration;

use mockall::predicate;
use nonempty_collections::nev;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workload_pinning_backend::PinningStatus;
use waymark_workload_pinning_core::UnpinMode;

use crate::test_utils::helpers::{
    long_heartbeat, short_heartbeat, test_fencing_margin, test_max_concurrent, test_node_id,
    test_pinning, test_pinning_ttl, test_poll_interval, test_unpin_retry_interval,
};
use crate::test_utils::mock::{MockBackend, MockError};
use crate::{Params, PinnedHandle, run};

/// A normal shutdown via cancellation should exit cleanly.
#[tokio::test]
async fn run_exits_on_cancellation() {
    let mut backend = MockBackend::new();
    backend
        .expect_poll_unpinned()
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
        poll_interval: test_poll_interval(),
        pinning_heartbeat: short_heartbeat(),
        unpin_retry_interval: test_unpin_retry_interval(),
        pinning_fencing_margin: test_fencing_margin(),
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
        .expect_poll_unpinned()
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
        poll_interval: test_poll_interval(),
        pinning_heartbeat: short_heartbeat(),
        unpin_retry_interval: test_unpin_retry_interval(),
        pinning_fencing_margin: test_fencing_margin(),
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
    // First poll returns an workload; second poll errors.
    let mut poll_calls = 0;
    backend.expect_poll_unpinned().returning(move |_, _, _| {
        poll_calls += 1;
        if poll_calls == 1 {
            Box::pin(std::future::ready(Ok(Some(nev![id]))))
        } else {
            Box::pin(std::future::ready(Err(MockError)))
        }
    });
    // Heartbeat may fire while the workload is active, but the drain
    // cycle can complete before the first tick — optional expectation.
    backend
        .expect_refresh_pinnings()
        .times(0..)
        .returning(move |_, _, _| {
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                workload_id: id,
                pinning: Some(pinning.clone()),
            }])))
        });
    // Eviction unpins the workload.
    backend
        .expect_unpin_workloads()
        .with(
            predicate::eq(node_id),
            predicate::eq(nev![(id, UnpinMode::Release)]),
        )
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
            poll_interval: test_poll_interval(),
            pinning_heartbeat: short_heartbeat(),
            unpin_retry_interval: test_unpin_retry_interval(),
            pinning_fencing_margin: test_fencing_margin(),
        }),
    )
    .await
    .expect("run should exit after maintenance drains");

    // Ensure the handle was received and dropped.
    handle_dropper.await.expect("handle dropper panicked");

    // Explicit: unpin_workloads was called (and completed) before run() returned.
    let mut mock = Arc::into_inner(backend)
        .unwrap_or_else(|| panic!("backend should be exclusively owned after run() drops its Arc"));
    mock.checkpoint();

    match outcome.poll_error {
        Some(_) => {} // expected — poll errored
        other => panic!("expected Poll error on poll_error, got {other:?}"),
    }
}

/// After the poll loop dies, the maintenance loop must still execute
/// heartbeats for in-flight workloads. Three heartbeats are observed:
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
        backend.expect_poll_unpinned().returning(move |_, _, _| {
            poll_calls += 1;
            if poll_calls == 1 {
                // First call: hand out an workload so maintenance has
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
                    workload_id: id,
                    pinning: Some(pinning.clone()),
                }])))
            });
    }
    backend
        .expect_unpin_workloads()
        .with(
            predicate::eq(node_id),
            predicate::eq(nev![(id, UnpinMode::Release)]),
        )
        .return_once(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);

    let (pinned_tx, mut pinned_rx) =
        mpsc::channel::<nonempty_collections::NEVec<PinnedHandle<u64>>>(1);

    let heartbeat = Duration::from_millis(10);

    // The monitor owns the pinned handle — it holds the workload
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
            poll_interval: test_poll_interval(),
            pinning_heartbeat: NonZeroDuration::new(heartbeat).unwrap(),
            unpin_retry_interval: test_unpin_retry_interval(),
            pinning_fencing_margin: test_fencing_margin(),
        }),
    )
    .await
    .expect("run should exit after maintenance drains");

    monitor.await.expect("monitor panicked");

    // Explicit: unpin_workloads was called (and completed) before run() returned.
    let mut mock = Arc::into_inner(backend)
        .unwrap_or_else(|| panic!("backend should be exclusively owned after run() drops its Arc"));
    mock.checkpoint();

    match outcome.poll_error {
        Some(_) => {} // expected
        other => panic!("expected Poll error on poll_error, got {other:?}"),
    }
}

/// When the maintenance loop exits by force shutdown while workloads
/// are still active (their handles held, never evicted), cleanup must
/// unpin all of them with the release mode.
#[tokio::test]
async fn cleanup_unpins_remaining_workloads_on_force_shutdown() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    let mut poll_calls = 0;
    backend.expect_poll_unpinned().returning(move |_, _, _| {
        poll_calls += 1;
        if poll_calls == 1 {
            Box::pin(std::future::ready(Ok(Some(nev![id]))))
        } else {
            Box::pin(async {
                // Yield so the single-threaded runtime can schedule
                // spawned tasks between polls.
                tokio::task::yield_now().await;
                Ok(None)
            })
        }
    });
    backend
        .expect_unpin_workloads()
        .with(
            predicate::eq(node_id),
            predicate::eq(nev![(id, UnpinMode::Release)]),
        )
        .return_once(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);

    let (pinned_tx, mut pinned_rx) =
        mpsc::channel::<nonempty_collections::NEVec<PinnedHandle<u64>>>(1);
    let force_shutdown = CancellationToken::new();

    let run_future = run(Params {
        shutdown_token: CancellationToken::new(),
        force_shutdown_token: force_shutdown.clone(),
        backend: Arc::clone(&backend),
        node_id,
        pinned_tx,
        max_pinned: test_max_concurrent(),
        pinning_ttl: test_pinning_ttl(),
        poll_interval: test_poll_interval(),
        pinning_heartbeat: long_heartbeat(),
        unpin_retry_interval: test_unpin_retry_interval(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(run_future);

    // Receiving the handles proves the maintenance loop acked the batch,
    // so the workload is in the active set.
    let handles = tokio::select! {
        handles = pinned_rx.recv() => handles.expect("pinned handles dispatched"),
        _ = &mut run_future => panic!("run exited before dispatching handles"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("handles never dispatched"),
    };

    force_shutdown.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(5), &mut run_future)
        .await
        .expect("run should exit after force shutdown");

    // The handles were held for the whole run — the eviction path never
    // fired; only cleanup could have unpinned.
    drop(handles);

    match outcome.maintenance_error {
        Some(crate::MaintenanceError::ForceShutdown) => {}
        other => panic!("expected ForceShutdown, got {other:?}"),
    }
    assert!(
        outcome.unpin_error.is_none(),
        "expected no unpin error, got {:?}",
        outcome.unpin_error
    );

    // Explicit: the cleanup unpin was called exactly once.
    let mut mock = Arc::into_inner(backend)
        .unwrap_or_else(|| panic!("backend should be exclusively owned after run() drops its Arc"));
    mock.checkpoint();
}

/// When the cleanup unpin itself fails, the error must surface on
/// `unpin_error` instead of being swallowed.
#[tokio::test(flavor = "multi_thread")]
async fn cleanup_reports_unpin_error() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    let mut poll_calls = 0;
    backend.expect_poll_unpinned().returning(move |_, _, _| {
        poll_calls += 1;
        if poll_calls == 1 {
            Box::pin(std::future::ready(Ok(Some(nev![id]))))
        } else {
            Box::pin(async {
                // Yield so the single-threaded runtime can schedule
                // spawned tasks between polls.
                tokio::task::yield_now().await;
                Ok(None)
            })
        }
    });
    backend
        .expect_unpin_workloads()
        .with(
            predicate::eq(node_id),
            predicate::eq(nev![(id, UnpinMode::Release)]),
        )
        .returning(move |_, _| Box::pin(std::future::ready(Err(MockError))));
    // The heartbeat is short so the unpin loop's retries land inside the
    // test window; refreshes are incidental here.
    backend
        .expect_refresh_pinnings()
        .times(0..)
        .returning(move |_, _, _| {
            let pinning = test_pinning(node_id, 1);
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                workload_id: id,
                pinning: Some(pinning),
            }])))
        });

    let backend = Arc::new(backend);

    let (pinned_tx, mut pinned_rx) =
        mpsc::channel::<nonempty_collections::NEVec<PinnedHandle<u64>>>(1);
    let force_shutdown = CancellationToken::new();

    let run_future = run(Params {
        shutdown_token: CancellationToken::new(),
        force_shutdown_token: force_shutdown.clone(),
        backend: Arc::clone(&backend),
        node_id,
        pinned_tx,
        max_pinned: test_max_concurrent(),
        pinning_ttl: test_pinning_ttl(),
        poll_interval: test_poll_interval(),
        pinning_heartbeat: short_heartbeat(),
        unpin_retry_interval: test_unpin_retry_interval(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(run_future);

    let handles = tokio::select! {
        handles = pinned_rx.recv() => handles.expect("pinned handles dispatched"),
        _ = &mut run_future => panic!("run exited before dispatching handles"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("handles never dispatched"),
    };

    force_shutdown.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(10), &mut run_future)
        .await
        .expect("run should exit after force shutdown");

    drop(handles);

    assert!(
        outcome.unpin_error.is_some(),
        "expected the cleanup unpin failure to be reported"
    );
}

/// A handle unpinned with the park mode must flow end to end: the
/// consumer's `unpin(Park)` reaches the backend as a park eviction and
/// the run drains cleanly afterwards.
#[tokio::test]
async fn unpin_park_flows_end_to_end() {
    let id = 1u64;
    let node_id = test_node_id();
    let pinning = test_pinning(node_id, 1);

    let mut backend = MockBackend::new();
    // First poll returns the workload; second poll errors so the run
    // can drain and exit once the workload is parked.
    let mut poll_calls = 0;
    backend.expect_poll_unpinned().returning(move |_, _, _| {
        poll_calls += 1;
        if poll_calls == 1 {
            Box::pin(std::future::ready(Ok(Some(nev![id]))))
        } else {
            Box::pin(std::future::ready(Err(MockError)))
        }
    });
    backend
        .expect_refresh_pinnings()
        .times(0..)
        .returning(move |_, _, _| {
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                workload_id: id,
                pinning: Some(pinning.clone()),
            }])))
        });
    backend
        .expect_unpin_workloads()
        .with(
            predicate::eq(node_id),
            predicate::eq(nev![(id, UnpinMode::Park)]),
        )
        .return_once(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);

    let (pinned_tx, mut pinned_rx) =
        mpsc::channel::<nonempty_collections::NEVec<PinnedHandle<u64>>>(1);

    // Receive the pinned handles and park them all.
    let parker = tokio::spawn(async move {
        let handles = pinned_rx
            .recv()
            .await
            .expect("should receive pinned handle");
        for handle in handles {
            handle.unpin(UnpinMode::Park);
        }
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
            poll_interval: test_poll_interval(),
            pinning_heartbeat: short_heartbeat(),
            unpin_retry_interval: test_unpin_retry_interval(),
            pinning_fencing_margin: test_fencing_margin(),
        }),
    )
    .await
    .expect("run should exit after the workload is parked");

    parker.await.expect("parker panicked");

    // Explicit: the park eviction reached the backend before run() returned.
    let mut mock = Arc::into_inner(backend)
        .unwrap_or_else(|| panic!("backend should be exclusively owned after run() drops its Arc"));
    mock.checkpoint();

    assert!(
        outcome.unpin_error.is_none(),
        "expected no unpin error, got {:?}",
        outcome.unpin_error
    );
}
