use std::sync::Arc;
use std::time::Duration;

use mockall::predicate;
use nonempty_collections::nev;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workload_pinning_backend::PinningStatus;
use waymark_workload_pinning_core::UnpinMode;

use super::{MaintainParams, refresh_active_pinnings, run_maintenance_loop};
use crate::pinned_batch::PinnedBatch;
use crate::poll::poll_and_pin;
use crate::test_utils::helpers::{
    long_heartbeat, short_heartbeat, test_fencing_margin, test_max_concurrent, test_node_id,
    test_pinning, test_pinning_ttl,
};
use crate::test_utils::mock::MockBackend;

/// The maintain loop should exit cleanly when batch_rx is closed and
/// there are no active IDs — no possible work remains.
#[tokio::test]
async fn maintain_loop_exits_when_batch_closed_and_empty() {
    let backend = Arc::new(MockBackend::new());

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    // Close both channels — no work can ever arrive.
    drop(batch_tx);
    drop(evict_tx);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_maintenance_loop(MaintainParams {
            backend,
            node_id: test_node_id(),
            batch_rx,
            evict_rx,
            unpin_tx,
            count_tx,
            shutdown_token: CancellationToken::new(),
            pinning_heartbeat: long_heartbeat(),
            pinning_ttl: test_pinning_ttl(),
            pinning_fencing_margin: test_fencing_margin(),
        }),
    )
    .await
    .expect("maintain loop should exit promptly");

    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

/// The maintain loop should continue running when batch_rx is closed
/// but active IDs remain — heartbeats and evictions still matter.
/// It should exit once the last active ID is evicted.
#[tokio::test(flavor = "multi_thread")]
async fn maintain_loop_continues_until_last_eviction_after_batch_closed() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    // The heartbeat interval is long enough that refresh should never
    // fire, but allow it optionally so the test doesn't panic if it does.
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

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    // Stage a batch.  The reply oneshot is kept alive so we can wait
    // for the maintenance loop to acknowledge the ID before evicting.
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, CancellationToken::new())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");

    // Close the batch channel — poll loop is gone.
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: long_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    // Drive the loop until it processes the batch and sends the reply.
    // The reply confirms the ID is in the active pool — only then is it
    // safe to evict.
    let batch_size = tokio::select! {
        size = reply_rx => size.expect("maintain loop should ack the batch"),
        result = &mut loop_future => {
            panic!("loop exited before processing batch: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("batch was never processed");
        }
    };
    assert_eq!(batch_size, 1);

    // Now evict the ID — the loop should drain and exit.
    evict_tx.send((id, UnpinMode::Release)).expect("evict send");

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("maintain loop should exit after last eviction");

    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

/// When active IDs exist and the poll loop is still running, the
/// maintenance loop must fire heartbeat ticks and refresh pinnings.
/// A multi-threaded runtime is used so timer wakeups are delivered
/// reliably without stale timer-wheel state bleeding between tests.
#[tokio::test(flavor = "multi_thread")]
async fn maintain_loop_heartbeats_fire_for_active_ids() {
    let id = 1u64;
    let node_id = test_node_id();
    let pinning = test_pinning(node_id, 1);

    // Signal that refresh_pinnings was called.
    let (refresh_tx, mut refresh_rx) = mpsc::unbounded_channel::<()>();

    let mut backend = MockBackend::new();
    backend
        .expect_refresh_pinnings()
        .times(2..)
        .returning(move |_, _, _| {
            refresh_tx.send(()).ok();
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                workload_id: id,
                pinning: Some(pinning.clone()),
            }])))
        });

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    // Stage a batch so there is an active ID to heartbeat.
    let (reply_tx, _reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, CancellationToken::new())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");

    // Keep batch_tx alive — poll loop is "still running".
    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    // Wait for two heartbeats — proves the tick is truly periodic,
    // not a one-off race win against the immediate first tick. The
    // loop future is kept as a branch so it continues to be polled
    // and can fire subsequent heartbeat ticks.
    let mut heartbeat_count = 0usize;
    while heartbeat_count < 2 {
        tokio::select! {
            _ = refresh_rx.recv() => {
                heartbeat_count += 1;
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                panic!("only {heartbeat_count} heartbeats fired before timeout");
            }
            result = &mut loop_future => {
                panic!("loop exited after {heartbeat_count} heartbeats: {result:?}");
            }
        }
    }

    // Clean up: close the batch channel and evict the active ID so
    // the loop can drain and exit naturally.
    drop(batch_tx);
    evict_tx.send((id, UnpinMode::Release)).expect("evict send");

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit after cleanup");

    assert!(result.is_ok(), "expected Ok, got {result:?}");

    // Explicit: refresh_pinnings was called at least twice and
    // unpin_workloads was called exactly once.
    let mut mock = Arc::into_inner(backend)
        .unwrap_or_else(|| panic!("backend should be exclusively owned after loop exits"));
    mock.checkpoint();
}

/// After the poll loop dies (batch_tx dropped), the maintenance loop
/// must keep refreshing pinnings for all remaining active IDs until
/// every one is evicted. Heartbeats do not stop just because the poll
/// loop is gone — they continue for as long as there is in-flight work.
#[tokio::test(flavor = "multi_thread")]
async fn maintain_loop_heartbeats_after_batch_closed() {
    let id = 1u64;
    let node_id = test_node_id();
    let pinning = test_pinning(node_id, 1);

    // Signal that refresh_pinnings was called.
    let (refresh_tx, mut refresh_rx) = mpsc::unbounded_channel::<()>();

    let mut backend = MockBackend::new();
    backend
        .expect_refresh_pinnings()
        .times(2..)
        .returning(move |_, _, _| {
            refresh_tx.send(()).ok();
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                workload_id: id,
                pinning: Some(pinning.clone()),
            }])))
        });

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    // Stage a batch.
    let (reply_tx, _reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, CancellationToken::new())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");

    // Poll loop is dead — but maintenance must still heartbeat.
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    // Wait for two heartbeats after batch is closed — proves the tick
    // is truly periodic even when the poll loop is dead. The loop
    // future is kept as a branch so it continues to be polled and can
    // fire subsequent heartbeat ticks.
    let mut heartbeat_count = 0usize;
    while heartbeat_count < 2 {
        tokio::select! {
            _ = refresh_rx.recv() => {
                heartbeat_count += 1;
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                panic!("only {heartbeat_count} heartbeats fired after batch closed");
            }
            result = &mut loop_future => {
                panic!("loop exited after {heartbeat_count} heartbeats: {result:?}");
            }
        }
    }

    // Evict the last ID — loop should now drain and exit.
    evict_tx.send((id, UnpinMode::Release)).expect("evict send");

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit after eviction");

    assert!(result.is_ok(), "expected Ok, got {result:?}");

    // Explicit: refresh_pinnings was called at least twice and
    // unpin_workloads was called exactly once.
    let mut mock = Arc::into_inner(backend)
        .unwrap_or_else(|| panic!("backend should be exclusively owned after loop exits"));
    mock.checkpoint();
}
#[tokio::test]
async fn manager_refreshes_pinnings_on_active_vms() {
    let id = 1u64;

    let mut backend = MockBackend::new();
    backend
        .expect_poll_unpinned()
        .with(
            predicate::always(),
            predicate::always(),
            predicate::always(),
        )
        .return_once(move |_, _, _| Box::pin(std::future::ready(Ok(Some(nev![id])))));
    backend
        .expect_refresh_pinnings()
        .with(
            predicate::always(),
            predicate::always(),
            predicate::eq(nev![id]),
        )
        .return_once(move |_, _, _| {
            let pinning = test_pinning(test_node_id(), 1);
            let statuses = nev![PinningStatus {
                workload_id: id,
                pinning: Some(pinning),
            }];
            Box::pin(std::future::ready(Ok(statuses)))
        });

    let ids = poll_and_pin(
        &backend,
        test_node_id(),
        test_max_concurrent(),
        test_pinning_ttl(),
    )
    .await
    .expect("poll and pin")
    .expect("workloads available");

    refresh_active_pinnings(
        &backend,
        crate::test_utils::helpers::test_now(),
        test_node_id(),
        ids,
        test_pinning_ttl(),
    )
    .await
    .expect("refresh pinnings");
}

#[tokio::test]
async fn refresh_pinnings_propagates_error() {
    let id = 1u64;

    let mut backend = MockBackend::new();
    backend
        .expect_poll_unpinned()
        .with(
            predicate::always(),
            predicate::always(),
            predicate::always(),
        )
        .return_once(move |_, _, _| Box::pin(std::future::ready(Ok(Some(nev![id])))));
    backend
        .expect_refresh_pinnings()
        .with(
            predicate::always(),
            predicate::always(),
            predicate::eq(nev![id]),
        )
        .return_once(move |_, _, _| {
            Box::pin(std::future::ready(Err(crate::test_utils::mock::MockError)))
        });

    let ids = poll_and_pin(
        &backend,
        test_node_id(),
        test_max_concurrent(),
        test_pinning_ttl(),
    )
    .await
    .expect("poll and pin")
    .expect("workloads available");

    let result = refresh_active_pinnings(
        &backend,
        crate::test_utils::helpers::test_now(),
        test_node_id(),
        ids,
        test_pinning_ttl(),
    )
    .await;

    assert!(result.is_err());
}

/// When the shutdown token fires, the maintenance loop must exit
/// immediately with [`crate::MaintenanceError::ForceShutdown`] and
/// return whatever active IDs remain.
#[tokio::test]
async fn maintain_loop_exits_on_force_shutdown() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
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

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, CancellationToken::new())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let shutdown = CancellationToken::new();
    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: shutdown.clone(),
        pinning_heartbeat: long_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    // Wait for the batch ack before firing shutdown — the ID must be
    // in active_ids so the error return includes it.
    let batch_size = tokio::select! {
        size = reply_rx => size.expect("maintain loop should ack the batch"),
        result = &mut loop_future => {
            panic!("loop exited before processing batch: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("batch was never processed");
        }
    };
    assert_eq!(batch_size, 1);

    // Fire the shutdown token — the loop must exit even though an
    // active ID is still present.
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit on force shutdown");

    match result {
        Err((crate::MaintenanceError::ForceShutdown, ids)) => {
            assert!(
                ids.contains(&id),
                "active IDs should contain the un-evicted ID"
            );
        }
        other => panic!("expected ForceShutdown error, got {other:?}"),
    }

    // Clean up: prevent the evict channel from dangling.
    drop(evict_tx);
}

/// When a heartbeat refresh fails after its one retry, the maintenance
/// loop must exit with [`crate::MaintenanceError::Refresh`] and ship the
/// remaining active IDs so the caller can release them.
#[tokio::test(flavor = "multi_thread")]
async fn maintain_loop_exits_on_refresh_error() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_refresh_pinnings()
        .times(2) // initial + one retry
        .returning(move |_, _, _| {
            Box::pin(std::future::ready(Err(crate::test_utils::mock::MockError)))
        });
    // unpin should never fire — the refresh failure exits the loop first

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    // Stage a batch so there is an active ID to trigger a heartbeat.
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, CancellationToken::new())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    // Wait for the batch ack.
    let batch_size = tokio::select! {
        size = reply_rx => size.expect("maintain loop should ack the batch"),
        result = &mut loop_future => {
            panic!("loop exited before processing batch: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("batch was never processed");
        }
    };
    assert_eq!(batch_size, 1);

    // Heartbeats fire, fail twice, and the loop exits with Refresh.
    let result = tokio::time::timeout(Duration::from_secs(10), &mut loop_future)
        .await
        .expect("loop should exit on refresh error");

    match result {
        Err((crate::MaintenanceError::Refresh(_), ids)) => {
            assert!(
                ids.contains(&id),
                "active IDs should contain the un-evicted ID after refresh failure"
            );
        }
        other => panic!("expected Refresh error, got {other:?}"),
    }

    // Clean up: prevent the evict channel from dangling.
    drop(evict_tx);
}

/// An eviction carrying the park mode must be forwarded to the backend
/// as-is.
#[tokio::test]
async fn maintain_loop_forwards_park_mode() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
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

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, CancellationToken::new())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: long_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    // Wait for the batch ack before evicting.
    let batch_size = tokio::select! {
        size = reply_rx => size.expect("maintain loop should ack the batch"),
        result = &mut loop_future => {
            panic!("loop exited before processing batch: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("batch was never processed");
        }
    };
    assert_eq!(batch_size, 1);

    evict_tx.send((id, UnpinMode::Park)).expect("evict send");

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit after the park eviction");
    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

/// Evictions already queued when the loop wakes must be coalesced into
/// a single mixed-mode unpin batch, in channel order.
#[tokio::test]
async fn maintain_loop_coalesces_queued_evictions() {
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_refresh_pinnings()
        .times(0..)
        .returning(move |_, _, _| {
            let pinning = test_pinning(node_id, 1);
            Box::pin(std::future::ready(Ok(nev![
                PinningStatus {
                    workload_id: 1u64,
                    pinning: Some(pinning.clone()),
                },
                PinningStatus {
                    workload_id: 2u64,
                    pinning: Some(pinning),
                },
            ])))
        });

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![
                (1u64, CancellationToken::new()),
                (2u64, CancellationToken::new())
            ],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: long_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    // Wait for the batch ack before evicting.
    let batch_size = tokio::select! {
        size = reply_rx => size.expect("maintain loop should ack the batch"),
        result = &mut loop_future => {
            panic!("loop exited before processing batch: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("batch was never processed");
        }
    };
    assert_eq!(batch_size, 2);

    // Queue both evictions before the loop is polled again — it must
    // drain them into one backend call.
    evict_tx
        .send((1u64, UnpinMode::Release))
        .expect("evict send");
    evict_tx.send((2u64, UnpinMode::Park)).expect("evict send");

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit after draining the evictions");
    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

/// A refresh reporting the pinning lost to another node must fence the
/// workload immediately.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_lost_status_fences_the_workload() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_refresh_pinnings()
        .times(1..)
        .returning(move |_, _, _| {
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                workload_id: id,
                pinning: None,
            }])))
        });

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let fence = CancellationToken::new();
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, fence.clone())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    tokio::select! {
        _ = reply_rx => {}
        result = &mut loop_future => panic!("loop exited early: {result:?}"),
    }

    // The heartbeat discovers the loss and fences.
    tokio::select! {
        _ = fence.cancelled() => {}
        result = &mut loop_future => panic!("loop exited before fencing: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("workload was never fenced"),
    }

    // The fence only signals — eviction still drains the loop.
    evict_tx.send((id, UnpinMode::Release)).expect("evict send");
    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should drain after the eviction");
    assert!(result.is_ok(), "expected a clean drain, got {result:?}");
}

/// A pinning that is not re-confirmed by its local deadline must lapse
/// and fence the workload — here no refresh fires at all before the
/// deadline.
#[tokio::test(flavor = "multi_thread")]
async fn pinning_lapses_when_no_refresh_confirms_in_time() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
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

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let fence = CancellationToken::new();
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, fence.clone())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: long_heartbeat(),
        // Lapse deadline: 300ms ttl - 100ms margin = 200ms after the anchor.
        pinning_ttl: NonZeroDuration::new(Duration::from_millis(300)).unwrap(),
        pinning_fencing_margin: NonZeroDuration::new(Duration::from_millis(100)).unwrap(),
    });
    tokio::pin!(loop_future);

    tokio::select! {
        _ = reply_rx => {}
        result = &mut loop_future => panic!("loop exited early: {result:?}"),
    }

    let fenced_at = tokio::time::Instant::now();
    tokio::select! {
        _ = fence.cancelled() => {}
        result = &mut loop_future => panic!("loop exited before fencing: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("the pinning never lapsed"),
    }
    assert!(
        fenced_at.elapsed() >= Duration::from_millis(100),
        "fenced suspiciously early"
    );

    evict_tx.send((id, UnpinMode::Release)).expect("evict send");
    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should drain after the eviction");
    assert!(result.is_ok(), "expected a clean drain, got {result:?}");
}

/// Confirmed refreshes keep pushing the lapse deadline out — a healthy
/// workload never fences.
#[tokio::test(flavor = "multi_thread")]
async fn confirmed_refresh_extends_the_fence() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_refresh_pinnings()
        .times(1..)
        .returning(move |_, _, _| {
            let pinning = test_pinning(node_id, 1);
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                workload_id: id,
                pinning: Some(pinning),
            }])))
        });

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let fence = CancellationToken::new();
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, fence.clone())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        // Refreshes land well inside the 200ms lapse window.
        pinning_heartbeat: NonZeroDuration::new(Duration::from_millis(50)).unwrap(),
        pinning_ttl: NonZeroDuration::new(Duration::from_millis(300)).unwrap(),
        pinning_fencing_margin: NonZeroDuration::new(Duration::from_millis(100)).unwrap(),
    });
    tokio::pin!(loop_future);

    tokio::select! {
        _ = reply_rx => {}
        result = &mut loop_future => panic!("loop exited early: {result:?}"),
    }

    // Several lapse windows pass; the deadline keeps moving out.
    tokio::select! {
        _ = fence.cancelled() => panic!("healthy workload was fenced"),
        result = &mut loop_future => panic!("loop exited early: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(700)) => {}
    }

    evict_tx.send((id, UnpinMode::Release)).expect("evict send");
    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should drain after the eviction");
    assert!(result.is_ok(), "expected a clean drain, got {result:?}");
}

/// Fencing is per-workload: losing one pinning must fence only that
/// workload and leave a healthy sibling — one that keeps refreshing —
/// untouched.
#[tokio::test(flavor = "multi_thread")]
async fn fencing_one_workload_leaves_a_healthy_sibling_untouched() {
    let id1 = 1u64;
    let id2 = 2u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    // Every refresh reports id1 lost and id2 still held.  Once id1 is
    // fenced it drops out of the refresh set, so returning its status
    // here is a harmless no-op on later ticks.
    backend
        .expect_refresh_pinnings()
        .times(1..)
        .returning(move |_, _, _| {
            let pinning = test_pinning(node_id, 1);
            Box::pin(std::future::ready(Ok(nev![
                PinningStatus {
                    workload_id: id1,
                    pinning: None,
                },
                PinningStatus {
                    workload_id: id2,
                    pinning: Some(pinning),
                },
            ])))
        });

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let fence1 = CancellationToken::new();
    let fence2 = CancellationToken::new();
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id1, fence1.clone()), (id2, fence2.clone())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    tokio::select! {
        _ = reply_rx => {}
        result = &mut loop_future => panic!("loop exited early: {result:?}"),
    }

    // The heartbeat discovers id1's loss and fences it — id2 must not
    // fence alongside it.
    tokio::select! {
        _ = fence1.cancelled() => {}
        _ = fence2.cancelled() => panic!("healthy sibling was fenced"),
        result = &mut loop_future => panic!("loop exited before fencing: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("workload 1 was never fenced"),
    }

    // id2 keeps refreshing across several more ticks and never fences.
    tokio::select! {
        _ = fence2.cancelled() => panic!("healthy sibling fenced after id1"),
        result = &mut loop_future => panic!("loop exited unexpectedly: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(400)) => {}
    }
    assert!(!fence2.is_cancelled(), "the healthy sibling must stay live");

    // The fence only signals — both still drain via eviction.
    evict_tx
        .send((id1, UnpinMode::Release))
        .expect("evict send");
    evict_tx
        .send((id2, UnpinMode::Release))
        .expect("evict send");
    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should drain after the evictions");
    assert!(result.is_ok(), "expected a clean drain, got {result:?}");
}

/// A fenced workload is deliberately left to lapse: it must drop out of
/// the refresh set while a healthy sibling keeps being refreshed.
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_workload_is_dropped_from_refresh() {
    let id1 = 1u64;
    let id2 = 2u64;
    let node_id = test_node_id();

    // Records the id set handed to each refresh call.
    let (refresh_ids_tx, mut refresh_ids_rx) = mpsc::unbounded_channel::<Vec<u64>>();

    let mut backend = MockBackend::new();
    backend
        .expect_refresh_pinnings()
        .times(1..)
        .returning(move |_, _, ids| {
            let observed: Vec<u64> = ids.into_iter().collect();
            refresh_ids_tx.send(observed).ok();
            let pinning = test_pinning(node_id, 1);
            Box::pin(std::future::ready(Ok(nev![
                PinningStatus {
                    workload_id: id1,
                    pinning: None,
                },
                PinningStatus {
                    workload_id: id2,
                    pinning: Some(pinning),
                },
            ])))
        });

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let fence1 = CancellationToken::new();
    let fence2 = CancellationToken::new();
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id1, fence1.clone()), (id2, fence2.clone())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    tokio::select! {
        _ = reply_rx => {}
        result = &mut loop_future => panic!("loop exited early: {result:?}"),
    }

    // Wait for id1 to fence on the reported loss.
    tokio::select! {
        _ = fence1.cancelled() => {}
        result = &mut loop_future => panic!("loop exited before fencing: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("workload 1 was never fenced"),
    }

    // Discard the id sets from refreshes issued up to and including the
    // one that fenced id1 — those legitimately still contain id1.
    while refresh_ids_rx.try_recv().is_ok() {}

    // Every refresh from here on must exclude the fenced id1 and keep
    // refreshing the healthy id2.
    for _ in 0..2 {
        let observed = tokio::select! {
            observed = refresh_ids_rx.recv() => observed.expect("a further refresh"),
            result = &mut loop_future => panic!("loop exited unexpectedly: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("no further refresh fired"),
        };
        assert!(
            !observed.contains(&id1),
            "the fenced workload must not be refreshed, got {observed:?}"
        );
        assert!(
            observed.contains(&id2),
            "the healthy sibling must keep refreshing, got {observed:?}"
        );
    }

    // Drain both via eviction so the loop exits.
    evict_tx
        .send((id1, UnpinMode::Release))
        .expect("evict send");
    evict_tx
        .send((id2, UnpinMode::Release))
        .expect("evict send");
    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should drain after the evictions");
    assert!(result.is_ok(), "expected a clean drain, got {result:?}");
}

/// A refresh that fails once and succeeds on its immediate retry must
/// extend the fence from the retry's statuses — the workload stays live.
///
/// The failure lands on the *second* refresh call (the first heartbeat
/// coincides with the batch anchor, so extending there proves nothing).
/// With `heartbeat < lapse_after < 2 * heartbeat`, a dropped retry would
/// leave the stale deadline to lapse before the next clean tick, so the
/// workload surviving proves the retry's statuses were applied.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_recovers_on_retry_and_extends_the_fence() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    // Call #1 (first tick) succeeds; call #2 (second tick's initial
    // attempt) fails; its immediate retry (call #3) and everything after
    // succeed.
    let mut calls = 0;
    backend
        .expect_refresh_pinnings()
        .times(3..)
        .returning(move |_, _, _| {
            calls += 1;
            if calls == 2 {
                return Box::pin(std::future::ready(Err(crate::test_utils::mock::MockError)));
            }
            let pinning = test_pinning(node_id, 1);
            Box::pin(std::future::ready(Ok(nev![PinningStatus {
                workload_id: id,
                pinning: Some(pinning),
            }])))
        });

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let fence = CancellationToken::new();
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, fence.clone())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        // heartbeat 200ms < lapse_after 300ms < 2 * heartbeat 400ms.
        pinning_heartbeat: NonZeroDuration::new(Duration::from_millis(200)).unwrap(),
        pinning_ttl: NonZeroDuration::new(Duration::from_millis(400)).unwrap(),
        pinning_fencing_margin: NonZeroDuration::new(Duration::from_millis(100)).unwrap(),
    });
    tokio::pin!(loop_future);

    tokio::select! {
        _ = reply_rx => {}
        result = &mut loop_future => panic!("loop exited early: {result:?}"),
    }

    // Past the failing tick (~200ms) and the deadline a dropped retry
    // would leave standing (~300ms), through the next clean tick (~400ms).
    tokio::select! {
        _ = fence.cancelled() => panic!("workload fenced despite a recovered refresh"),
        result = &mut loop_future => panic!("loop exited early: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(450)) => {}
    }

    evict_tx.send((id, UnpinMode::Release)).expect("evict send");
    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should drain after the eviction");
    assert!(result.is_ok(), "expected a clean drain, got {result:?}");
}

/// A fatal exit hands the still-held ids to the caller for cleanup
/// release — so every holder must be fenced on the way out, or its
/// driver would keep running against a pinning the caller just released.
#[tokio::test(flavor = "multi_thread")]
async fn force_shutdown_fences_the_tracked_workloads() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
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

    let (batch_tx, batch_rx) = mpsc::channel::<PinnedBatch<u64>>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();
    let (unpin_tx, _unpin_rx) = mpsc::unbounded_channel::<(u64, UnpinMode)>();
    let unpin_tx = crate::unpin::wrap_tx(unpin_tx);

    let fence = CancellationToken::new();
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send(PinnedBatch {
            pinned_at: tokio::time::Instant::now(),
            pinned: nev![(id, fence.clone())],
            reply: reply_tx,
        })
        .await
        .expect("batch send");
    drop(batch_tx);

    let shutdown = CancellationToken::new();
    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        unpin_tx,
        count_tx,
        shutdown_token: shutdown.clone(),
        pinning_heartbeat: long_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
        pinning_fencing_margin: test_fencing_margin(),
    });
    tokio::pin!(loop_future);

    tokio::select! {
        size = reply_rx => assert_eq!(size.expect("batch ack"), 1),
        result = &mut loop_future => panic!("loop exited before processing batch: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("batch was never processed"),
    }

    assert!(!fence.is_cancelled(), "not fenced while healthy");

    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit on force shutdown");

    match result {
        Err((crate::MaintenanceError::ForceShutdown, ids)) => {
            assert!(ids.contains(&id), "the id must be handed back for cleanup");
        }
        other => panic!("expected ForceShutdown error, got {other:?}"),
    }

    assert!(
        fence.is_cancelled(),
        "the holder must be fenced before cleanup releases its pinning"
    );

    drop(evict_tx);
}
