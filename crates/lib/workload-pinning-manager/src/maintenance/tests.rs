use std::sync::Arc;
use std::time::Duration;

use mockall::predicate;
use nonempty_collections::{NEVec, nev};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use waymark_workload_pinning_backend::PinningStatus;

use super::{MaintainParams, refresh_active_pinnings, run_maintenance_loop};
use crate::poll::poll_and_pin;
use crate::test_utils::helpers::{
    long_heartbeat, short_heartbeat, test_max_concurrent, test_node_id, test_pinning,
    test_pinning_ttl,
};
use crate::test_utils::mock::MockBackend;

/// The maintain loop should exit cleanly when batch_rx is closed and
/// there are no active IDs — no possible work remains.
#[tokio::test]
async fn maintain_loop_exits_when_batch_closed_and_empty() {
    let backend = Arc::new(MockBackend::new());

    let (batch_tx, batch_rx) = mpsc::channel::<(NEVec<u64>, oneshot::Sender<usize>)>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<u64>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();

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
            count_tx,
            shutdown_token: CancellationToken::new(),
            pinning_heartbeat: long_heartbeat(),
            pinning_ttl: test_pinning_ttl(),
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
    backend
        .expect_release_pinnings()
        .with(predicate::eq(node_id), predicate::eq(nev![id]))
        .return_once(move |_, _| Box::pin(std::future::ready(Ok(()))));
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

    let (batch_tx, batch_rx) = mpsc::channel::<(NEVec<u64>, oneshot::Sender<usize>)>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<u64>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();

    // Stage a batch.  The reply oneshot is kept alive so we can wait
    // for the maintenance loop to acknowledge the ID before evicting.
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send((nev![id], reply_tx))
        .await
        .expect("batch send");

    // Close the batch channel — poll loop is gone.
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: long_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
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
    evict_tx.send(id).expect("evict send");

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
    backend
        .expect_release_pinnings()
        .with(predicate::eq(node_id), predicate::eq(nev![id]))
        .return_once(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<(NEVec<u64>, oneshot::Sender<usize>)>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<u64>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();

    // Stage a batch so there is an active ID to heartbeat.
    let (reply_tx, _reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send((nev![id], reply_tx))
        .await
        .expect("batch send");

    // Keep batch_tx alive — poll loop is "still running".
    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
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
    evict_tx.send(id).expect("evict send");

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit after cleanup");

    assert!(result.is_ok(), "expected Ok, got {result:?}");

    // Explicit: refresh_pinnings was called at least twice and
    // release_pinnings was called exactly once.
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
    backend
        .expect_release_pinnings()
        .with(predicate::eq(node_id), predicate::eq(nev![id]))
        .return_once(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<(NEVec<u64>, oneshot::Sender<usize>)>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<u64>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();

    // Stage a batch.
    let (reply_tx, _reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send((nev![id], reply_tx))
        .await
        .expect("batch send");

    // Poll loop is dead — but maintenance must still heartbeat.
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
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
    evict_tx.send(id).expect("evict send");

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit after eviction");

    assert!(result.is_ok(), "expected Ok, got {result:?}");

    // Explicit: refresh_pinnings was called at least twice and
    // release_pinnings was called exactly once.
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

/// When releasing a pinning fails, the maintenance loop must exit with
/// the error and return the remaining active IDs.
#[tokio::test]
async fn maintain_loop_exits_on_release_error() {
    let id = 1u64;
    let node_id = test_node_id();

    let mut backend = MockBackend::new();
    backend
        .expect_release_pinnings()
        .with(predicate::eq(node_id), predicate::eq(nev![id]))
        .return_once(move |_, _| {
            Box::pin(std::future::ready(Err(crate::test_utils::mock::MockError)))
        });
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

    let (batch_tx, batch_rx) = mpsc::channel::<(NEVec<u64>, oneshot::Sender<usize>)>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<u64>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();

    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send((nev![id], reply_tx))
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: long_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
    });
    tokio::pin!(loop_future);

    // Wait for the batch ack — the ID must be in active_ids before
    // we evict, otherwise the eviction is a no-op.
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

    // Evict the ID — the release call will fail.
    evict_tx.send(id).expect("evict send");

    let result = tokio::time::timeout(Duration::from_secs(5), &mut loop_future)
        .await
        .expect("loop should exit on release error");

    match result {
        Err((crate::MaintenanceError::Release(_), ids)) => {
            assert!(
                ids.contains(&id),
                "the failed-to-release id must remain in active_ids, got {ids:?}"
            );
        }
        other => panic!("expected Release error, got {other:?}"),
    }
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

    let (batch_tx, batch_rx) = mpsc::channel::<(NEVec<u64>, oneshot::Sender<usize>)>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<u64>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();

    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send((nev![id], reply_tx))
        .await
        .expect("batch send");
    drop(batch_tx);

    let shutdown = CancellationToken::new();
    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        count_tx,
        shutdown_token: shutdown.clone(),
        pinning_heartbeat: long_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
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
    // release should never fire — the refresh failure exits the loop first
    backend
        .expect_release_pinnings()
        .times(0)
        .returning(move |_, _| Box::pin(std::future::ready(Ok(()))));

    let backend = Arc::new(backend);

    let (batch_tx, batch_rx) = mpsc::channel::<(NEVec<u64>, oneshot::Sender<usize>)>(1);
    let (evict_tx, evict_rx) = mpsc::unbounded_channel::<u64>();
    let (count_tx, _count_rx) = mpsc::unbounded_channel::<usize>();

    // Stage a batch so there is an active ID to trigger a heartbeat.
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    batch_tx
        .send((nev![id], reply_tx))
        .await
        .expect("batch send");
    drop(batch_tx);

    let loop_future = run_maintenance_loop(MaintainParams {
        backend: Arc::clone(&backend),
        node_id,
        batch_rx,
        evict_rx,
        count_tx,
        shutdown_token: CancellationToken::new(),
        pinning_heartbeat: short_heartbeat(),
        pinning_ttl: test_pinning_ttl(),
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
