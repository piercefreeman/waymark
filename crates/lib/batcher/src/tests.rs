//! Unit tests for the shared loop's lifecycle — closed intake, drop-driven
//! drain, and signal-driven shutdown — exercised through one mode each;
//! per-mode semantics are tested in the strategy sub-modules.

use std::sync::Arc;

use crate::test_helpers::{nz, recording_flush, secs};
use crate::{Policy, read_batcher, write_batcher};

#[tokio::test]
async fn submit_after_batcher_is_gone_reports_closed() {
    let (handle, batcher) = write_batcher(
        Policy {
            max_batch: nz(4),
            max_delay: secs(3600),
        },
        recording_flush(Arc::new(std::sync::Mutex::new(Vec::new())), |item: u32| {
            item
        }),
        std::future::pending(),
    );
    // The batcher never runs and is dropped, closing the intake channel.
    drop(batcher);

    let result = handle.submit(1).await;
    assert!(
        matches!(result, Err(crate::Closed)),
        "submit must report Closed"
    );
}

#[tokio::test]
async fn dropping_all_handles_ends_the_batcher() {
    let (handle, batcher) = write_batcher(
        Policy {
            max_batch: nz(4),
            max_delay: secs(3600),
        },
        recording_flush(Arc::new(std::sync::Mutex::new(Vec::new())), |item: u32| {
            item
        }),
        std::future::pending(),
    );
    let batcher = tokio::spawn(batcher);

    let clone = handle.clone();
    drop(handle);
    drop(clone);

    batcher.await.expect("batcher join");
}

#[tokio::test]
async fn shutdown_signal_stops_the_batcher_and_closes_intake() {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (handle, batcher) = read_batcher(
        Policy {
            // Neither trigger can fire on its own — only the shutdown signal
            // ends this batcher.
            max_batch: nz(1000),
            max_delay: secs(3600),
        },
        recording_flush(Arc::new(std::sync::Mutex::new(Vec::new())), |key: u32| key),
        async move {
            let _ = shutdown_rx.await;
        },
    );
    let batcher = tokio::spawn(batcher);

    // Signal shutdown while a handle is still alive: the batcher must exit
    // anyway, which drop-driven shutdown alone could not do.
    shutdown_tx.send(()).expect("send shutdown");
    batcher.await.expect("batcher join");

    assert!(
        matches!(handle.submit(1).await, Err(crate::Closed)),
        "submit after shutdown must report Closed",
    );
}
