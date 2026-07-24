//! Unit tests for both batcher modes.
//!
//! The flush closure is a plain in-memory function that records the size of
//! each batch it saw and maps every input to an output, so the batching,
//! deduplication, timing, and delivery logic is exercised with no database or
//! domain types involved.

use std::num::NonZeroUsize;
use std::sync::Arc;

use nonempty_collections::NEVec;
use waymark_nonzero_duration::NonZeroDuration;

use crate::{Policy, read_batcher, write_batcher};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("non-zero")
}

fn secs(value: u64) -> NonZeroDuration {
    NonZeroDuration::from_secs(value).expect("non-zero")
}

fn millis(value: u64) -> NonZeroDuration {
    NonZeroDuration::from_millis(value).expect("non-zero")
}

/// A flush that records each batch's size and maps every input through `map`.
fn recording_flush<In, Out>(
    seen: Arc<std::sync::Mutex<Vec<usize>>>,
    map: impl Fn(In) -> Out + Clone,
) -> impl FnMut(NEVec<In>) -> std::future::Ready<NEVec<Out>> + Clone {
    move |batch: NEVec<In>| {
        seen.lock().expect("lock").push(batch.len().get());
        let outputs: Vec<Out> = batch.into_iter().map(map.clone()).collect();
        std::future::ready(NEVec::try_from_vec(outputs).expect("batch was non-empty"))
    }
}

// ---- write_batcher (positional) -------------------------------------------

#[tokio::test]
async fn write_flushes_when_the_batch_fills() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (handle, batcher) = write_batcher(
        Policy {
            max_batch: nz(3),
            max_delay: secs(3600),
        },
        recording_flush(Arc::clone(&seen), |item: u32| item * 10),
        std::future::pending(),
    );
    let batcher = tokio::spawn(batcher);

    let submissions: Vec<_> = (0..3)
        .map(|item| {
            let handle = handle.clone();
            tokio::spawn(async move { handle.submit(item).await })
        })
        .collect();

    for (item, submission) in submissions.into_iter().enumerate() {
        let output = submission.await.expect("join").expect("not closed");
        assert_eq!(output, item as u32 * 10, "each item gets its own output");
    }

    drop(handle);
    batcher.await.expect("batcher join");
    assert_eq!(
        *seen.lock().expect("lock"),
        vec![3],
        "one full batch of three"
    );
}

#[tokio::test(start_paused = true)]
async fn write_flushes_when_the_delay_elapses() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (handle, batcher) = write_batcher(
        Policy {
            max_batch: nz(1000),
            max_delay: millis(50),
        },
        recording_flush(Arc::clone(&seen), |item: u32| item),
        std::future::pending(),
    );
    let batcher = tokio::spawn(batcher);

    let a = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.submit(1).await })
    };
    let b = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.submit(2).await })
    };

    assert_eq!(a.await.expect("join").expect("not closed"), 1);
    assert_eq!(b.await.expect("join").expect("not closed"), 2);

    drop(handle);
    batcher.await.expect("batcher join");
    assert_eq!(
        *seen.lock().expect("lock"),
        vec![2],
        "delay-triggered batch"
    );
}

#[tokio::test]
async fn write_every_submitter_gets_its_own_output() {
    let (handle, batcher) = write_batcher(
        Policy {
            max_batch: nz(8),
            max_delay: secs(3600),
        },
        // Identity map: the output must be the submitter's own input, which
        // only holds if the positional zip lines up waiters and outputs.
        recording_flush(Arc::new(std::sync::Mutex::new(Vec::new())), |item: u64| {
            item
        }),
        std::future::pending(),
    );
    let batcher = tokio::spawn(batcher);

    let submissions: Vec<_> = (0..8u64)
        .map(|item| {
            let handle = handle.clone();
            tokio::spawn(async move { (item, handle.submit(item).await) })
        })
        .collect();

    for submission in submissions {
        let (item, output) = submission.await.expect("join");
        assert_eq!(output.expect("not closed"), item);
    }

    drop(handle);
    batcher.await.expect("batcher join");
}

// ---- read_batcher (deduplicating) -----------------------------------------

#[tokio::test]
async fn read_duplicate_keys_load_once_and_fan_out() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (handle, batcher) = read_batcher(
        // Two raw keys fill the batch; both are the same key.
        Policy {
            max_batch: nz(2),
            max_delay: secs(3600),
        },
        recording_flush(Arc::clone(&seen), |key: u32| key * 10),
        std::future::pending(),
    );
    let batcher = tokio::spawn(batcher);

    let a = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.submit(7).await })
    };
    let b = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.submit(7).await })
    };

    assert_eq!(a.await.expect("join").expect("not closed"), 70);
    assert_eq!(b.await.expect("join").expect("not closed"), 70);

    drop(handle);
    batcher.await.expect("batcher join");
    assert_eq!(
        *seen.lock().expect("lock"),
        vec![1],
        "the shared key was loaded once, not twice",
    );
}

#[tokio::test]
async fn read_distinct_keys_each_get_their_own_value() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (handle, batcher) = read_batcher(
        Policy {
            max_batch: nz(3),
            max_delay: secs(3600),
        },
        recording_flush(Arc::clone(&seen), |key: u64| key * 100),
        std::future::pending(),
    );
    let batcher = tokio::spawn(batcher);

    let submissions: Vec<_> = (1..=3u64)
        .map(|key| {
            let handle = handle.clone();
            tokio::spawn(async move { (key, handle.submit(key).await) })
        })
        .collect();

    for submission in submissions {
        let (key, value) = submission.await.expect("join");
        assert_eq!(value.expect("not closed"), key * 100);
    }

    drop(handle);
    batcher.await.expect("batcher join");
    assert_eq!(*seen.lock().expect("lock"), vec![3], "three distinct keys");
}

// ---- shared lifecycle (exercised through one mode) ------------------------

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
