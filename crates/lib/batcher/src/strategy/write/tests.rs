//! Unit tests for the positional (write) strategy's semantics.

use std::sync::Arc;

use crate::test_helpers::{millis, nz, recording_flush, secs};
use crate::{Policy, write_batcher};

#[tokio::test]
async fn flushes_when_the_batch_fills() {
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
async fn flushes_when_the_delay_elapses() {
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
async fn every_submitter_gets_its_own_output() {
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
