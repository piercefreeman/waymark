//! Unit tests for the deduplicating (read) strategy's semantics.

use std::sync::Arc;

use crate::test_helpers::{nz, recording_flush, secs};
use crate::{Policy, read_batcher};

#[tokio::test]
async fn duplicate_keys_load_once_and_fan_out() {
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
async fn distinct_keys_each_get_their_own_value() {
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
