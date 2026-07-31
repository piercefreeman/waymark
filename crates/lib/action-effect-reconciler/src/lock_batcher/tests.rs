//! Flush-behavior tests for the shared lock batcher: positional per-VM
//! outcome alignment, and the no-retry failure fan.

use std::num::NonZeroUsize;
use std::sync::Arc;

use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::lock_batcher::{LockError, VmLockerHandle, lock_batcher};
use crate::test_support::{MockBackend, MockRow, TestKey};

const LOCK_TIME_TO_LIVE: std::time::Duration = std::time::Duration::from_secs(60);

/// A locker whose batcher only ever flushes on the size trigger, so a test
/// that completes proves its submissions shared one batch.
fn spawn_size_gated_locker(backend: &Arc<MockBackend>, max_batch: usize) -> VmLockerHandle<u64> {
    let (locker, batcher) = lock_batcher(
        Arc::clone(backend),
        7u32,
        LOCK_TIME_TO_LIVE.try_into().unwrap(),
        waymark_batcher::Policy {
            max_batch: NonZeroUsize::new(max_batch).expect("non-zero"),
            max_delay: NonZeroDuration::from_secs(3600).expect("non-zero"),
        },
        std::future::pending(),
    );
    tokio::spawn(batcher);
    locker
}

fn seed_unlocked_row(backend: &MockBackend, vm_id: u64, promise: usize) {
    backend.rows.lock().unwrap().insert(
        TestKey {
            vm_id,
            promise_state_id: PromiseStateId(promise),
        },
        MockRow {
            effect_number: EffectNumber(1),
            request: b"call".to_vec(),
            locked_by: None,
            lock_expires_at: None,
        },
    );
}

#[tokio::test]
async fn each_vm_gets_its_own_outcome_from_one_batch() {
    let backend = Arc::new(MockBackend::default());
    seed_unlocked_row(&backend, 1, 10);
    let locker = spawn_size_gated_locker(&backend, 2);

    // The delay trigger is effectively off, so these can only resolve if
    // both VMs left in the same batch.
    let seeded = {
        let locker = locker.clone();
        tokio::spawn(async move { locker.submit(1).await })
    };
    let empty = {
        let locker = locker.clone();
        tokio::spawn(async move { locker.submit(2).await })
    };

    let (outcome, seeded_taken_at) = seeded
        .await
        .expect("join")
        .expect("not closed")
        .expect("locked");
    assert_eq!(outcome.vm_id, 1);
    assert_eq!(outcome.locked.len(), 1);
    assert_eq!(outcome.locked[0].promise_state_id, PromiseStateId(10));

    let (outcome, empty_taken_at) = empty
        .await
        .expect("join")
        .expect("not closed")
        .expect("locked");
    assert_eq!(outcome.vm_id, 2);
    assert!(outcome.locked.is_empty());
    assert!(outcome.held_elsewhere.is_empty());

    // One pre-send instant per flush: same-batch locks share their
    // fence base.
    assert_eq!(seeded_taken_at, empty_taken_at);
}

#[tokio::test]
async fn a_duplicate_vm_submission_routes_to_the_live_waiter() {
    let backend = Arc::new(MockBackend::default());
    seed_unlocked_row(&backend, 1, 10);
    let locker = spawn_size_gated_locker(&backend, 2);

    // Same-vm duplicate in one window, submitted in listing order: the
    // earlier occurrence models the abandoned waiter and is folded out
    // with `Failed`; the newcomer gets the real outcome.
    let (abandoned, live) = tokio::join!(locker.submit(1), locker.submit(1));

    assert!(matches!(
        abandoned.expect("not closed"),
        Err(LockError::Failed)
    ));
    let (outcome, _taken_at) = live.expect("not closed").expect("locked");
    assert_eq!(outcome.vm_id, 1);
    assert_eq!(outcome.locked.len(), 1);
    assert_eq!(outcome.locked[0].promise_state_id, PromiseStateId(10));
}

#[tokio::test]
async fn a_failed_batch_fails_every_waiter_without_retry() {
    let backend = Arc::new(MockBackend::default());
    *backend.fail_locks.lock().unwrap() = true;
    let locker = spawn_size_gated_locker(&backend, 2);

    let a = {
        let locker = locker.clone();
        tokio::spawn(async move { locker.submit(1).await })
    };
    let b = {
        let locker = locker.clone();
        tokio::spawn(async move { locker.submit(2).await })
    };

    for submission in [a, b] {
        let outcome = submission.await.expect("join").expect("not closed");
        assert!(matches!(outcome, Err(LockError::Failed)));
    }
}
