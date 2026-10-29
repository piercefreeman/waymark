//! Flush-behavior tests for the shared lock batcher: positional per-VM
//! outcome alignment, and the no-retry failure fan.

use std::num::NonZeroUsize;
use std::sync::Arc;

use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::{PerVmLockerHandle, PerVmLockingError};
use crate::action_call_request::locks::Tracker;
use crate::test_support::{
    LOCK_OWNER, LOCK_TIME_TO_LIVE, MockBackend, MockRow, TestKey, shared_tracker, tracks_request,
};

/// A locker whose batcher only ever flushes on the size trigger, so a test
/// that completes proves its submissions shared one batch.
fn spawn_size_gated_locker(
    backend: &Arc<MockBackend>,
    tracker: &Arc<std::sync::Mutex<Tracker<u64>>>,
    max_batch: usize,
) -> PerVmLockerHandle<u64> {
    let (locker, batcher) = super::run(
        Arc::clone(backend),
        LOCK_OWNER,
        LOCK_TIME_TO_LIVE.try_into().unwrap(),
        Arc::clone(tracker),
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
            lock_nonce: None,
        },
    );
}

#[tokio::test]
async fn each_vm_gets_its_own_outcome_from_one_batch() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_unlocked_row(&backend, 1, 10);
    let locker = spawn_size_gated_locker(&backend, &tracker, 2);

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

    let seeded = seeded
        .await
        .expect("join")
        .expect("not closed")
        .expect("locked");
    assert_eq!(seeded.vm_id, 1);
    assert_eq!(seeded.locked.len(), 1);
    assert_eq!(seeded.locked[0].record.promise_state_id, PromiseStateId(10));

    let empty = empty
        .await
        .expect("join")
        .expect("not closed")
        .expect("locked");
    assert_eq!(empty.vm_id, 2);
    assert!(empty.locked.is_empty());
    assert!(empty.held_elsewhere.is_empty());
    assert!(empty.held_by_this_owner.is_empty());

    // The locked row's lock is tracked, its call pending; dropped
    // unconfirmed, the lock leaves the tracked set and is up for
    // unlocking.
    assert!(tracks_request(
        &tracker.lock().unwrap(),
        &TestKey {
            vm_id: 1,
            promise_state_id: PromiseStateId(10),
        }
    ));
    drop(seeded);
    let tracker = tracker.lock().unwrap();
    assert!(tracker.tracked_lock_ids().is_empty());
    let pending = tracker
        .pending_unlocks()
        .expect("the dropped pending call request's lock");
    assert_eq!(
        pending
            .lock_ids()
            .iter()
            .map(|lock_id| lock_id.request)
            .collect::<Vec<_>>(),
        vec![TestKey {
            vm_id: 1,
            promise_state_id: PromiseStateId(10),
        }]
    );
}

#[tokio::test]
async fn a_duplicate_vm_submission_routes_to_the_live_waiter() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_unlocked_row(&backend, 1, 10);
    let locker = spawn_size_gated_locker(&backend, &tracker, 2);

    // Same-vm duplicate in one window, submitted in listing order: the
    // earlier occurrence models the abandoned waiter and is folded out
    // with `Failed`; the newcomer gets the real outcome.
    let (abandoned, live) = tokio::join!(locker.submit(1), locker.submit(1));

    assert!(matches!(
        abandoned.expect("not closed"),
        Err(PerVmLockingError::Failed)
    ));
    let live = live.expect("not closed").expect("locked");
    assert_eq!(live.vm_id, 1);
    assert_eq!(live.locked.len(), 1);
    assert_eq!(live.locked[0].record.promise_state_id, PromiseStateId(10));
}

#[tokio::test]
async fn a_failed_batch_fails_every_waiter_without_retry() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    *backend.fail_locks.lock().unwrap() = true;
    let locker = spawn_size_gated_locker(&backend, &tracker, 2);

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
        assert!(matches!(outcome, Err(PerVmLockingError::Failed)));
    }
}
