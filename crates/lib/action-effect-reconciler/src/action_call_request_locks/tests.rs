use std::sync::Arc;
use std::time::{Duration, Instant};

use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::{
    RenewalStatus, RequestLockRenewal,
};
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::action_call_request_locks::{PendingCallRequest, Tracker};
use crate::test_support::TestKey;

const TIME_TO_LIVE: Duration = Duration::from_secs(60);

fn key(promise: usize) -> TestKey {
    TestKey {
        vm_id: 42,
        promise_state_id: PromiseStateId(promise),
    }
}

fn shared() -> Arc<std::sync::Mutex<Tracker<u64>>> {
    Arc::new(std::sync::Mutex::new(Tracker::new()))
}

fn track_lock(
    tracker: &Arc<std::sync::Mutex<Tracker<u64>>>,
    promise: usize,
) -> PendingCallRequest<u64> {
    Tracker::track_lock(tracker, key(promise), Instant::now() + TIME_TO_LIVE)
}

#[test]
fn a_lock_is_tracked_from_the_moment_it_is_taken() {
    let shared = shared();
    let pending = track_lock(&shared, 1);

    let tracker = shared.lock().unwrap();
    assert!(tracker.is_tracking(&key(1)));
    assert_eq!(tracker.tracked_keys(), vec![key(1)]);
    assert!(tracker.pending_unlocks().is_none());
    drop(tracker);

    // Confirming keeps it tracked and nothing is up for unlocking.
    pending.confirm_requested();
    let tracker = shared.lock().unwrap();
    assert!(tracker.is_tracking(&key(1)));
    assert!(tracker.pending_unlocks().is_none());
}

#[test]
fn a_dropped_pending_call_request_untracks_and_unlocks_its_lock() {
    let shared = shared();
    let pending = track_lock(&shared, 1);
    drop(pending);

    let tracker = shared.lock().unwrap();
    assert!(!tracker.is_tracking(&key(1)));
    let pending_unlocks = tracker.pending_unlocks().expect("one pending unlock");
    assert_eq!(
        pending_unlocks.keys().iter().copied().collect::<Vec<_>>(),
        vec![key(1)]
    );
}

#[test]
fn an_unconfirmed_unlock_leaves_the_keys_pending() {
    let shared = shared();
    drop(track_lock(&shared, 1));

    let tracker = shared.lock().unwrap();
    let pending = tracker.pending_unlocks().expect("pending");
    drop(pending);

    assert!(tracker.pending_unlocks().is_some());
}

#[test]
fn a_confirmed_unlock_removes_exactly_its_keys() {
    let shared = shared();
    drop(track_lock(&shared, 1));

    let pending = shared.lock().unwrap().pending_unlocks().expect("pending");
    // A second request fails to go through while the unlock is in flight.
    drop(track_lock(&shared, 2));

    let mut tracker = shared.lock().unwrap();
    pending.confirm_unlocked(&mut tracker);
    let still_pending = tracker.pending_unlocks().expect("the later key stays");
    assert_eq!(
        still_pending.keys().iter().copied().collect::<Vec<_>>(),
        vec![key(2)]
    );
}

/// A lock taken again on a key whose earlier lock is still up for
/// unlocking: the stale unlock is cancelled, and a confirmation of it
/// leaves the fresh lock tracked.
#[test]
fn tracking_a_lock_again_cancels_its_pending_unlock() {
    let shared = shared();
    drop(track_lock(&shared, 1));
    let pending = shared.lock().unwrap().pending_unlocks().expect("pending");

    let fresh = track_lock(&shared, 1);
    {
        let tracker = shared.lock().unwrap();
        assert!(tracker.is_tracking(&key(1)));
        assert!(tracker.pending_unlocks().is_none());
    }

    // The unlock snapshot taken before the fresh lock confirms: the fresh
    // lock is not what it unlocked.
    pending.confirm_unlocked(&mut shared.lock().unwrap());
    assert!(shared.lock().unwrap().is_tracking(&key(1)));
    fresh.confirm_requested();
    assert!(shared.lock().unwrap().is_tracking(&key(1)));
}

#[test]
fn the_fence_is_the_given_deadline_until_renewed() {
    let shared = shared();
    let now = Instant::now();
    Tracker::track_lock(&shared, key(1), now + TIME_TO_LIVE).confirm_requested();

    let mut tracker = shared.lock().unwrap();
    assert_eq!(tracker.fence_deadline(&key(1)), Some(now + TIME_TO_LIVE));
    assert!(tracker.keys_past_fence(now).is_empty());
    assert_eq!(tracker.keys_past_fence(now + TIME_TO_LIVE), vec![key(1)]);

    // A confirmed renewal pushes the deadline out from its own instant.
    let renewed_at = now + Duration::from_secs(30);
    tracker.apply_renewal(
        RequestLockRenewal {
            key: key(1),
            status: RenewalStatus::Renewed,
        },
        renewed_at,
        TIME_TO_LIVE,
    );
    assert_eq!(
        tracker.fence_deadline(&key(1)),
        Some(renewed_at + TIME_TO_LIVE)
    );
    assert!(tracker.keys_past_fence(now + TIME_TO_LIVE).is_empty());
}

#[test]
fn a_renewal_never_adds_a_lock() {
    let shared = shared();
    let now = Instant::now();
    // The lock was dropped while its renewal was in flight.
    drop(track_lock(&shared, 1));

    let mut tracker = shared.lock().unwrap();
    tracker.apply_renewal(
        RequestLockRenewal {
            key: key(1),
            status: RenewalStatus::Renewed,
        },
        now,
        TIME_TO_LIVE,
    );

    assert!(!tracker.is_tracking(&key(1)));
    assert!(tracker.pending_unlocks().is_some());
}

#[test]
fn renewal_statuses_prune_or_keep() {
    let shared = shared();
    let now = Instant::now();
    for promise in 1..=3 {
        Tracker::track_lock(&shared, key(promise), now + TIME_TO_LIVE).confirm_requested();
    }

    let mut tracker = shared.lock().unwrap();
    let renewal = |promise, status| RequestLockRenewal {
        key: key(promise),
        status,
    };
    tracker.apply_renewal(renewal(1, RenewalStatus::Missing), now, TIME_TO_LIVE);
    tracker.apply_renewal(renewal(2, RenewalStatus::Unconfirmed), now, TIME_TO_LIVE);
    tracker.apply_renewal(renewal(3, RenewalStatus::HeldElsewhere), now, TIME_TO_LIVE);

    assert!(!tracker.is_tracking(&key(1)));
    assert!(tracker.is_tracking(&key(2)));
    assert!(tracker.is_tracking(&key(3)));
    // The unconfirmed and the held-elsewhere lock keep their deadlines.
    let mut past_fence = tracker.keys_past_fence(now + TIME_TO_LIVE);
    past_fence.sort_by_key(|key| key.promise_state_id);
    assert_eq!(past_fence, vec![key(2), key(3)]);
}
