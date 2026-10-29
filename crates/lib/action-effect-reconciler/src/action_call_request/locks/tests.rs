use std::sync::Arc;
use std::time::{Duration, Instant};

use waymark_action_effect_reconciler_backend::ActionCallRequestLockId;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::{
    ActionCallRequestLockRenewal, ActionCallRequestLockRenewalStatus,
};
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::{PendingCallRequest, Tracker};
use crate::test_support::TestKey;

const TIME_TO_LIVE: Duration = Duration::from_secs(60);

fn lock_id(promise: usize, nonce: u64) -> ActionCallRequestLockId<u64> {
    ActionCallRequestLockId {
        request: TestKey {
            vm_id: 42,
            promise_state_id: PromiseStateId(promise),
        },
        nonce,
    }
}

fn shared() -> Arc<std::sync::Mutex<Tracker<u64>>> {
    Arc::new(std::sync::Mutex::new(Tracker::new()))
}

/// Track a lock on `promise` under `nonce`, taken now.
fn track_lock(
    tracker: &Arc<std::sync::Mutex<Tracker<u64>>>,
    promise: usize,
    nonce: u64,
) -> PendingCallRequest<u64> {
    Tracker::track_lock(
        tracker,
        lock_id(promise, nonce),
        Instant::now() + TIME_TO_LIVE,
    )
}

fn pending_lock_ids(tracker: &Tracker<u64>) -> Vec<ActionCallRequestLockId<u64>> {
    tracker
        .pending_unlocks()
        .map(|pending| pending.lock_ids().iter().copied().collect())
        .unwrap_or_default()
}

#[test]
fn a_lock_is_tracked_from_the_moment_it_is_taken() {
    let shared = shared();
    let pending = track_lock(&shared, 1, 1);

    let tracker = shared.lock().unwrap();
    assert!(tracker.is_tracking(&lock_id(1, 1)));
    assert_eq!(tracker.tracked_lock_ids(), vec![lock_id(1, 1)]);
    assert!(tracker.pending_unlocks().is_none());
    drop(tracker);

    // Confirming keeps it tracked and nothing is up for unlocking.
    pending.confirm_requested();
    let tracker = shared.lock().unwrap();
    assert!(tracker.is_tracking(&lock_id(1, 1)));
    assert!(tracker.pending_unlocks().is_none());
}

#[test]
fn a_dropped_pending_call_request_untracks_and_unlocks_its_lock() {
    let shared = shared();
    let pending = track_lock(&shared, 1, 1);
    drop(pending);

    let tracker = shared.lock().unwrap();
    assert!(!tracker.is_tracking(&lock_id(1, 1)));
    assert_eq!(pending_lock_ids(&tracker), vec![lock_id(1, 1)]);
}

#[test]
fn an_unconfirmed_unlock_leaves_the_locks_pending() {
    let shared = shared();
    drop(track_lock(&shared, 1, 1));

    let tracker = shared.lock().unwrap();
    let pending = tracker.pending_unlocks().expect("pending");
    drop(pending);

    assert!(tracker.pending_unlocks().is_some());
}

#[test]
fn a_confirmed_unlock_removes_exactly_its_locks() {
    let shared = shared();
    drop(track_lock(&shared, 1, 1));

    let pending = shared.lock().unwrap().pending_unlocks().expect("pending");
    // A second request fails to go through while the unlock is in flight.
    drop(track_lock(&shared, 2, 2));

    let mut tracker = shared.lock().unwrap();
    pending.confirm_unlocked(&mut tracker);
    assert_eq!(pending_lock_ids(&tracker), vec![lock_id(2, 2)]);
}

/// A lock taken again on a request whose earlier lock is still up for
/// unlocking is another lock: the stale unlock neither untracks it nor
/// is confused with it.
#[test]
fn a_fresh_lock_on_a_request_up_for_unlocking_is_another_lock() {
    let shared = shared();
    drop(track_lock(&shared, 1, 1));
    let pending = shared.lock().unwrap().pending_unlocks().expect("pending");

    let fresh = track_lock(&shared, 1, 2);
    {
        let tracker = shared.lock().unwrap();
        assert!(tracker.is_tracking(&lock_id(1, 2)));
        assert!(!tracker.is_tracking(&lock_id(1, 1)));
        assert_eq!(pending_lock_ids(&tracker), vec![lock_id(1, 1)]);
    }

    pending.confirm_unlocked(&mut shared.lock().unwrap());
    {
        let tracker = shared.lock().unwrap();
        assert!(tracker.is_tracking(&lock_id(1, 2)));
        assert!(tracker.pending_unlocks().is_none());
    }
    fresh.confirm_requested();
    assert!(shared.lock().unwrap().is_tracking(&lock_id(1, 2)));
}

/// An earlier lock's pending call request dropped after the request was
/// locked again leaves the fresh lock tracked.
#[test]
fn a_stale_pending_call_request_does_not_untrack_a_fresh_lock() {
    let shared = shared();
    let stale = track_lock(&shared, 1, 1);
    track_lock(&shared, 1, 2).confirm_requested();

    drop(stale);

    let tracker = shared.lock().unwrap();
    assert!(tracker.is_tracking(&lock_id(1, 2)));
    assert!(!tracker.is_tracking(&lock_id(1, 1)));
    assert_eq!(pending_lock_ids(&tracker), vec![lock_id(1, 1)]);
}

#[test]
fn the_fence_is_the_given_deadline_until_renewed() {
    let shared = shared();
    let now = Instant::now();
    Tracker::track_lock(&shared, lock_id(1, 1), now + TIME_TO_LIVE).confirm_requested();

    let mut tracker = shared.lock().unwrap();
    assert_eq!(
        tracker.fence_deadline(&lock_id(1, 1)),
        Some(now + TIME_TO_LIVE)
    );
    assert!(tracker.lock_ids_past_fence(now).is_empty());
    assert_eq!(
        tracker.lock_ids_past_fence(now + TIME_TO_LIVE),
        vec![lock_id(1, 1)]
    );

    // A confirmed renewal pushes the deadline out from its own instant.
    let renewed_at = now + Duration::from_secs(30);
    tracker.apply_renewal(
        ActionCallRequestLockRenewal {
            lock_id: lock_id(1, 1),
            status: ActionCallRequestLockRenewalStatus::Renewed,
        },
        renewed_at,
        TIME_TO_LIVE,
    );
    assert_eq!(
        tracker.fence_deadline(&lock_id(1, 1)),
        Some(renewed_at + TIME_TO_LIVE)
    );
    assert!(tracker.lock_ids_past_fence(now + TIME_TO_LIVE).is_empty());
}

#[test]
fn a_renewal_never_adds_a_lock() {
    let shared = shared();
    let now = Instant::now();
    // The lock was dropped while its renewal was in flight.
    drop(track_lock(&shared, 1, 1));

    let mut tracker = shared.lock().unwrap();
    tracker.apply_renewal(
        ActionCallRequestLockRenewal {
            lock_id: lock_id(1, 1),
            status: ActionCallRequestLockRenewalStatus::Renewed,
        },
        now,
        TIME_TO_LIVE,
    );

    assert!(!tracker.is_tracking(&lock_id(1, 1)));
    assert!(tracker.pending_unlocks().is_some());
}

#[test]
fn renewal_statuses_prune_or_keep() {
    let shared = shared();
    let now = Instant::now();
    for promise in 1..=4 {
        Tracker::track_lock(&shared, lock_id(promise, 1), now + TIME_TO_LIVE).confirm_requested();
    }

    let mut tracker = shared.lock().unwrap();
    let renewal = |promise, status| ActionCallRequestLockRenewal {
        lock_id: lock_id(promise, 1),
        status,
    };
    tracker.apply_renewal(
        renewal(1, ActionCallRequestLockRenewalStatus::Missing),
        now,
        TIME_TO_LIVE,
    );
    tracker.apply_renewal(
        renewal(2, ActionCallRequestLockRenewalStatus::Unconfirmed),
        now,
        TIME_TO_LIVE,
    );
    tracker.apply_renewal(
        renewal(3, ActionCallRequestLockRenewalStatus::HeldElsewhere),
        now,
        TIME_TO_LIVE,
    );
    tracker.apply_renewal(
        renewal(4, ActionCallRequestLockRenewalStatus::Relocked),
        now,
        TIME_TO_LIVE,
    );

    assert!(!tracker.is_tracking(&lock_id(1, 1)));
    assert!(tracker.is_tracking(&lock_id(2, 1)));
    assert!(tracker.is_tracking(&lock_id(3, 1)));
    assert!(tracker.is_tracking(&lock_id(4, 1)));
    // The unconfirmed, held-elsewhere and relocked locks keep their
    // deadlines.
    let mut past_fence = tracker.lock_ids_past_fence(now + TIME_TO_LIVE);
    past_fence.sort_by_key(|lock_id| lock_id.request.promise_state_id);
    assert_eq!(
        past_fence,
        vec![lock_id(2, 1), lock_id(3, 1), lock_id(4, 1)]
    );
}
