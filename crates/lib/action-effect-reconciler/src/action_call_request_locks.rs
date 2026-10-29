//! What this process knows about the action-call request locks it holds.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nonempty_collections::{NESlice, NEVec};
use waymark_action_effect_reconciler_backend::ActionCallRequestKey;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::{
    RenewalStatus, RequestLockRenewal,
};

/// The locks this process holds: the tracked ones, which the renewal
/// keeps alive, and the ones to unlock, whose requests never went
/// through.
///
/// A lock is tracked from the moment the lock statement took it
/// ([`track_lock`](Tracker::track_lock)), under a [`PendingCallRequest`]
/// until its call is requested; one dropped before that moves its lock
/// to the unlock list, which the renewal loop works off in two phases
/// ([`pending_unlocks`](Tracker::pending_unlocks)).  Shared by the
/// issuance paths, the settlers and the renewal loop, wrapped by its
/// owner.
#[derive(Debug)]
pub struct Tracker<VmId> {
    /// Key → fence deadline on the local monotonic clock.
    tracked: HashMap<ActionCallRequestKey<VmId>, Instant>,

    /// The requests that never went through, whose locks are to be
    /// unlocked.
    unlock_pending: Vec<ActionCallRequestKey<VmId>>,
}

impl<VmId> Default for Tracker<VmId> {
    fn default() -> Self {
        Self::new()
    }
}

impl<VmId> Tracker<VmId> {
    /// A tracker holding nothing.
    pub fn new() -> Self {
        Self {
            tracked: HashMap::new(),
            unlock_pending: Vec::new(),
        }
    }
}

impl<VmId> Tracker<VmId>
where
    VmId: Clone + Eq + std::hash::Hash,
{
    /// Track the lock just taken on `key`: it is renewed from here on.
    ///
    /// `fence_deadline` is the instant, on the local monotonic clock, past
    /// which the lock can no longer be vouched for: the instant the
    /// lock-taking call was sent plus the lock's time-to-live.  The
    /// returned [`PendingCallRequest`] stands for the call not yet
    /// requested; confirm it once it is, or drop it to have the lock
    /// unlocked.
    pub fn track_lock(
        this: &Arc<std::sync::Mutex<Self>>,
        key: ActionCallRequestKey<VmId>,
        fence_deadline: Instant,
    ) -> PendingCallRequest<VmId> {
        {
            let mut tracker = this
                .lock()
                .expect("action-call request locks tracker poisoned");
            // A lock taken again on a key whose earlier lock is still up
            // for unlocking: that unlock is moot, the row has a fresh lock.
            tracker.unlock_pending.retain(|pending| *pending != key);
            tracker.tracked.insert(key.clone(), fence_deadline);
        }

        PendingCallRequest {
            key: Some(key),
            tracker: Arc::clone(this),
        }
    }

    /// Whether the lock of `key` is tracked: taken by this process and
    /// kept renewed.
    pub fn is_tracking(&self, key: &ActionCallRequestKey<VmId>) -> bool {
        self.tracked.contains_key(key)
    }

    /// The tracked keys, to renew.
    pub fn tracked_keys(&self) -> Vec<ActionCallRequestKey<VmId>> {
        self.tracked.keys().cloned().collect()
    }

    /// The fence deadline of the lock of `key`, while tracked.
    pub fn fence_deadline(&self, key: &ActionCallRequestKey<VmId>) -> Option<Instant> {
        self.tracked.get(key).copied()
    }

    /// The tracked keys whose fence deadline has passed at `now`.
    pub fn keys_past_fence(&self, now: Instant) -> Vec<ActionCallRequestKey<VmId>> {
        self.tracked
            .iter()
            .filter(|(_, fence_deadline)| **fence_deadline <= now)
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// The requests that never went through, as the first phase of their
    /// unlock; `None` when there are none.
    pub fn pending_unlocks(&self) -> Option<PendingUnlocks<VmId>> {
        let keys = NEVec::try_from_vec(self.unlock_pending.clone())?;

        Some(PendingUnlocks { keys })
    }

    /// Apply one renewal status to a tracked lock.
    ///
    /// `now` is the renewal's pre-send instant, the base of the renewed
    /// fence deadline.  A renewal never adds a lock: one dropped while
    /// the renewal call was in flight stays gone.  A lock held elsewhere
    /// is left as it is: that is the renewal loop's error, not a state of
    /// the tracker.
    pub fn apply_renewal(
        &mut self,
        renewal: RequestLockRenewal<VmId>,
        now: Instant,
        time_to_live: Duration,
    ) {
        let RequestLockRenewal { key, status } = renewal;
        match status {
            RenewalStatus::Renewed => {
                if let Some(fence_deadline) = self.tracked.get_mut(&key) {
                    *fence_deadline = now + time_to_live;
                }
            }
            RenewalStatus::Missing => {
                self.tracked.remove(&key);
            }
            RenewalStatus::HeldElsewhere | RenewalStatus::Unconfirmed => {}
        }
    }
}

impl<VmId> Tracker<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    /// The request of `key` never went through: its lock leaves the
    /// tracked set and is to be unlocked.
    fn request_never_went_through(&mut self, key: ActionCallRequestKey<VmId>) {
        self.tracked.remove(&key);
        self.unlock_pending.push(key);
    }
}

/// The call of a tracked lock, not yet requested.
///
/// Returned by [`Tracker::track_lock`] in the lock-taking statement's
/// task, so that no lock is tracked without one until its call is
/// requested.  [`confirm_requested`](PendingCallRequest::confirm_requested)
/// once it is; dropped before that, it reports the request as never gone
/// through: the lock leaves the tracked set and the renewal loop unlocks
/// the row.
#[must_use = "dropping it reports the request as never gone through; call confirm_requested once the call is requested"]
pub struct PendingCallRequest<VmId>
where
    // On the type: `Drop` untracks the key, and a `Drop` impl can carry
    // no bounds the type does not.
    VmId: Eq + std::hash::Hash,
{
    /// Taken by `confirm_requested`; still here when dropped unconfirmed.
    key: Option<ActionCallRequestKey<VmId>>,

    tracker: Arc<std::sync::Mutex<Tracker<VmId>>>,
}

impl<VmId> PendingCallRequest<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    /// The call was requested: the lock stays tracked.
    pub fn confirm_requested(self) {
        let mut this = self;
        this.key = None;
    }
}

// By hand: the shared tracker is not part of what the value is.
impl<VmId> core::fmt::Debug for PendingCallRequest<VmId>
where
    VmId: Eq + std::hash::Hash + core::fmt::Debug,
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("PendingCallRequest")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl<VmId> Drop for PendingCallRequest<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };

        // A poisoned tracker is left alone: the lock then lapses by its
        // time-to-live.
        let Ok(mut tracker) = self.tracker.lock() else {
            return;
        };
        tracker.request_never_went_through(key);
    }
}

/// The two-phase unlock of the requests that never went through: the
/// keys to hand to the backend, then
/// [`confirm_unlocked`](PendingUnlocks::confirm_unlocked) once it did.
/// Dropped instead, the tracker is unchanged and the next heartbeat
/// finds the same keys pending.
#[derive(Debug)]
pub struct PendingUnlocks<VmId> {
    keys: NEVec<ActionCallRequestKey<VmId>>,
}

impl<VmId> PendingUnlocks<VmId>
where
    VmId: Clone + Eq + std::hash::Hash,
{
    /// The keys to unlock.
    pub fn keys(&self) -> NESlice<'_, ActionCallRequestKey<VmId>> {
        self.keys.as_nonempty_slice()
    }

    /// The backend unlocked the keys: they leave the unlock list.  Keys
    /// added to the list since stay.
    pub fn confirm_unlocked(self, tracker: &mut Tracker<VmId>) {
        tracker
            .unlock_pending
            .retain(|pending| !self.keys.contains(pending));
    }
}
