//! What this process knows about the action-call request locks it holds.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nonempty_collections::{NESlice, NEVec};
use waymark_action_effect_reconciler_backend::ActionCallRequestLockId;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::{
    ActionCallRequestLockRenewal, ActionCallRequestLockRenewalStatus,
};

/// The locks this process holds, each by its id: the tracked ones, which
/// the renewal keeps alive, and the ones to unlock, whose requests never
/// went through.
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
    /// Lock → fence deadline on the local monotonic clock.
    tracked: HashMap<ActionCallRequestLockId<VmId>, Instant>,

    /// The locks of the requests that never went through, to unlock.
    unlock_pending: Vec<ActionCallRequestLockId<VmId>>,
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
    /// Track the lock just taken: it is renewed from here on.
    ///
    /// `fence_deadline` is the instant, on the local monotonic clock, past
    /// which the lock can no longer be vouched for: the instant the
    /// lock-taking call was sent plus the lock's time-to-live.  The
    /// returned [`PendingCallRequest`] stands for the call not yet
    /// requested; confirm it once it is, or drop it to have the lock
    /// unlocked.
    pub fn track_lock(
        this: &Arc<std::sync::Mutex<Self>>,
        lock_id: ActionCallRequestLockId<VmId>,
        fence_deadline: Instant,
    ) -> PendingCallRequest<VmId> {
        this.lock()
            .expect("action-call request locks tracker poisoned")
            .tracked
            .insert(lock_id.clone(), fence_deadline);

        PendingCallRequest {
            lock_id: Some(lock_id),
            tracker: Arc::clone(this),
        }
    }

    /// Whether the lock is tracked: taken by this process and kept
    /// renewed.
    pub fn is_tracking(&self, lock_id: &ActionCallRequestLockId<VmId>) -> bool {
        self.tracked.contains_key(lock_id)
    }

    /// The tracked locks, to renew.
    pub fn tracked_lock_ids(&self) -> Vec<ActionCallRequestLockId<VmId>> {
        self.tracked.keys().cloned().collect()
    }

    /// The fence deadline of the lock, while tracked.
    pub fn fence_deadline(&self, lock_id: &ActionCallRequestLockId<VmId>) -> Option<Instant> {
        self.tracked.get(lock_id).copied()
    }

    /// The tracked locks whose fence deadline has passed at `now`.
    pub fn lock_ids_past_fence(&self, now: Instant) -> Vec<ActionCallRequestLockId<VmId>> {
        self.tracked
            .iter()
            .filter(|(_, fence_deadline)| **fence_deadline <= now)
            .map(|(lock_id, _)| lock_id.clone())
            .collect()
    }

    /// The locks of the requests that never went through, as the first
    /// phase of their unlock; `None` when there are none.
    pub fn pending_unlocks(&self) -> Option<PendingUnlocks<VmId>> {
        let lock_ids = NEVec::try_from_vec(self.unlock_pending.clone())?;

        Some(PendingUnlocks { lock_ids })
    }

    /// Apply one renewal status to a tracked lock.
    ///
    /// `now` is the renewal's pre-send instant, the base of the renewed
    /// fence deadline.  A renewal never adds a lock: one dropped while
    /// the renewal call was in flight stays gone.  A lock relocked or held
    /// elsewhere is left as it is: those are the renewal loop's errors,
    /// not states of the tracker.
    pub fn apply_renewal(
        &mut self,
        renewal: ActionCallRequestLockRenewal<VmId>,
        now: Instant,
        time_to_live: Duration,
    ) {
        let ActionCallRequestLockRenewal { lock_id, status } = renewal;
        match status {
            ActionCallRequestLockRenewalStatus::Renewed => {
                if let Some(fence_deadline) = self.tracked.get_mut(&lock_id) {
                    *fence_deadline = now + time_to_live;
                }
            }
            ActionCallRequestLockRenewalStatus::Missing => {
                self.tracked.remove(&lock_id);
            }
            ActionCallRequestLockRenewalStatus::Relocked
            | ActionCallRequestLockRenewalStatus::HeldElsewhere
            | ActionCallRequestLockRenewalStatus::Unconfirmed => {}
        }
    }
}

impl<VmId> Tracker<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    /// The request of the lock never went through: the lock leaves the
    /// tracked set and is to be unlocked.
    fn request_never_went_through(&mut self, lock_id: ActionCallRequestLockId<VmId>) {
        self.tracked.remove(&lock_id);
        self.unlock_pending.push(lock_id);
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
    // On the type: `Drop` untracks the lock, and a `Drop` impl can carry
    // no bounds the type does not.
    VmId: Eq + std::hash::Hash,
{
    /// Taken by `confirm_requested`; still here when dropped unconfirmed.
    lock_id: Option<ActionCallRequestLockId<VmId>>,

    tracker: Arc<std::sync::Mutex<Tracker<VmId>>>,
}

impl<VmId> PendingCallRequest<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    /// The call was requested: the lock stays tracked.
    pub fn confirm_requested(self) {
        let mut this = self;
        this.lock_id = None;
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
            .field("lock_id", &self.lock_id)
            .finish_non_exhaustive()
    }
}

impl<VmId> Drop for PendingCallRequest<VmId>
where
    VmId: Eq + std::hash::Hash,
{
    fn drop(&mut self) {
        let Some(lock_id) = self.lock_id.take() else {
            return;
        };

        // A poisoned tracker is left alone: the lock then lapses by its
        // time-to-live.
        let Ok(mut tracker) = self.tracker.lock() else {
            return;
        };
        tracker.request_never_went_through(lock_id);
    }
}

/// The two-phase unlock of the locks whose requests never went through:
/// the locks to hand to the backend, then
/// [`confirm_unlocked`](PendingUnlocks::confirm_unlocked) once it did.
/// Dropped instead, the tracker is unchanged and the next heartbeat
/// finds the same locks pending.
#[derive(Debug)]
pub struct PendingUnlocks<VmId> {
    lock_ids: NEVec<ActionCallRequestLockId<VmId>>,
}

impl<VmId> PendingUnlocks<VmId>
where
    VmId: Clone + Eq + std::hash::Hash,
{
    /// The locks to unlock.
    pub fn lock_ids(&self) -> NESlice<'_, ActionCallRequestLockId<VmId>> {
        self.lock_ids.as_nonempty_slice()
    }

    /// The backend unlocked the locks: they leave the unlock list.  Locks
    /// added to the list since stay.
    pub fn confirm_unlocked(self, tracker: &mut Tracker<VmId>) {
        tracker
            .unlock_pending
            .retain(|pending| !self.lock_ids.contains(pending));
    }
}
