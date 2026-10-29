//! The lock renewal heartbeat — per-process background plumbing.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use nonempty_collections::NEVec;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::ActionCallRequestLockRenewalStatus;
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestLockId, HasLockOwnerId, HasTimestamp, HasVmId, RenewActionCallRequestLocks,
    UnlockActionCallRequests,
};
use waymark_nonzero_duration::NonZeroDuration;

use crate::action_call_request::locks::Tracker;

/// Error returned when [`run`] stops with locks it can no longer vouch
/// for.
///
/// A held lock is the authorization to be executing its attempt.  The loop
/// has no per-attempt termination primitive: on a fence breach or a lock
/// taken elsewhere, the attempt keeps running in the local pool
/// unauthorized, and this error is the only signal that it does. The
/// variant docs below describe that, the tick path. The exception is the
/// shutdown path: under [`run`]'s contract for `shutdown`, no attempt runs
/// by then unless the stop is forced, so a fence breach or a lock taken
/// elsewhere found by the last heartbeat, and [`Abandoned`], signal a
/// running attempt only on a forced stop.
///
/// [`Abandoned`]: Error::Abandoned
#[derive(Debug, thiserror::Error)]
pub enum Error<VmId> {
    /// These locks passed their local fence deadline without a confirmed
    /// renewal — the attempts can no longer be authorized.
    #[error("lock fence breached — locks expired without renewal: {0:?}")]
    FenceBreached(NEVec<ActionCallRequestLockId<VmId>>),

    /// These locks are held by another owner while our attempts still
    /// run — authorization is definitively lost.
    #[error("locks taken by another owner: {0:?}")]
    HeldElsewhere(NEVec<ActionCallRequestLockId<VmId>>),

    /// The rows of these locks hold other locks of this process while the
    /// attempts these authorized still run — the authorization is gone,
    /// and the fence should have said so first.
    #[error("locks replaced by other locks of this process: {0:?}")]
    Relocked(NEVec<ActionCallRequestLockId<VmId>>),

    /// The shutdown future resolved while these locks were still tracked,
    /// not confirmed gone; they lapse by their time-to-live and are
    /// redelivered, unless their row is already gone.
    #[error("stopped with locks not confirmed gone, left to lapse by their time-to-live: {0:?}")]
    Abandoned(NEVec<ActionCallRequestLockId<VmId>>),
}

/// Parameters for [`run`].
pub struct Params<Backend>
where
    Backend: HasVmId + HasLockOwnerId,
{
    /// The durable requests backend.
    pub backend: Arc<Backend>,

    /// The identity of this process as a lock owner.
    pub lock_owner_id: Backend::LockOwnerId,

    /// How long a renewed lock lasts before it needs to be renewed again.
    pub lock_time_to_live: NonZeroDuration,

    /// How often to renew the held locks.
    pub heartbeat: NonZeroDuration,

    /// The tracker of this process's action-call request locks: the
    /// locks to renew, and the requests that never went through, to
    /// unlock.
    pub action_call_request_locks_tracker: Arc<std::sync::Mutex<Tracker<Backend::VmId>>>,
}

/// Run the lock renewal heartbeat.
///
/// A held lock is the authorization to be executing its attempt: while
/// every tracked lock renews in time, the local pool's attempts are
/// authorized.  Each key carries a fence deadline on the local monotonic
/// clock (pre-send instant + time-to-live, conservative with respect to
/// the database-authoritative expiry); a confirmed renewal pushes the
/// deadline out, and a deadline passing without one is a fence breach —
/// [`Error::FenceBreached`] — because the attempt keeps running in the
/// local pool and there is no per-attempt termination primitive.  The loop
/// stops with it, the attempts it authorized still running, unless it came
/// from the last heartbeat on a shutdown that was not forced.
///
/// Tracked locks leave peacefully only via
/// [`ActionCallRequestLockRenewalStatus::Missing`] — the row is gone
/// because its completion was durably recorded (or the VM was purged).
/// [`ActionCallRequestLockRenewalStatus::HeldElsewhere`] is a breach
/// ([`Error::HeldElsewhere`]): the backend reports it only from a verified
/// current read, and under an intact fence another owner cannot take an
/// unexpired lock while our attempt is still running.
/// [`ActionCallRequestLockRenewalStatus::Relocked`] is a breach of the
/// same kind ([`Error::Relocked`]): the row holds another lock of this
/// process, which under an intact fence it cannot either — the lock would
/// have had to lapse first.
/// [`ActionCallRequestLockRenewalStatus::Unconfirmed`] keeps the lock
/// tracked with its existing fence deadline — the next heartbeat retries
/// the extension.
///
/// Each heartbeat first unlocks the requests that never went through:
/// their locks authorize no attempt, and unlocked, their rows are up for
/// adoption at once instead of at their time-to-live.  A failed unlock
/// call leaves them pending for the next heartbeat.
///
/// Renewal call failures are logged and retried at the next heartbeat —
/// they only matter once they push a lock to its fence (keep the
/// heartbeat well under the time-to-live). The last heartbeat on
/// shutdown, below, has no next one.
///
/// The loop stops once `shutdown` resolves. Resolve it only once no
/// attempt the tracked locks authorize runs anymore, or on a forced stop:
/// the locks still tracked then lapse by their time-to-live. With nothing
/// tracked it returns `Ok(())` at once. With locks still tracked it first
/// runs one last heartbeat, which can breach or lose a lock as above, and
/// then returns [`Error::Abandoned`] listing the locks still tracked after
/// it, or `Ok(())` when that heartbeat reported every one gone. When that
/// renewal call fails, every tracked lock is listed.
pub async fn run<Backend, Shutdown>(
    params: Params<Backend>,
    shutdown: Shutdown,
) -> Result<(), Error<Backend::VmId>>
where
    Backend: HasVmId + HasLockOwnerId + HasTimestamp<Timestamp = DateTime<Utc>>,
    Backend: RenewActionCallRequestLocks + UnlockActionCallRequests + Send + Sync,
    Backend::VmId: Clone + Eq + std::hash::Hash + Send + Sync + core::fmt::Debug,
    Backend::LockOwnerId: Clone + Send + Sync,
    Shutdown: Future<Output = ()>,
{
    let Params {
        backend,
        lock_owner_id,
        lock_time_to_live,
        heartbeat,
        action_call_request_locks_tracker: tracker,
    } = params;

    let mut interval = tokio::time::interval(heartbeat.get());
    // A heartbeat held up by a slow renewal is not made up for in a burst.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            // FIXME(#725): this arm is a stand-in. What should stop an
            // action-call request lock's renewal is the end of the
            // action-call request fulfillment attempt it authorizes, that
            // is the one delivery of the action call to the local worker
            // pool: while the local worker pool processes the action
            // call, the request lock is renewed; once it no longer does,
            // whether the action-call completion was recorded or the
            // processing ended without one, the request lock should be
            // released at once and dropped from the renewal's tracked
            // set. The proper mechanism is the pending call request
            // travelling on with the action call's outcome to the action
            // completions writer and releasing the request lock and its
            // renewal tracking on drop, the workload pinning shape; today
            // it lives only until the call is requested. Until then,
            // `shutdown` stands for the local worker pool's shutdown: no
            // action-call request fulfillment attempt runs there anymore
            // unless the stop is forced, and the request locks still held
            // lapse by their time-to-live instead of being released. They
            // are renewed once more first, so the `Abandoned` list holds
            // the locks still tracked after it; when that renewal call
            // fails, every tracked lock is listed.
            () = &mut shutdown => {
                if tracker.lock().expect("action-call request locks tracker poisoned").tracked_lock_ids().is_empty() {
                    tracing::info!("shutdown with no locks held; stopping");
                    return Ok(());
                }
                heartbeat_once(&*backend, &lock_owner_id, lock_time_to_live, &tracker).await?;
                let still_tracked = tracker.lock().expect("action-call request locks tracker poisoned").tracked_lock_ids();
                return match NEVec::try_from_vec(still_tracked) {
                    None => {
                        tracing::info!("shutdown with every lock accounted for; stopping");
                        Ok(())
                    }
                    Some(keys) => Err(Error::Abandoned(keys)),
                };
            }
            _ = interval.tick() => {
                // A failed unlock or renewal call and an unconfirmed
                // renewal are retried here, at the next heartbeat.
                heartbeat_once(&*backend, &lock_owner_id, lock_time_to_live, &tracker).await?;
            }
        }
    }
}

/// One heartbeat: the unlock of the requests that never went through,
/// then, over a non-empty tracked set, the fence check and one renewal
/// call, each reported status applied to the set.  A failed backend call
/// is logged and leaves the tracker as it was.
async fn heartbeat_once<Backend>(
    backend: &Backend,
    lock_owner_id: &Backend::LockOwnerId,
    lock_time_to_live: NonZeroDuration,
    tracker: &std::sync::Mutex<Tracker<Backend::VmId>>,
) -> Result<(), Error<Backend::VmId>>
where
    Backend: HasVmId + HasLockOwnerId + HasTimestamp<Timestamp = DateTime<Utc>>,
    Backend: RenewActionCallRequestLocks + UnlockActionCallRequests + Send + Sync,
    Backend::VmId: Clone + Eq + std::hash::Hash + Send + Sync + core::fmt::Debug,
    Backend::LockOwnerId: Clone + Send + Sync,
{
    let time_to_live = lock_time_to_live.get();

    let pending_unlocks = tracker
        .lock()
        .expect("action-call request locks tracker poisoned")
        .pending_unlocks();
    if let Some(pending_unlocks) = pending_unlocks {
        let unlocked = backend
            .unlock_action_call_requests(lock_owner_id, pending_unlocks.lock_ids())
            .await;
        match unlocked {
            Ok(()) => {
                pending_unlocks.confirm_unlocked(
                    &mut tracker
                        .lock()
                        .expect("action-call request locks tracker poisoned"),
                );
            }
            Err(error) => {
                tracing::warn!(?error, "unlocking requests that never went through failed");
            }
        }
    }

    // One instant per heartbeat: the fence-check point, and — being no
    // later than the renewal send — the conservative base for the renewed
    // deadlines.
    let now = Instant::now();

    let lock_ids = {
        let tracker = tracker
            .lock()
            .expect("action-call request locks tracker poisoned");

        // Fence check first: a deadline passing without a confirmed
        // renewal means the attempt can no longer be authorized.
        if let Some(lock_ids) = NEVec::try_from_vec(tracker.lock_ids_past_fence(now)) {
            return Err(Error::FenceBreached(lock_ids));
        }

        tracker.tracked_lock_ids()
    };
    let Some(lock_ids) = NEVec::try_from_vec(lock_ids) else {
        return Ok(());
    };

    // One wall-clock instant for both the lock expiry and the store-clock
    // baseline, so the store reconstructs the intended time-to-live
    // exactly.  (Distinct from `now` above: the monotonic fence clock.)
    let wall_now = Utc::now();
    let expires_at = wall_now
        + chrono::Duration::from_std(time_to_live)
            .expect("the lock time-to-live fits the chrono duration range");
    let renewals = match backend
        .renew_action_call_request_locks(
            wall_now,
            lock_owner_id,
            expires_at,
            lock_ids.as_nonempty_slice(),
        )
        .await
    {
        Ok(renewals) => renewals,
        Err(error) => {
            tracing::warn!(?error, "renewing request locks failed");
            return Ok(());
        }
    };

    let mut held_elsewhere = Vec::new();
    let mut relocked = Vec::new();
    let mut tracker = tracker
        .lock()
        .expect("action-call request locks tracker poisoned");
    for renewal in renewals {
        match renewal.status {
            ActionCallRequestLockRenewalStatus::Missing => {
                // The completion was durably recorded (the store removed
                // the row), or the VM was purged.
                tracing::debug!(lock_id = ?renewal.lock_id, "request row gone; untracking");
            }
            ActionCallRequestLockRenewalStatus::Relocked => {
                relocked.push(renewal.lock_id.clone());
            }
            ActionCallRequestLockRenewalStatus::HeldElsewhere => {
                held_elsewhere.push(renewal.lock_id.clone());
            }
            ActionCallRequestLockRenewalStatus::Unconfirmed => {
                // Still ours, but this pass could not confirm the
                // extension — the existing fence deadline stands.
                tracing::debug!(lock_id = ?renewal.lock_id, "lock renewal unconfirmed");
            }
            ActionCallRequestLockRenewalStatus::Renewed => {}
        }
        tracker.apply_renewal(renewal, now, time_to_live);
    }
    if let Some(lock_ids) = NEVec::try_from_vec(relocked) {
        return Err(Error::Relocked(lock_ids));
    }
    if let Some(lock_ids) = NEVec::try_from_vec(held_elsewhere) {
        return Err(Error::HeldElsewhere(lock_ids));
    }

    Ok(())
}
