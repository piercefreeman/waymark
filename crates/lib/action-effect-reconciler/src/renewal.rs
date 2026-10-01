//! The lock renewal heartbeat — per-process background plumbing.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use nonempty_collections::NEVec;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::{
    RenewalStatus, RequestLockRenewal,
};
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, HasLockOwnerId, HasTimestamp, HasVmId, RenewActionCallRequestLocks,
};
use waymark_nonzero_duration::NonZeroDuration;

use crate::issuance::fresh_lock;

/// A held request lock to keep renewed: the delivered call's key and the
/// instant captured **before** the lock-taking call was sent.
///
/// The pre-send instant makes the local fence deadline (`taken_at` + the
/// time-to-live, on the monotonic clock) conservative with respect to the
/// database-authoritative expiry.
#[derive(Debug, Clone, Copy)]
pub struct HeldLock<VmId> {
    /// The request whose lock this process holds.
    pub key: ActionCallRequestKey<VmId>,

    /// When the lock-taking call was sent, on the local monotonic clock.
    pub taken_at: Instant,
}

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
    FenceBreached(NEVec<ActionCallRequestKey<VmId>>),

    /// These locks are held by another owner while our attempts still
    /// run — authorization is definitively lost.
    #[error("locks taken by another owner: {0:?}")]
    HeldElsewhere(NEVec<ActionCallRequestKey<VmId>>),

    /// The shutdown future resolved while these locks were still tracked,
    /// not confirmed gone; they lapse by their time-to-live and are
    /// redelivered, unless their row is already gone.
    #[error("stopped with locks not confirmed gone, left to lapse by their time-to-live: {0:?}")]
    Abandoned(NEVec<ActionCallRequestKey<VmId>>),
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

    /// Locks taken by the issuance paths for calls delivered to the
    /// local pool.
    pub held_locks_rx: tokio::sync::mpsc::UnboundedReceiver<HeldLock<Backend::VmId>>,
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
/// [`RenewalStatus::Missing`] — the row is gone because its completion
/// was durably recorded (or the VM was purged).  [`RenewalStatus::HeldElsewhere`]
/// is a breach ([`Error::HeldElsewhere`]): the backend reports it only
/// from a verified current read, and under an intact fence another owner
/// cannot take an unexpired lock while our attempt is still running.
/// [`RenewalStatus::Unconfirmed`] keeps the lock tracked with its
/// existing fence deadline — the next heartbeat retries the extension.
///
/// Renewal call failures are logged and retried at the next heartbeat —
/// they only matter once they push a lock to its fence (keep the
/// heartbeat well under the time-to-live). The last heartbeat on
/// shutdown, below, has no next one.
///
/// Returns `Ok(())` once the channel is closed and every tracked lock has
/// been reported gone — the natural graceful-shutdown drain.
///
/// The loop also stops once `shutdown` resolves. Resolve it only once no
/// attempt the tracked locks authorize runs anymore, or on a forced stop:
/// the locks still tracked then lapse by their time-to-live. With nothing
/// tracked it returns `Ok(())` at once. With locks still tracked it first
/// runs one last heartbeat, which can breach or lose a lock as above, and
/// then returns [`Error::Abandoned`] naming the locks still tracked after
/// it, or `Ok(())` when that heartbeat reported every one gone. When that
/// renewal call fails, every tracked lock is named.
pub async fn run<Backend, Shutdown>(
    params: Params<Backend>,
    shutdown: Shutdown,
) -> Result<(), Error<Backend::VmId>>
where
    Backend: HasVmId + HasLockOwnerId + HasTimestamp<Timestamp = DateTime<Utc>>,
    Backend: RenewActionCallRequestLocks + Send + Sync,
    Backend::VmId: Copy + Eq + std::hash::Hash + Send + Sync + core::fmt::Debug,
    Backend::LockOwnerId: Clone + Send + Sync,
    Shutdown: Future<Output = ()>,
{
    let Params {
        backend,
        lock_owner_id,
        lock_time_to_live,
        heartbeat,
        mut held_locks_rx,
    } = params;

    let time_to_live = lock_time_to_live.get();

    let mut interval = tokio::time::interval(heartbeat.get());
    // A heartbeat held up by a slow renewal is not made up for in a burst.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Tracked locks: key → fence deadline on the local monotonic clock.
    let mut tracked: HashMap<ActionCallRequestKey<Backend::VmId>, Instant> = HashMap::new();
    let mut channel_closed = false;
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        // One stop for both the channel closing and the heartbeat that
        // empties the tracked set, checked before every wait so neither
        // waits for the next tick.
        if channel_closed && tracked.is_empty() {
            tracing::info!("all locks accounted for and channel closed; stopping");
            return Ok(());
        }

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
            // set. The proper mechanism is a fulfillment token travelling
            // with the action call's outcome to the action completions
            // writer and releasing the request lock and its renewal
            // tracking on drop, the workload pinning shape. Until then,
            // `shutdown` stands for the local worker pool's shutdown: no
            // action-call request fulfillment attempt runs there anymore
            // unless the stop is forced, and the request locks still held
            // lapse by their time-to-live instead of being released. They
            // are renewed once more first, so the `Abandoned` list names the
            // locks still tracked after it; when that renewal call fails, every
            // tracked lock is named. Locks queued in the channel at the stop
            // are not read.
            () = &mut shutdown => {
                if tracked.is_empty() {
                    tracing::info!("shutdown with no locks held; stopping");
                    return Ok(());
                }
                heartbeat_once(&*backend, &lock_owner_id, lock_time_to_live, &mut tracked).await?;
                return match NEVec::try_from_vec(tracked.into_keys().collect()) {
                    None => {
                        tracing::info!("shutdown with every lock accounted for; stopping");
                        Ok(())
                    }
                    Some(keys) => Err(Error::Abandoned(keys)),
                };
            }
            held_lock = held_locks_rx.recv(), if !channel_closed => {
                match held_lock {
                    Some(HeldLock { key, taken_at }) => {
                        tracked.insert(key, taken_at + time_to_live);
                    }
                    None => {
                        channel_closed = true;
                    }
                }
            }
            _ = interval.tick() => {
                if tracked.is_empty() {
                    continue;
                }

                // A failed renewal call and an unconfirmed renewal are
                // retried here, at the next heartbeat.
                heartbeat_once(&*backend, &lock_owner_id, lock_time_to_live, &mut tracked).await?;
            }
        }
    }
}

/// One heartbeat over the non-empty tracked set: the fence check, then
/// one renewal call, each reported status applied to the set. A failed
/// renewal call is logged and leaves the set as it was.
async fn heartbeat_once<Backend>(
    backend: &Backend,
    lock_owner_id: &Backend::LockOwnerId,
    lock_time_to_live: NonZeroDuration,
    tracked: &mut HashMap<ActionCallRequestKey<Backend::VmId>, Instant>,
) -> Result<(), Error<Backend::VmId>>
where
    Backend: HasVmId + HasLockOwnerId + HasTimestamp<Timestamp = DateTime<Utc>>,
    Backend: RenewActionCallRequestLocks + Send + Sync,
    Backend::VmId: Copy + Eq + std::hash::Hash + Send + Sync + core::fmt::Debug,
    Backend::LockOwnerId: Clone + Send + Sync,
{
    let time_to_live = lock_time_to_live.get();

    // One instant per heartbeat: the fence-check point, and — being no
    // later than the renewal send — the conservative base for the renewed
    // deadlines.
    let now = Instant::now();

    // Fence check first: a deadline passing without a confirmed renewal
    // means the attempt can no longer be authorized.
    let breached: Vec<_> = tracked
        .iter()
        .filter(|(_, fence_deadline)| **fence_deadline <= now)
        .map(|(key, _)| *key)
        .collect();
    if let Some(keys) = NEVec::try_from_vec(breached) {
        return Err(Error::FenceBreached(keys));
    }

    let keys =
        NEVec::try_from_vec(tracked.keys().copied().collect()).expect("tracked is non-empty");

    // One wall-clock instant for both the lock expiry and the store-clock
    // baseline, so the store reconstructs the intended time-to-live
    // exactly.  (Distinct from `now` above: the monotonic fence clock.)
    let wall_now = Utc::now();
    let lock = fresh_lock(wall_now, lock_owner_id, lock_time_to_live);
    let renewals = match backend
        .renew_action_call_request_locks(wall_now, lock, keys.as_nonempty_slice())
        .await
    {
        Ok(renewals) => renewals,
        Err(error) => {
            tracing::warn!(?error, "renewing request locks failed");
            return Ok(());
        }
    };

    let mut held_elsewhere = Vec::new();
    for renewal in renewals {
        let RequestLockRenewal { key, status } = renewal;
        match status {
            RenewalStatus::Renewed => {
                tracked.insert(key, now + time_to_live);
            }
            RenewalStatus::Missing => {
                // The completion was durably recorded (the store removed
                // the row), or the VM was purged.  Also the future
                // cancellation signal: removed row ⇒ cancel the local
                // attempt.
                tracing::debug!(?key, "request row gone; untracking");
                tracked.remove(&key);
            }
            RenewalStatus::HeldElsewhere => {
                held_elsewhere.push(key);
            }
            RenewalStatus::Unconfirmed => {
                // Still ours, but this pass could not confirm the
                // extension — the existing fence deadline stands.
                tracing::debug!(?key, "lock renewal unconfirmed");
            }
        }
    }
    if let Some(keys) = NEVec::try_from_vec(held_elsewhere) {
        return Err(Error::HeldElsewhere(keys));
    }

    Ok(())
}
