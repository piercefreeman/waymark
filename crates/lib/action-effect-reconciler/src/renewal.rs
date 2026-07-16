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

/// Error returned when [`run`] stops because the lock fence was breached.
///
/// A held lock is the authorization to be executing its attempt.  Without
/// a per-attempt termination primitive, the only instrument that honors
/// "force-terminate attempts we can no longer authorize" is killing the
/// whole process — drive this loop under a drop guard so a breach
/// escalates to subsystem shutdown, taking the local pool (and every
/// running attempt) with it.
#[derive(Debug, thiserror::Error)]
pub enum Error<VmId> {
    /// These locks passed their local fence deadline without a confirmed
    /// renewal — the attempts can no longer be authorized.
    #[error("lock fence breached — locks expired without renewal: {0:?}")]
    FenceBreached(NEVec<ActionCallRequestKey<VmId>>),

    /// These locks are held by another owner while our attempts still
    /// run — authorization is definitively lost.
    #[error("locks taken by another owner while attempts still run: {0:?}")]
    HeldElsewhere(NEVec<ActionCallRequestKey<VmId>>),
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
/// local pool and there is no per-attempt termination primitive.  Drive
/// this loop under a drop guard: the breach escalates to subsystem
/// shutdown, force-terminating every local attempt with the process.
///
/// Tracked locks leave peacefully only via
/// [`RenewalStatus::Missing`] — the row is gone because its completion
/// was durably recorded (or the VM was purged).  [`RenewalStatus::HeldElsewhere`]
/// is a breach ([`Error::HeldElsewhere`]): under an intact fence another
/// owner cannot take an unexpired lock, and our attempt is still running.
///
/// Renewal call failures are logged and retried at the next heartbeat —
/// they only matter once they push a lock to its fence (keep the
/// heartbeat well under the time-to-live).
///
/// Returns `Ok(())` once the channel is closed and every tracked lock has
/// been reported gone — the natural graceful-shutdown drain.  For a
/// forced stop, abort the task.
pub async fn run<Backend>(params: Params<Backend>) -> Result<(), Error<Backend::VmId>>
where
    Backend: HasVmId + HasLockOwnerId + HasTimestamp<Timestamp = DateTime<Utc>>,
    Backend: RenewActionCallRequestLocks + Send + Sync,
    Backend::VmId: Copy + Eq + std::hash::Hash + Send + Sync + core::fmt::Debug,
    Backend::LockOwnerId: Clone + Send + Sync,
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
    // Tracked locks: key → fence deadline on the local monotonic clock.
    let mut tracked: HashMap<ActionCallRequestKey<Backend::VmId>, Instant> = HashMap::new();
    let mut channel_closed = false;

    loop {
        tokio::select! {
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
                    if channel_closed {
                        tracing::info!("all locks accounted for and channel closed; stopping");
                        return Ok(());
                    }
                    continue;
                }

                // One instant per tick: the fence-check point, and —
                // being no later than the renewal send — the conservative
                // base for the renewed deadlines.
                let now = Instant::now();

                // Fence check first: a deadline passing without a
                // confirmed renewal means the attempt can no longer be
                // authorized.
                let breached: Vec<_> = tracked
                    .iter()
                    .filter(|(_, fence_deadline)| **fence_deadline <= now)
                    .map(|(key, _)| *key)
                    .collect();
                if let Some(keys) = NEVec::try_from_vec(breached) {
                    return Err(Error::FenceBreached(keys));
                }

                let keys = NEVec::try_from_vec(tracked.keys().copied().collect())
                    .expect("tracked is non-empty");

                let lock = fresh_lock(&lock_owner_id, lock_time_to_live);
                let renewals = match backend
                    .renew_action_call_request_locks(lock, keys.as_nonempty_slice())
                    .await
                {
                    Ok(renewals) => renewals,
                    Err(error) => {
                        tracing::warn!(?error, "renewing request locks failed; will retry");
                        continue;
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
                            // The completion was durably recorded (the
                            // store removed the row), or the VM was
                            // purged.  Also the future cancellation
                            // signal: removed row ⇒ cancel the local
                            // attempt.
                            tracing::debug!(?key, "request row gone; untracking");
                            tracked.remove(&key);
                        }
                        RenewalStatus::HeldElsewhere => {
                            held_elsewhere.push(key);
                        }
                    }
                }
                if let Some(keys) = NEVec::try_from_vec(held_elsewhere) {
                    return Err(Error::HeldElsewhere(keys));
                }
            }
        }
    }
}
