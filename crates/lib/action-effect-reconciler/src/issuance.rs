//! Shared helpers for the two issuance paths (effect emission and
//! revival reconcile) and the renewal heartbeat.

use chrono::{DateTime, Utc};
use waymark_action_effect_reconciler_backend::RequestLock;
use waymark_nonzero_duration::NonZeroDuration;

use crate::renewal::HeldLock;

/// A fresh lock for this process, expiring one time-to-live from `now`.
///
/// `now` is the caller-clock instant the expiry is computed against; pass
/// the same instant to the backend call taking the lock, so the store can
/// reconstruct the intended time-to-live exactly.
pub(crate) fn fresh_lock<LockOwnerId: Clone>(
    now: DateTime<Utc>,
    lock_owner_id: &LockOwnerId,
    lock_time_to_live: NonZeroDuration,
) -> RequestLock<LockOwnerId, DateTime<Utc>> {
    let time_to_live = chrono::Duration::from_std(lock_time_to_live.get())
        .expect("the lock time-to-live fits the chrono duration range");
    RequestLock {
        owner: lock_owner_id.clone(),
        expires_at: now + time_to_live,
    }
}

/// Hand a held lock to the renewal loop.
///
/// The renewal loop going away means the subsystem is shutting down; the
/// lock will expire and the call will be redelivered elsewhere, so a
/// closed channel is only worth a warning.
pub(crate) fn track_for_renewal<VmId>(
    held_locks_tx: &tokio::sync::mpsc::UnboundedSender<HeldLock<VmId>>,
    held_lock: HeldLock<VmId>,
) {
    if held_locks_tx.send(held_lock).is_err() {
        tracing::warn!("renewal loop is gone; the delivered call's lock will expire");
    }
}
