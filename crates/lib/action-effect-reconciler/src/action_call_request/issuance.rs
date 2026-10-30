//! Shared helpers for the two issuance paths (effect emission and
//! adoption) and the renewal heartbeat.

use chrono::{DateTime, Utc};
use waymark_action_effect_reconciler_backend::ActionCallRequestLock;
use waymark_nonzero_duration::NonZeroDuration;

/// A fresh lock for this process under `nonce`, expiring one time-to-live
/// from `now`.
///
/// `now` is the caller-clock instant the expiry is computed against; pass
/// the same instant to the backend call taking the lock, so the store can
/// reconstruct the intended time-to-live exactly.
pub(crate) fn fresh_lock<LockOwnerId: Clone>(
    now: DateTime<Utc>,
    lock_owner_id: &LockOwnerId,
    lock_time_to_live: NonZeroDuration,
    nonce: u64,
) -> ActionCallRequestLock<LockOwnerId, DateTime<Utc>> {
    let time_to_live = chrono::Duration::from_std(lock_time_to_live.get())
        .expect("the lock time-to-live fits the chrono duration range");
    ActionCallRequestLock {
        owner: lock_owner_id.clone(),
        expires_at: now + time_to_live,
        nonce,
    }
}
