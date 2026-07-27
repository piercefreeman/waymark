//! Shared helpers for the two issuance paths (effect emission and
//! revival reconcile) and the renewal heartbeat.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use nonempty_collections::{NEVec, nev};
use waymark_action_effect_reconciler_backend::record_action_call_requests::{
    Error as _, ErrorKind, RecordingSuccess,
};
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestRecord, RecordActionCallRequests, RequestLock,
};
use waymark_nonzero_duration::NonZeroDuration;

use crate::renewal::HeldLock;

/// Initial delay between retries of a failed (retryable) record operation.
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);

/// Cap on the retry delay.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// A fresh lock for this process, expiring one time-to-live from now.
pub(crate) fn fresh_lock<LockOwnerId: Clone>(
    lock_owner_id: &LockOwnerId,
    lock_time_to_live: NonZeroDuration,
) -> RequestLock<LockOwnerId, DateTime<Utc>> {
    let time_to_live = chrono::Duration::from_std(lock_time_to_live.get())
        .expect("the lock time-to-live fits the chrono duration range");
    RequestLock {
        owner: lock_owner_id.clone(),
        expires_at: Utc::now() + time_to_live,
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

/// Record one request born locked, retrying retryable failures
/// indefinitely with a fresh lock per attempt.
///
/// Returns the recording outcome together with the instant captured
/// before the successful attempt was sent — the conservative base for
/// the lock's local fence deadline.  Returns an error only on payload
/// divergence — the one failure that must never be retried.
pub(crate) async fn record_with_retry<Backend>(
    backend: &Backend,
    lock_owner_id: &Backend::LockOwnerId,
    lock_time_to_live: NonZeroDuration,
    record: &ActionCallRequestRecord<Backend::VmId>,
) -> Result<(RecordingSuccess<Backend::VmId>, Instant), Backend::Error>
where
    Backend: RecordActionCallRequests<Timestamp = DateTime<Utc>>,
    Backend::LockOwnerId: Clone,
    Backend::VmId: Clone,
{
    let records: NEVec<_> = nev![record.clone()];
    let mut backoff = RETRY_INITIAL_BACKOFF;
    loop {
        let taken_at = Instant::now();
        let lock = fresh_lock(lock_owner_id, lock_time_to_live);
        match backend
            .record_action_call_requests(lock, records.as_nonempty_slice())
            .await
        {
            Ok(success) => return Ok((success, taken_at)),
            Err(error) => match error.kind() {
                ErrorKind::Internal => {
                    tracing::warn!(?error, ?backoff, "recording a request failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
                }
                ErrorKind::DivergentPayload => {
                    return Err(error);
                }
            },
        }
    }
}
