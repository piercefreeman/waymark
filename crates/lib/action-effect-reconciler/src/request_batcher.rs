//! Batched durable recording of emitted action-call requests.
//!
//! Every VM's [`EffectHandler`](crate::EffectHandler) submits each emitted
//! request into one shared [`waymark_batcher`] write-batcher and awaits its
//! own outcome, so store-before-deliver still holds per call while many
//! single-row inserts coalesce into one multi-row
//! [`record_action_call_requests`](RecordActionCallRequests::record_action_call_requests)
//! statement.
//!
//! One lock is built per batch, at flush, from a single caller-clock
//! instant — the only honest place for it, since a batch takes one lock
//! set.  Each submitter's local fence base (`taken_at`) is captured before
//! its submission, which predates the flush instant, so the local fence
//! always trips before the stored lock lapses — the same conservative
//! direction as the unbatched path, just with more margin.
//!
//! Retryable (internal) backend failures are retried here, whole-batch,
//! with backoff — safe because the failed statement is atomic, so nothing
//! of the batch landed.  Payload divergence is not retried: it is a
//! data-integrity violation fanned to the whole batch as a fatal
//! [`RecordError::DivergentPayload`], failing the awaiting drive loops.
//! Rows of innocent same-batch VMs were still durably recorded (the
//! backend inserts everything not named in the divergence error), so their
//! calls are redelivered by the revival reconcile once the batch lock
//! lapses.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use nonempty_collections::{NEVec, NonEmptyIterator as _};
use waymark_action_effect_reconciler_backend::record_action_call_requests::{
    Error as _, ErrorKind, RecordingSuccess,
};
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestRecord, RecordActionCallRequests,
};
use waymark_nonzero_duration::NonZeroDuration;

use crate::issuance::fresh_lock;

/// Initial delay between retries of a failed (retryable) batch recording.
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);

/// Cap on the retry delay.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// The per-record outcome of a batched recording.  Non-fatal facts only —
/// anything here means the record's durable fate is decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    /// Freshly inserted, born locked by this process — the caller must
    /// deliver the call and track the lock for renewal.
    Recorded,

    /// The key already existed with an identical payload (a VM replaying a
    /// previously emitted effect) — the caller must not deliver.
    AlreadyRecorded,
}

/// Fatal error for a batched recording — either way the awaiting VM's
/// drive loop cannot continue.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum RecordError {
    /// The key exists with a different payload: replay determinism is
    /// broken.  A data-integrity violation, never retried.
    #[error("a request key already exists with a divergent payload")]
    DivergentPayload,

    /// The request batcher has shut down; the record was never persisted.
    #[error("the request batcher is closed")]
    Closed,
}

/// Handle for submitting emitted requests to the shared request batcher.
pub type RequestRecorderHandle<VmId> = waymark_batcher::BatcherHandle<
    ActionCallRequestRecord<VmId>,
    Result<RecordOutcome, RecordError>,
>;

/// Create the shared request batcher: a recorder handle for the per-VM
/// effect handlers, and the batcher future for the caller to spawn.
///
/// The future resolves once `shutdown` fires (or every handle is dropped)
/// and the last buffered batch has been flushed.
pub fn request_batcher<Backend>(
    backend: Arc<Backend>,
    lock_owner_id: Backend::LockOwnerId,
    lock_time_to_live: NonZeroDuration,
    policy: waymark_batcher::Policy,
    shutdown: impl Future<Output = ()>,
) -> (
    RequestRecorderHandle<Backend::VmId>,
    impl Future<Output = ()>,
)
where
    Backend: RecordActionCallRequests<Timestamp = DateTime<Utc>>,
    Backend::VmId: Clone + Hash + Eq,
    Backend::LockOwnerId: Clone,
{
    waymark_batcher::write_batcher(
        policy,
        move |batch: NEVec<ActionCallRequestRecord<Backend::VmId>>| {
            let backend = Arc::clone(&backend);
            let lock_owner_id = lock_owner_id.clone();
            async move {
                let mut backoff = RETRY_INITIAL_BACKOFF;
                loop {
                    let now = Utc::now();
                    let lock = fresh_lock(now, &lock_owner_id, lock_time_to_live);
                    match backend
                        .record_action_call_requests(now, lock, batch.as_nonempty_slice())
                        .await
                    {
                        Ok(RecordingSuccess::AllRecorded) => {
                            return NEVec::from_elem(Ok(RecordOutcome::Recorded), batch.len());
                        }
                        Ok(RecordingSuccess::SomeAlreadyRecorded(keys)) => {
                            let already: HashSet<ActionCallRequestKey<Backend::VmId>> =
                                keys.into_iter().collect();
                            return batch
                                .nonempty_iter()
                                .map(|record| {
                                    let key = ActionCallRequestKey {
                                        vm_id: record.vm_id.clone(),
                                        promise_state_id: record.promise_state_id,
                                    };
                                    if already.contains(&key) {
                                        Ok(RecordOutcome::AlreadyRecorded)
                                    } else {
                                        Ok(RecordOutcome::Recorded)
                                    }
                                })
                                .collect();
                        }
                        Err(error) => match error.kind() {
                            ErrorKind::Internal => {
                                tracing::warn!(
                                    ?error,
                                    ?backoff,
                                    "recording a request batch failed; retrying"
                                );
                                tokio::time::sleep(backoff).await;
                                backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
                            }
                            ErrorKind::DivergentPayload => {
                                tracing::error!(?error, "divergent request payload in a batch");
                                return NEVec::from_elem(
                                    Err(RecordError::DivergentPayload),
                                    batch.len(),
                                );
                            }
                        },
                    }
                }
            }
        },
        shutdown,
    )
}

#[cfg(test)]
mod tests;
