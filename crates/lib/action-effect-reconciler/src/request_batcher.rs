//! Batched durable recording of emitted action-call requests.
//!
//! Every VM's [`EffectHandler`](crate::EffectHandler) submits each emitted
//! request into one shared [`waymark_batcher`] write-batcher and awaits its
//! own outcome, so store-before-deliver still holds per call while many
//! single-row inserts coalesce into one multi-row
//! [`record_action_call_requests`](RecordActionCallRequests::record_action_call_requests)
//! statement.
//!
//! One lock is built per flush attempt, from a single caller-clock
//! instant — the only honest place for it, since a batch takes one lock
//! set.  Each recorded row is tracked here too, in the flush, under its
//! [`PendingCallRequest`](crate::action_call_request_locks::PendingCallRequest),
//! so every lock the statement reports taken is tracked; its fence
//! deadline is based on the instant just before the attempt's statement
//! is sent: pre-send, so the local fence still trips before the stored
//! lock lapses, and per-attempt, so its staleness is bounded by one
//! attempt — a delayed flush (queue wait, retries) cannot deliver a lock
//! whose fence is already dead.
//!
//! Retryable (internal) backend failures are retried here, whole-batch,
//! with backoff.  A statement that committed but reported failure leaves
//! its rows locked under a lock nobody tracks; the retry finds them
//! already recorded, and the effect handler requests a
//! [`recheck`](crate::adopting_action_settler::recheck) so the VM's
//! adopter picks the rows up once their locks lapse.  Payload divergence
//! is not retried: it is a data-integrity violation fanned to the whole
//! batch as a fatal [`RecordError::DivergentPayload`], failing the
//! awaiting drive loops.
//! Rows of innocent same-batch VMs were still durably recorded (the
//! backend inserts everything the divergence error does not refer to),
//! so their calls are redelivered by adoption once the batch lock lapses.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use nonempty_collections::{NEVec, NonEmptyIterator as _};
use waymark_action_effect_reconciler_backend::record_action_call_requests::{
    Error as _, ErrorKind, RecordingSuccess,
};
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestLockId, ActionCallRequestRecord,
    RecordActionCallRequests,
};
use waymark_nonzero_duration::NonZeroDuration;

use crate::action_call_request_locks::Tracker;
use crate::issuance::fresh_lock;

/// Initial delay between retries of a failed (retryable) batch recording.
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);

/// Cap on the retry delay.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// The per-record outcome of a batched recording.  Non-fatal facts only —
/// anything here means the record's durable fate is decided.
#[derive(Debug)]
pub enum RecordOutcome<VmId>
where
    VmId: Eq + Hash,
{
    /// Freshly inserted, born locked by this process, the lock tracked —
    /// the caller must deliver the call and confirm it.
    Recorded {
        /// The call not yet requested; confirm it once it is.
        pending_call_request: crate::action_call_request_locks::PendingCallRequest<VmId>,
    },

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
    Result<RecordOutcome<VmId>, RecordError>,
>;

/// Create the shared request batcher: a recorder handle for the per-VM
/// effect handlers, and the batcher future for the caller to spawn.
///
/// `tracker` is the tracker of action-call request locks the taken locks
/// go into.
///
/// The future resolves once `shutdown` fires (or every handle is dropped)
/// and the last buffered batch has been flushed.
pub fn request_batcher<Backend>(
    backend: Arc<Backend>,
    lock_owner_id: Backend::LockOwnerId,
    lock_time_to_live: NonZeroDuration,
    tracker: Arc<std::sync::Mutex<Tracker<Backend::VmId>>>,
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
            let tracker = Arc::clone(&tracker);
            async move {
                let mut backoff = RETRY_INITIAL_BACKOFF;
                loop {
                    let taken_at = Instant::now();
                    let now = Utc::now();
                    // One nonce per attempt: the batch takes one lock set,
                    // and a later lock on any of these rows draws its own.
                    let lock_nonce = rand::random::<u64>();
                    let lock = fresh_lock(now, &lock_owner_id, lock_time_to_live, lock_nonce);
                    let recorded = |record: &ActionCallRequestRecord<Backend::VmId>| {
                        let lock_id = ActionCallRequestLockId {
                            request: ActionCallRequestKey {
                                vm_id: record.vm_id.clone(),
                                promise_state_id: record.promise_state_id,
                            },
                            nonce: lock_nonce,
                        };
                        RecordOutcome::Recorded {
                            pending_call_request: Tracker::track_lock(
                                &tracker,
                                lock_id,
                                taken_at + lock_time_to_live.get(),
                            ),
                        }
                    };
                    match backend
                        .record_action_call_requests(now, lock, batch.as_nonempty_slice())
                        .await
                    {
                        Ok(RecordingSuccess::AllRecorded) => {
                            return batch
                                .nonempty_iter()
                                .map(|record| Ok(recorded(record)))
                                .collect();
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
                                        Ok(recorded(record))
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
                                return batch
                                    .nonempty_iter()
                                    .map(|_| Err(RecordError::DivergentPayload))
                                    .collect();
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
