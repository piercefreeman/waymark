//! Batched locking of VMs' pending action-call requests.
//!
//! Every pass of a VM's [`AdoptingActionSettler`](crate::AdoptingActionSettler)
//! submits the vm id into one shared
//! [`deduplicating_write_batcher`](waymark_batcher::deduplicating_write_batcher)
//! keyed by the vm id and awaits its own [`PerVmLocks`], so many single-VM
//! lock statements coalesce into one multi-VM
//! [`lock_action_call_requests`](LockActionCallRequests::lock_action_call_requests)
//! statement pair.  The trait's per-input-order contract aligns exactly
//! with the batcher's positional fan-out, so outcomes zip straight back
//! to their waiters.
//!
//! A duplicate vm id within one window is reachable: a pass dropped
//! mid-submit strands its item, and the VM's next pass submits the same
//! vm again.  Submission order then guarantees the earlier occurrence's
//! waiter is dead — a VM's passes run one after another, from the one
//! settler of the resident VM — so `LastSubmissionWins` folds it out and
//! only the live submission reaches the statement.
//!
//! One lock is built per batch, at flush.  Each locked row is tracked
//! here too, in the flush, under its
//! [`PendingCallRequest`](crate::action_call_request::locks::PendingCallRequest),
//! so every lock the statement reports taken is tracked; its fence
//! deadline is based on the instant just before the statement is sent:
//! pre-send, so the local fence still trips before the stored lock
//! lapses, and per-flush, so its staleness is bounded by one attempt —
//! queue wait behind a slow flush cannot deliver locks whose fence is
//! already dead.
//!
//! There is deliberately **no retry** here: a failed pass fails the VM's
//! wait, the VM is evicted, and the next pinning cycle revives it.  A
//! backend error therefore fans [`PerVmLockingError::Failed`] to every
//! waiter in the batch; the affected VMs are evicted and re-pinned.

use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use waymark_action_effect_reconciler_backend::lock_action_call_requests::PerVmActionCallRequestLockingOutcome;
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestLockId, ActionCallRequestRecord, LockActionCallRequests,
};
use waymark_nonzero_duration::NonZeroDuration;

use super::issuance::fresh_lock;
use crate::action_call_request::locks::Tracker;

/// Fatal per-VM error for a batched lock — the VM's wait fails and the
/// workload is re-pinned later.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum PerVmLockingError {
    /// The batched lock statement reported failure.  A lock it took anyway
    /// lapses by its time-to-live and is adopted then.
    #[error("locking pending action-call requests failed")]
    Failed,

    /// The lock batcher has shut down; nothing was locked.
    #[error("the lock batcher is closed")]
    Closed,
}

/// One request locked by a pass, its lock tracked, its call pending.
#[derive(Debug)]
pub struct LockedRequest<VmId>
where
    VmId: Eq + Hash,
{
    /// The stored request — deliver this call.
    pub record: ActionCallRequestRecord<VmId>,

    /// The call not yet requested; confirm it once it is.
    pub pending_call_request: crate::action_call_request::locks::PendingCallRequest<VmId>,
}

/// The outcome of one VM's pass: the locked requests, their calls
/// pending, and every other row of the VM.
#[derive(Debug)]
pub struct PerVmLocks<VmId>
where
    VmId: Eq + Hash,
{
    /// The VM this outcome is for.
    pub vm_id: VmId,

    /// Rows now locked by this process — deliver these calls.
    pub locked: Vec<LockedRequest<VmId>>,

    /// Rows not locked by this call and not held by this process.
    pub held_elsewhere: Vec<ActionCallRequestKey<VmId>>,

    /// Rows this process held with a lock unexpired when the lock statement
    /// ran, each with the lock it holds.
    pub held_by_this_owner: Vec<ActionCallRequestLockId<VmId>>,
}

/// Handle for submitting a VM to the shared lock batcher.
pub type PerVmLockerHandle<VmId> =
    waymark_batcher::BatcherHandle<VmId, Result<PerVmLocks<VmId>, PerVmLockingError>>;

/// Folds a duplicate vm id within one window onto the newer submission.
struct LastSubmissionWins;

impl<VmId>
    waymark_batcher::deduplicating_write::ConflictResolver<
        VmId,
        Result<PerVmLocks<VmId>, PerVmLockingError>,
    > for LastSubmissionWins
where
    VmId: Eq + Hash,
{
    type Placeholder = ();

    fn resolve_conflict<'a>(
        &self,
        slot: waymark_batcher::deduplicating_write::ConflictedSlot<
            'a,
            VmId,
            Result<PerVmLocks<VmId>, PerVmLockingError>,
            (),
        >,
        newcomer: VmId,
    ) -> waymark_batcher::deduplicating_write::ConflictResolvedToken<'a> {
        let (_abandoned, resolving) = slot.replace(newcomer);
        resolving.resolve(())
    }

    fn settle_conflict(
        &self,
        _conflicted_out: (),
        _winner_out: &Result<PerVmLocks<VmId>, PerVmLockingError>,
    ) -> Result<PerVmLocks<VmId>, PerVmLockingError> {
        Err(PerVmLockingError::Failed)
    }
}

/// The shared lock batcher: a locker handle for the adopting settlers,
/// and the batcher's loop for the caller to spawn.
///
/// `tracker` is the tracker of action-call request locks the taken locks
/// go into.
///
/// The future resolves once `shutdown` fires (or every handle is dropped)
/// and the last buffered batch has been flushed.
pub fn run<Backend>(
    backend: Arc<Backend>,
    lock_owner_id: Backend::LockOwnerId,
    lock_time_to_live: NonZeroDuration,
    tracker: Arc<std::sync::Mutex<Tracker<Backend::VmId>>>,
    policy: waymark_batcher::Policy,
    shutdown: impl Future<Output = ()>,
) -> (PerVmLockerHandle<Backend::VmId>, impl Future<Output = ()>)
where
    Backend: LockActionCallRequests<Timestamp = DateTime<Utc>>,
    Backend::VmId: Clone + Hash + Eq,
    Backend::LockOwnerId: Clone,
{
    waymark_batcher::deduplicating_write_batcher(
        policy,
        |vm_id: &Backend::VmId| vm_id.clone(),
        LastSubmissionWins,
        move |batch: NEVec<Backend::VmId>| {
            let backend = Arc::clone(&backend);
            let lock_owner_id = lock_owner_id.clone();
            let tracker = Arc::clone(&tracker);
            async move {
                let taken_at = Instant::now();
                let now = Utc::now();
                // One nonce per statement: the batch takes one lock set,
                // and a later lock on any of these rows draws its own.
                let lock_nonce = rand::random::<u64>();
                let lock = fresh_lock(now, &lock_owner_id, lock_time_to_live, lock_nonce);
                match backend
                    .lock_action_call_requests(now, lock, batch.as_nonempty_slice())
                    .await
                {
                    Ok(outcomes) => outcomes
                        .into_nonempty_iter()
                        .map(|outcome| {
                            let PerVmActionCallRequestLockingOutcome {
                                vm_id,
                                locked,
                                held_elsewhere,
                                held_by_this_owner,
                            } = outcome;
                            let locked = locked
                                .into_iter()
                                .map(|record| {
                                    let lock_id = ActionCallRequestLockId {
                                        request: ActionCallRequestKey {
                                            vm_id: record.vm_id.clone(),
                                            promise_state_id: record.promise_state_id,
                                        },
                                        nonce: lock_nonce,
                                    };
                                    LockedRequest {
                                        record,
                                        pending_call_request: Tracker::track_lock(
                                            &tracker,
                                            lock_id,
                                            taken_at + lock_time_to_live.get(),
                                        ),
                                    }
                                })
                                .collect();
                            Ok(PerVmLocks {
                                vm_id,
                                locked,
                                held_elsewhere,
                                held_by_this_owner,
                            })
                        })
                        .collect(),
                    Err(error) => {
                        tracing::warn!(?error, "locking a request batch failed");
                        batch
                            .nonempty_iter()
                            .map(|_| Err(PerVmLockingError::Failed))
                            .collect()
                    }
                }
            }
        },
        shutdown,
    )
}

#[cfg(test)]
mod tests;
