//! Batched locking of VMs' pending action-call requests at revival
//! reconcile.
//!
//! Every revived VM's [`ReconcilingFactory`](crate::ReconcilingFactory)
//! submits its id into one shared
//! [`deduplicating_write_batcher`](waymark_batcher::deduplicating_write_batcher)
//! keyed by the vm id and awaits its own [`VmLockOutcome`], so
//! reconcile-before-produce still holds per VM while many single-VM lock
//! statements coalesce into one multi-VM
//! [`lock_action_call_requests`](LockActionCallRequests::lock_action_call_requests)
//! statement pair.  The trait's per-input-order contract aligns exactly
//! with the batcher's positional fan-out, so outcomes zip straight back
//! to their waiters.
//!
//! A duplicate vm id within one window is reachable: a
//! [`produce`](waymark_state_manager_core::Factory::produce) future
//! dropped mid-submit strands its item, and the state manager's retry
//! submits the same vm again.  Submission order then guarantees the
//! earlier occurrence's waiter is dead — the state manager's per-key
//! single-flight admits no concurrent producers — so [`LastSubmissionWins`]
//! folds it out and only the live submission reaches the statement.
//!
//! One lock is built per batch, at flush.  Each waiter's local fence
//! base (`taken_at`) is captured at the same point, just before the
//! statement is sent, and returned alongside its outcome: pre-send, so
//! the local fence still trips before the stored lock lapses, and
//! per-flush, so its staleness is bounded by one attempt — queue wait
//! behind a slow flush cannot deliver locks whose fence is already dead.
//!
//! There is deliberately **no retry** here: the unbatched path never
//! retried either — a failed reconcile fails the spawn, the workload
//! stays unpinned, and the next pinning cycle retries.  A backend error
//! therefore fans [`LockError::Failed`] to every waiter in the batch;
//! the affected spawns fail and re-pin.

use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use waymark_action_effect_reconciler_backend::LockActionCallRequests;
use waymark_action_effect_reconciler_backend::lock_action_call_requests::VmLockOutcome;
use waymark_nonzero_duration::NonZeroDuration;

use crate::issuance::fresh_lock;

/// Fatal per-VM error for a batched lock — the spawn fails and the
/// workload is re-pinned later.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum LockError {
    /// The batched lock statement failed; nothing of the batch was locked.
    #[error("locking pending action-call requests failed")]
    Failed,

    /// The lock batcher has shut down; nothing was locked.
    #[error("the lock batcher is closed")]
    Closed,
}

/// Handle for submitting VM ids to the shared lock batcher.
///
/// A success pairs the VM's [`VmLockOutcome`] with the local fence base
/// for the taken locks — the pre-send instant of the flush statement.
pub type VmLockerHandle<VmId> =
    waymark_batcher::BatcherHandle<VmId, Result<(VmLockOutcome<VmId>, Instant), LockError>>;

/// The lock batcher's conflict resolver: the last submission wins.
///
/// A same-vm duplicate means the earlier submission was abandoned (its
/// waiter is dead — see the module docs), so the newcomer takes the slot
/// and the ousted submission settles to [`LockError::Failed`], delivered
/// to a dropped receiver.
struct LastSubmissionWins;

impl<VmId>
    waymark_batcher::deduplicating_write::ConflictResolver<
        VmId,
        Result<(VmLockOutcome<VmId>, Instant), LockError>,
    > for LastSubmissionWins
{
    type Placeholder = ();

    fn resolve_conflict<'a>(
        &self,
        slot: waymark_batcher::deduplicating_write::ConflictedSlot<
            'a,
            VmId,
            Result<(VmLockOutcome<VmId>, Instant), LockError>,
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
        _winner_out: &Result<(VmLockOutcome<VmId>, Instant), LockError>,
    ) -> Result<(VmLockOutcome<VmId>, Instant), LockError> {
        Err(LockError::Failed)
    }
}

/// Create the shared lock batcher: a locker handle for the reconciling
/// factory, and the batcher future for the caller to spawn.
///
/// The future resolves once `shutdown` fires (or every handle is dropped)
/// and the last buffered batch has been flushed.
pub fn lock_batcher<Backend>(
    backend: Arc<Backend>,
    lock_owner_id: Backend::LockOwnerId,
    lock_time_to_live: NonZeroDuration,
    policy: waymark_batcher::Policy,
    shutdown: impl Future<Output = ()>,
) -> (VmLockerHandle<Backend::VmId>, impl Future<Output = ()>)
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
            async move {
                // The pre-send instant: the conservative local fence
                // base for the locks this flush takes.
                let taken_at = Instant::now();
                let now = Utc::now();
                let lock = fresh_lock(now, &lock_owner_id, lock_time_to_live);
                match backend
                    .lock_action_call_requests(now, lock, batch.as_nonempty_slice())
                    .await
                {
                    // One outcome per input VM, in input order — exactly the
                    // batcher's positional fan-out.
                    Ok(outcomes) => outcomes
                        .into_nonempty_iter()
                        .map(|outcome| Ok((outcome, taken_at)))
                        .collect(),
                    Err(error) => {
                        tracing::warn!(?error, "locking a request batch failed");
                        NEVec::from_elem(Err(LockError::Failed), batch.len())
                    }
                }
            }
        },
        shutdown,
    )
}

#[cfg(test)]
mod tests;
