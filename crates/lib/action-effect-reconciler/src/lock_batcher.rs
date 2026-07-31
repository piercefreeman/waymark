//! Batched locking of VMs' pending action-call requests at revival
//! reconcile.
//!
//! Every revived VM's [`ReconcilingFactory`](crate::ReconcilingFactory)
//! submits its id into one shared [`waymark_batcher`] write-batcher and
//! awaits its own [`VmLockOutcome`], so reconcile-before-produce still
//! holds per VM while many single-VM lock statements coalesce into one
//! multi-VM
//! [`lock_action_call_requests`](LockActionCallRequests::lock_action_call_requests)
//! statement pair.  The trait's per-input-order contract aligns exactly
//! with the batcher's positional fan-out, so outcomes zip straight back
//! to their waiters.
//!
//! One lock is built per batch, at flush.  Each submitter's local fence
//! base (`taken_at`) is captured before its submission, which predates
//! the flush instant, so the local fence always trips before the stored
//! lock lapses — the same conservative direction as the unbatched path.
//!
//! There is deliberately **no retry** here: the unbatched path never
//! retried either — a failed reconcile fails the spawn, the workload
//! stays unpinned, and the next pinning cycle retries.  A backend error
//! therefore fans [`LockError::Failed`] to every waiter in the batch;
//! the affected spawns fail and re-pin.

use std::sync::Arc;

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
pub type VmLockerHandle<VmId> =
    waymark_batcher::BatcherHandle<VmId, Result<VmLockOutcome<VmId>, LockError>>;

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
    Backend::VmId: Clone,
    Backend::LockOwnerId: Clone,
{
    waymark_batcher::write_batcher(
        policy,
        move |batch: NEVec<Backend::VmId>| {
            let backend = Arc::clone(&backend);
            let lock_owner_id = lock_owner_id.clone();
            async move {
                let now = Utc::now();
                let lock = fresh_lock(now, &lock_owner_id, lock_time_to_live);
                match backend
                    .lock_action_call_requests(now, lock, batch.as_nonempty_slice())
                    .await
                {
                    // One outcome per input VM, in input order — exactly the
                    // batcher's positional fan-out.
                    Ok(outcomes) => outcomes.into_nonempty_iter().map(Ok).collect(),
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
