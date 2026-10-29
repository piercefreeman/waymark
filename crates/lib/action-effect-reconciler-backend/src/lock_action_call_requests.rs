//! Per-VM request locking trait — relocks a VM's lapsed requests for
//! delivery.

use nonempty_collections::{NESlice, NEVec};

use super::common::{
    ActionCallRequestKey, ActionCallRequestRecord, HasLockOwnerId, HasTimestamp, HasVmId,
    RequestLockFor,
};

/// Backend capability for locking VMs' pending requests for delivery, in a
/// batch.
pub trait LockActionCallRequests: HasVmId + HasLockOwnerId + HasTimestamp {
    /// The error type for lock operations.
    type Error: core::fmt::Debug;

    /// Lock every eligible request of each of `vm_ids` with `lock`,
    /// returning one outcome per input VM, in input order.
    ///
    /// A row is eligible when it is unlocked or its lock expired at or
    /// before the store's own now.  `now` is the caller-clock instant
    /// `lock.expires_at` was computed against: the taken lock's expiry
    /// is stored as the store's now plus `expires_at - now` — a
    /// difference of two caller-clock values, so no cross-node clock
    /// agreement is needed.  Every other row of the VM is left untouched
    /// and reported: one the caller itself held, unexpired when the lock
    /// statement ran, via [`VmLockOutcome::held_by_this_owner`], any other
    /// via [`VmLockOutcome::held_elsewhere`] — an attempt is presumed running
    /// in that owner's pool.
    ///
    /// The caller must deliver exactly the returned
    /// [`VmLockOutcome::locked`] rows to its local worker pool.  A VM with
    /// no request rows yields an empty outcome — a no-op.
    fn lock_action_call_requests<'a>(
        &'a self,
        now: Self::Timestamp,
        lock: RequestLockFor<Self>,
        vm_ids: NESlice<'a, Self::VmId>,
    ) -> impl Future<Output = Result<NEVec<VmLockOutcome<Self::VmId>>, Self::Error>> + Send + 'a;
}

/// The outcome of locking one VM's pending requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmLockOutcome<VmId> {
    /// The VM this outcome is for.
    pub vm_id: VmId,

    /// Rows now locked by the caller — deliver these calls.
    pub locked: Vec<ActionCallRequestRecord<VmId>>,

    /// Rows not locked by this call and not held by the caller — left
    /// alone.
    pub held_elsewhere: Vec<ActionCallRequestKey<VmId>>,

    /// Rows the caller held with a lock unexpired when the lock statement
    /// ran — left alone.
    pub held_by_this_owner: Vec<ActionCallRequestKey<VmId>>,
}
