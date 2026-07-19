//! Per-VM request locking trait — the database half of revival reconcile.

use super::common::{
    ActionCallRequestKey, ActionCallRequestRecord, HasLockOwnerId, HasTimestamp, HasVmId,
    RequestLockFor,
};

/// Backend capability for locking a VM's pending requests for delivery.
pub trait LockVmActionCallRequests: HasVmId + HasLockOwnerId + HasTimestamp {
    /// The error type for lock operations.
    type Error: core::fmt::Debug;

    /// Lock every eligible request of `vm_id` with `lock`, returning the
    /// locked rows for delivery.
    ///
    /// A row is eligible when it is unlocked or its lock expired at or
    /// before the store's own now.  `now` is the caller-clock instant
    /// `lock.expires_at` was computed against: the taken lock's expiry
    /// is stored as the store's now plus `expires_at - now` — a
    /// difference of two caller-clock values, so no cross-node clock
    /// agreement is needed.  Rows locked by another owner and unexpired
    /// are left untouched and reported via
    /// [`VmLockOutcome::held_elsewhere`] — an attempt is presumed running
    /// in that owner's pool.
    ///
    /// The caller must deliver exactly the returned
    /// [`VmLockOutcome::locked`] rows to its local worker pool.  A VM with
    /// no request rows yields an empty outcome — a no-op.
    fn lock_vm_action_call_requests<'a>(
        &'a self,
        now: Self::Timestamp,
        lock: RequestLockFor<Self>,
        vm_id: &'a Self::VmId,
    ) -> impl Future<Output = Result<VmLockOutcome<Self::VmId>, Self::Error>> + Send + 'a;
}

/// The outcome of locking a VM's pending requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmLockOutcome<VmId> {
    /// Rows now locked by the caller — deliver these calls.
    pub locked: Vec<ActionCallRequestRecord<VmId>>,

    /// Rows held by another owner with an unexpired lock — left alone.
    pub held_elsewhere: Vec<ActionCallRequestKey<VmId>>,
}
