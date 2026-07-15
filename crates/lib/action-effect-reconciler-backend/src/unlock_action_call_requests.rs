//! Request unlocking trait — graceful-shutdown release of held locks.

use nonempty_collections::NESlice;

use super::common::{ActionCallRequestKey, HasLockOwnerId, HasVmId};

/// Backend capability for releasing request locks held by one owner.
pub trait UnlockActionCallRequests: HasVmId + HasLockOwnerId {
    /// The error type for unlock operations.
    type Error: core::fmt::Debug;

    /// Clear the lock on every key still locked by `owner`.
    ///
    /// Called at graceful shutdown for requests whose calls were never
    /// delivered to the local pool — an unlocked row is immediately
    /// eligible for delivery by another process's revival reconcile.
    /// Unlocking is idempotent: keys with no matching row, or rows locked
    /// by a different owner, are silently skipped.
    fn unlock_action_call_requests<'a>(
        &'a self,
        owner: &'a Self::LockOwnerId,
        keys: NESlice<'a, ActionCallRequestKey<Self::VmId>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
