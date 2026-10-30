//! Request unlocking trait — release of held locks.

use nonempty_collections::NESlice;

use super::common::{ActionCallRequestLockId, HasLockOwnerId, HasVmId};

/// Backend capability for releasing request locks held by one owner.
pub trait UnlockActionCallRequests: HasVmId + HasLockOwnerId {
    /// The error type for unlock operations.
    type Error: core::fmt::Debug;

    /// Clear each lock of `lock_ids` its row still holds for `owner`.
    ///
    /// Called for requests whose calls were never delivered to the local
    /// pool — an unlocked row is immediately eligible for relocking by
    /// the process hosting the VM.  Unlocking is idempotent: locks with no
    /// matching row, rows locked by a different owner, and rows the owner
    /// locked again since are silently skipped.
    fn unlock_action_call_requests<'a>(
        &'a self,
        owner: &'a Self::LockOwnerId,
        lock_ids: NESlice<'a, ActionCallRequestLockId<Self::VmId>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
