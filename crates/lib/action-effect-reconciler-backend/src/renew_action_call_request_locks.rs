//! Lock renewal trait — the per-process heartbeat over held locks.

use nonempty_collections::{NESlice, NEVec};

use super::common::{ActionCallRequestLockId, HasLockOwnerId, HasTimestamp, HasVmId};

/// Backend capability for renewing request locks held by one owner.
pub trait RenewActionCallRequestLocks: HasVmId + HasLockOwnerId + HasTimestamp {
    /// The error type for renew operations.
    type Error: core::fmt::Debug;

    /// Extend the lock expiry to `expires_at` for every lock of `lock_ids`
    /// the row still holds for `owner`.
    ///
    /// `now` is the caller-clock instant `expires_at` was computed
    /// against.  Implementations keep expiry on the store's clock alone:
    /// the extended expiry is stored as the store's now plus the
    /// remaining duration `expires_at - now` — a difference of two
    /// caller-clock values, so no cross-node clock agreement is needed.
    ///
    /// Returns one [`RequestLockRenewal`] per input lock.  Locks that
    /// could not be renewed tell the caller to prune its in-memory
    /// tracking: [`RenewalStatus::Missing`] means the row is gone — its
    /// completion was durably recorded (the store removed the row) or the
    /// VM was purged; [`RenewalStatus::Relocked`] means the row holds
    /// another lock of the same owner; [`RenewalStatus::HeldElsewhere`]
    /// means the lock expired and another owner took it — the attempt may
    /// now run duplicated there (accepted at-least-once semantics).
    ///
    /// [`RenewalStatus::Relocked`] and [`RenewalStatus::HeldElsewhere`]
    /// must be **verified against a current read**, never concluded from
    /// state that may predate a concurrent removal: a completion recorded
    /// while the renewal runs must classify as [`RenewalStatus::Missing`].
    /// When the lock is confirmed held but the expiry extension is not,
    /// the implementation reports [`RenewalStatus::Unconfirmed`] and the
    /// caller retries at its next heartbeat.
    fn renew_action_call_request_locks<'a>(
        &'a self,
        now: Self::Timestamp,
        owner: &'a Self::LockOwnerId,
        expires_at: Self::Timestamp,
        lock_ids: NESlice<'a, ActionCallRequestLockId<Self::VmId>>,
    ) -> impl Future<Output = Result<NEVec<RequestLockRenewal<Self::VmId>>, Self::Error>> + Send + 'a;
}

/// The per-lock result of a renewal pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLockRenewal<VmId> {
    /// The lock this renewal is for.
    pub lock_id: ActionCallRequestLockId<VmId>,

    /// What happened to the lock.
    pub status: RenewalStatus,
}

/// What happened to one lock during a renewal pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RenewalStatus {
    /// The row still held the lock; its expiry was extended.
    Renewed,

    /// No row exists for the lock's request — its completion was durably
    /// recorded, or the VM was purged.  Prune local tracking.
    Missing,

    /// The row holds another lock of the same owner.  Prune local
    /// tracking; the attempt the renewed lock authorized is running
    /// without authorization.
    Relocked,

    /// The row is locked by a different owner.  Prune local tracking; the
    /// attempt may run duplicated there.
    HeldElsewhere,

    /// The row still holds the lock, but this pass could not confirm the
    /// expiry extension (the row changed under the renewal statement).
    /// Keep local tracking unchanged — the existing fence deadline stands
    /// — and retry at the next heartbeat.
    Unconfirmed,
}
