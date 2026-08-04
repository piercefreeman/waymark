//! Lock renewal trait — the per-process heartbeat over held locks.

use nonempty_collections::{NESlice, NEVec};

use super::common::{ActionCallRequestKey, HasLockOwnerId, HasTimestamp, HasVmId, RequestLockFor};

/// Backend capability for renewing request locks held by one owner.
pub trait RenewActionCallRequestLocks: HasVmId + HasLockOwnerId + HasTimestamp {
    /// The error type for renew operations.
    type Error: core::fmt::Debug;

    /// Extend the lock expiry to `lock.expires_at` for every key still
    /// locked by `lock.owner`.
    ///
    /// Returns one [`RequestLockRenewal`] per input key.  Keys that could
    /// not be renewed tell the caller to prune its in-memory tracking:
    /// [`RenewalStatus::Missing`] means the row is gone — its completion
    /// was durably recorded (the store removed the row) or the VM was
    /// purged; [`RenewalStatus::HeldElsewhere`] means the lock expired and
    /// another owner took it — the attempt may now run duplicated there
    /// (accepted at-least-once semantics).
    ///
    /// [`RenewalStatus::HeldElsewhere`] must be **verified against a
    /// current read**, never concluded from state that may predate a
    /// concurrent removal: a completion recorded while the renewal runs
    /// must classify as [`RenewalStatus::Missing`].  When ownership is
    /// confirmed but the expiry extension is not, the implementation
    /// reports [`RenewalStatus::Unconfirmed`] and the caller retries at
    /// its next heartbeat.
    fn renew_action_call_request_locks<'a>(
        &'a self,
        lock: RequestLockFor<Self>,
        keys: NESlice<'a, ActionCallRequestKey<Self::VmId>>,
    ) -> impl Future<Output = Result<NEVec<RequestLockRenewal<Self::VmId>>, Self::Error>> + Send + 'a;
}

/// The per-key result of a renewal pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLockRenewal<VmId> {
    /// The request this renewal is for.
    pub key: ActionCallRequestKey<VmId>,

    /// What happened to this key's lock.
    pub status: RenewalStatus,
}

/// What happened to one key during a renewal pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RenewalStatus {
    /// The lock was still held by the caller; its expiry was extended.
    Renewed,

    /// No row exists for this key — its completion was durably recorded,
    /// or the VM was purged.  Prune local tracking.
    Missing,

    /// The row is locked by a different owner.  Prune local tracking; the
    /// attempt may run duplicated there.
    HeldElsewhere,

    /// The lock is still owned by the caller, but this pass could not
    /// confirm the expiry extension (the row changed under the renewal
    /// statement).  Keep local tracking unchanged — the existing fence
    /// deadline stands — and retry at the next heartbeat.
    Unconfirmed,
}
