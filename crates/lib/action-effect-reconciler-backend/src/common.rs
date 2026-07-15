//! Common types shared by all durable-requests backend traits.

use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Common base: every durable-requests backend is associated with a VM
/// identifier type.
pub trait HasVmId {
    /// The VM / workflow identifier type.
    type VmId;
}

/// Common base: every durable-requests backend is associated with a lock
/// owner identifier type — the identity of one execution-host process.
pub trait HasLockOwnerId {
    /// The lock owner identifier type.
    type LockOwnerId;
}

/// Common base: the time representation used for lock expiry.
pub trait HasTimestamp {
    /// The time representation used by this backend.
    type Timestamp;
}

/// One durably-stored action-call request.
///
/// Keyed by `(vm_id, promise_state_id)` — a re-emitted effect reuses the
/// same pair, so the key alone deduplicates replays.  The row's existence
/// means the call's outcome has not been durably recorded yet (see the
/// crate-level removal invariant).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionCallRequestRecord<VmId> {
    /// The VM instance that owns the call.
    pub vm_id: VmId,

    /// The promise the call will settle, scoped to the VM.
    pub promise_state_id: PromiseStateId,

    /// The effect that emitted the call.
    pub effect_number: EffectNumber,

    /// The opaque codec-encoded request payload (action reference and
    /// arguments).
    pub request: Vec<u8>,
}

/// The key identifying a durably-stored request — used for lock renewal
/// and unlocking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActionCallRequestKey<VmId> {
    /// The VM instance that owns the call.
    pub vm_id: VmId,

    /// The promise the call will settle, scoped to the VM.
    pub promise_state_id: PromiseStateId,
}

/// A lock to apply to request rows: who holds it and until when.
///
/// The expiry is authoritative in the database; holders self-limit on a
/// local monotonic countdown and treat a lock they could not renew in
/// time as lost.
#[derive(Debug, Clone)]
pub struct RequestLock<LockOwnerId, Timestamp> {
    /// The process holding the lock.
    pub owner: LockOwnerId,

    /// When this lock stops being valid unless renewed.
    pub expires_at: Timestamp,
}

/// Shorthand for a [`RequestLock`] using the associated types of `T`.
pub type RequestLockFor<T> =
    RequestLock<<T as crate::HasLockOwnerId>::LockOwnerId, <T as crate::HasTimestamp>::Timestamp>;
