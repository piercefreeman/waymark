//! Common types shared by the durable-sleeps backend traits.

use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Common base: every durable-sleeps backend is associated with a VM
/// identifier type.
pub trait HasVmId {
    /// The VM / workflow identifier type.
    type VmId;
}

/// Common base: every durable-sleeps backend is associated with a
/// timestamp type.
pub trait HasTimestamp {
    /// The timestamp type for the sleep wake deadlines.
    type Timestamp;
}

/// One durably-recorded sleep request.
///
/// Keyed by `(vm_id, promise_state_id)` — a re-emitted effect reuses the
/// same pair, so the key alone deduplicates re-deliveries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleepRecord<VmId, Timestamp> {
    /// The VM instance that owns the sleep.
    pub vm_id: VmId,

    /// The promise this sleep settles on elapsing, scoped to the VM.
    pub promise_state_id: PromiseStateId,

    /// The effect that emitted the sleep.  Participates in the divergence
    /// check on record: the same key arriving with a different effect
    /// number violates the "same effect ⇒ same pair" invariant.
    pub effect_number: EffectNumber,

    /// The absolute deadline at which the sleep elapses.
    pub wake_at: Timestamp,
}

/// The key identifying a durably-recorded sleep — used both for demand
/// polling and for acking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SleepKey<VmId> {
    /// The VM instance that owns the sleep.
    pub vm_id: VmId,

    /// The promise the caller is waiting on, scoped to the VM.
    pub promise_state_id: PromiseStateId,
}
