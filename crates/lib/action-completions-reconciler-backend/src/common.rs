//! Common types shared by the durable-completions backend traits.

use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Common base: every durable-completions backend is associated with a VM
/// identifier type.
pub trait HasVmId {
    /// The VM / workflow identifier type.
    type VmId;
}

/// One durably-stored action-call completion.
///
/// Keyed by `(vm_id, promise_state_id)` — a re-emitted effect reuses the
/// same pair, so the key alone deduplicates re-deliveries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionRecord<VmId> {
    /// The VM instance that owns the completed call.
    pub vm_id: VmId,

    /// The promise this completion settles, scoped to the VM.
    pub promise_state_id: PromiseStateId,

    /// The effect that emitted the call.  Participates in the divergence
    /// check on record: the same key arriving with a different effect
    /// number violates the "same effect ⇒ same pair" invariant.
    pub effect_number: EffectNumber,

    /// The opaque codec-encoded action-call outcome.
    pub outcome: Vec<u8>,
}

/// The key identifying a durably-stored completion — used both for demand
/// polling and for acking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletionKey<VmId> {
    /// The VM instance that owns the call.
    pub vm_id: VmId,

    /// The promise the caller is waiting on, scoped to the VM.
    pub promise_state_id: PromiseStateId,
}
