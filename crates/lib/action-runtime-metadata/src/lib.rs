//! Correlation metadata carried alongside action calls.
//!
//! The action runtime request/completion types
//! ([`waymark_action_runtime_core`]) are generic over a metadata type; this
//! crate provides the concrete metadata shapes and the accessor traits that
//! let metadata-agnostic consumers read only the fields they need.

#![warn(missing_docs)]

use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// The correlation carried alongside an action call.
///
/// This pairs the effect that triggered a call with the promise state it
/// fulfills — the minimum needed to route a completion back to the promise
/// that awaits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionCallCorrelation {
    /// The sequential number of the effect that triggered the call.
    pub effect_number: EffectNumber,

    /// The id of the promise state the call fulfills.
    pub promise_state_id: PromiseStateId,
}

/// Wraps action-call metadata with the identifier of the VM instance that
/// owns the call.
///
/// Deployments that multiplex many VMs over a shared requester/completions
/// pipeline use this so completions can be routed back to the originating VM.
/// The inner metadata is preserved unchanged, so all correlation continues to
/// work through the trait impls below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WithVmId<VmId, Metadata> {
    /// The VM instance that owns the call.
    pub vm_id: VmId,

    /// The wrapped metadata.
    pub inner: Metadata,
}

/// Metadata from which the action-call correlation can be recovered.
///
/// Consumers that only need to settle a promise (e.g. the reconciler) bound on
/// this trait rather than a concrete metadata type, so they work with any
/// metadata shape — including [`WithVmId`]-wrapped ones.
pub trait ActionCallCorrelated {
    /// Recover the correlation pair for this metadata.
    fn call_correlation(&self) -> ActionCallCorrelation;
}

impl ActionCallCorrelated for ActionCallCorrelation {
    fn call_correlation(&self) -> ActionCallCorrelation {
        *self
    }
}

impl<VmId, Metadata: ActionCallCorrelated> ActionCallCorrelated for WithVmId<VmId, Metadata> {
    fn call_correlation(&self) -> ActionCallCorrelation {
        self.inner.call_correlation()
    }
}

/// Metadata that identifies the VM instance owning the call.
///
/// Only VM-scoped metadata (i.e. [`WithVmId`]) implements this, so routing
/// code that bounds on it will not accept metadata that lacks a VM id.
pub trait VmScoped {
    /// The type of VM identifier carried by this metadata.
    type VmId: Copy;

    /// The VM instance that owns the call.
    fn vm_id(&self) -> Self::VmId;
}

impl<VmId: Copy, Metadata> VmScoped for WithVmId<VmId, Metadata> {
    type VmId = VmId;

    fn vm_id(&self) -> VmId {
        self.vm_id
    }
}
