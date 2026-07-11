//! Backend traits for persisting pending action calls.
//!
//! The action reconciler ([`waymark_action_reconciler`]) records every
//! dispatched action call through these traits so that a crash between the
//! dispatch and the settlement of the corresponding promise does not lose
//! the call: on revive, the still-pending records are loaded and
//! re-dispatched.
//!
//! A record lives from just before the dispatch until the settlement it
//! produces has been applied to the VM and the resulting VM state has been
//! persisted; at that point the record is removed.

#![warn(missing_docs)]

use waymark_action_runtime_metadata::ActionCallCorrelation;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Common base: every pending-action-call backend is associated with a VM
/// identifier type.
pub trait HasVmId {
    /// The VM identifier type.
    type VmId;
}

/// Records a pending action call before it is dispatched.
///
/// Implementations must be idempotent for identical values: a VM revived
/// from a snapshot taken before the call was dispatched deterministically
/// re-emits the same effect and stores the same record under the same
/// `(vm_id, promise_state_id)` key.
pub trait StorePendingActionCall: HasVmId {
    /// The error type for store operations.
    type Error: core::fmt::Debug;

    /// Record a pending action call.
    ///
    /// `payload` carries the codec-encoded call (action ref and arguments),
    /// opaque to the backend.
    fn store_pending_action_call<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        correlation: ActionCallCorrelation,
        payload: impl AsRef<[u8]> + Send + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Removes a pending action call once its settlement has been durably
/// applied to the VM state.
pub trait RemovePendingActionCall: HasVmId {
    /// The error type for remove operations.
    type Error: core::fmt::Debug;

    /// Remove the pending action call for the given promise.
    ///
    /// Removing an absent record is not an error — removal races benignly
    /// with reconciliation cleanup.
    fn remove_pending_action_call<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        promise_state_id: PromiseStateId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// A pending action call loaded from the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingActionCall {
    /// The correlation the call was dispatched with.
    pub correlation: ActionCallCorrelation,

    /// The codec-encoded call (action ref and arguments), as stored.
    pub payload: Vec<u8>,
}

/// Loads the pending action calls of a VM for re-dispatch on revive.
pub trait LoadPendingActionCalls: HasVmId {
    /// The error type for load operations.
    type Error: core::fmt::Debug;

    /// Load all pending action calls for the given VM, ordered by effect
    /// number.
    fn load_pending_action_calls<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
    ) -> impl Future<Output = Result<Vec<PendingActionCall>, Self::Error>> + Send + 'a;
}

/// Convenience trait: a backend that includes all traits from this crate.
pub trait ActionReconcilerBackend:
    StorePendingActionCall + RemovePendingActionCall + LoadPendingActionCalls
{
}

impl<T> ActionReconcilerBackend for T where
    T: StorePendingActionCall + RemovePendingActionCall + LoadPendingActionCalls
{
}
