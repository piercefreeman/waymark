//! Backend traits for persisting pending action calls.
//!
//! The action reconciler ([`waymark_action_reconciler`]) records every
//! dispatched action call through these traits so that a crash between the
//! dispatch and the settlement of the corresponding promise does not lose
//! the call: on revive, the still-pending records are loaded and either
//! settled from their recorded outcome or re-dispatched.
//!
//! A record lives from just before the dispatch until the settlement it
//! produces has been applied to the VM and the resulting VM state has been
//! persisted; at that point the record is removed. In between, the outcome
//! of the call's execution is recorded onto the record
//! ([`StoreActionCallOutcome`]) as soon as it is known, so a completed call
//! never re-executes just because the VM did not live to see the result.

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

/// The recorded outcome of a pending action call's execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingActionCallOutcome {
    /// The call completed successfully with this codec-encoded value.
    Value(Vec<u8>),

    /// The call failed with this codec-encoded exception.
    Exception(Vec<u8>),
}

/// The status of a [`StoreActionCallOutcome`] operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreActionCallOutcomeStatus {
    /// The outcome was recorded onto the pending-call record.
    Stored,

    /// Nothing was recorded: the record is absent (the call already settled
    /// and was removed) or it already carries an outcome (first write wins).
    NotPending,
}

/// Records the outcome of an action call's execution onto its pending-call
/// record.
///
/// First write wins: an outcome is recorded only onto a record that does
/// not have one yet, so duplicate completion deliveries cannot change the
/// outcome a promise settles with.
pub trait StoreActionCallOutcome: HasVmId {
    /// The error type for outcome-recording operations.
    type Error: core::fmt::Debug;

    /// Record the outcome of the given call's execution.
    fn store_action_call_outcome<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        promise_state_id: PromiseStateId,
        outcome: PendingActionCallOutcome,
    ) -> impl Future<Output = Result<StoreActionCallOutcomeStatus, Self::Error>> + Send + 'a;
}

/// A pending action call loaded from the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingActionCall {
    /// The correlation the call was dispatched with.
    pub correlation: ActionCallCorrelation,

    /// The codec-encoded call (action ref and arguments), as stored.
    pub payload: Vec<u8>,

    /// The recorded outcome of the call's execution, if it is already known.
    pub outcome: Option<PendingActionCallOutcome>,
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
    StorePendingActionCall + StoreActionCallOutcome + RemovePendingActionCall + LoadPendingActionCalls
{
}

impl<T> ActionReconcilerBackend for T where
    T: StorePendingActionCall
        + StoreActionCallOutcome
        + RemovePendingActionCall
        + LoadPendingActionCalls
{
}

/// A backend that persists nothing, for deployments where action calls are
/// not meant to outlive the executing process (e.g. transient executions
/// whose whole VM state dies with it).
///
/// Stores and removals do nothing; loads return no pending calls, so
/// reconciliation never re-dispatches anything.
#[derive(Debug, Clone, Copy)]
pub struct NoopBackend<VmId> {
    // The backend never owns a `VmId`; it only receives references.
    // `fn() -> VmId` keeps the type `Send`/`Sync` regardless of `VmId`.
    _phantom_data: core::marker::PhantomData<fn() -> VmId>,
}

// Manual impl: the derive would needlessly bound `VmId: Default`.
impl<VmId> Default for NoopBackend<VmId> {
    fn default() -> Self {
        Self {
            _phantom_data: core::marker::PhantomData,
        }
    }
}

impl<VmId> HasVmId for NoopBackend<VmId> {
    type VmId = VmId;
}

impl<VmId: Sync> StorePendingActionCall for NoopBackend<VmId> {
    type Error = core::convert::Infallible;

    async fn store_pending_action_call<'a>(
        &'a self,
        _vm_id: &'a Self::VmId,
        _correlation: ActionCallCorrelation,
        _payload: impl AsRef<[u8]> + Send + 'a,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<VmId: Sync> StoreActionCallOutcome for NoopBackend<VmId> {
    type Error = core::convert::Infallible;

    async fn store_action_call_outcome<'a>(
        &'a self,
        _vm_id: &'a Self::VmId,
        _promise_state_id: PromiseStateId,
        _outcome: PendingActionCallOutcome,
    ) -> Result<StoreActionCallOutcomeStatus, Self::Error> {
        Ok(StoreActionCallOutcomeStatus::NotPending)
    }
}

impl<VmId: Sync> RemovePendingActionCall for NoopBackend<VmId> {
    type Error = core::convert::Infallible;

    async fn remove_pending_action_call<'a>(
        &'a self,
        _vm_id: &'a Self::VmId,
        _promise_state_id: PromiseStateId,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<VmId: Sync> LoadPendingActionCalls for NoopBackend<VmId> {
    type Error = core::convert::Infallible;

    async fn load_pending_action_calls<'a>(
        &'a self,
        _vm_id: &'a Self::VmId,
    ) -> Result<Vec<PendingActionCall>, Self::Error> {
        Ok(Vec::new())
    }
}
