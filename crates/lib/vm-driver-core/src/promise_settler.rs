//! Promise settlement contracts for the VM driver.
//!
//! A suspended VM waits for one or more pending promises to be settled.
//! The [`PromiseSettler`] trait supplies batches of settlements, and
//! [`PromiseSettlementAck`] lets the settler know when each has been
//! durably consumed (i.e. the VM state has been persisted).

use nonempty_collections::NEVec;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// A single promise settlement, ready to be applied to a VM.
pub struct PromiseSettlement<Value, Ack> {
    /// Identifies the pending promise in the VM's state.
    pub promise_state_id: PromiseStateId,

    /// Whether to resolve or reject the promise.
    pub resolution: PromiseResolution<Value>,

    /// Opaque acknowledgement handle.
    ///
    /// Call [`PromiseSettlementAck::acknowledge_settlement`] after the
    /// settlement has been applied *and* the resulting VM state has been
    /// persisted, so the settler can reclaim resources or confirm delivery.
    pub ack: Ack,
}

/// A promise resolution.
pub enum PromiseResolution<Value> {
    /// Resolve the promise successfully.
    Resolved(Value),

    /// Reject the promise with an exception.
    Rejected(waymark_vm_runtime_exception::Exception<Value>),
}

/// Convenience alias for the [`PromiseSettlement`] type for a given
/// [`PromiseSettler`].
pub type PromiseSettlementFor<T> =
    PromiseSettlement<<T as PromiseSettler>::Value, <T as PromiseSettler>::Ack>;

/// Supplies promise settlements to a suspended VM.
///
/// The returned batch is non-empty. The driver applies each settlement
/// to the VM, snapshots the new state, and then calls
/// [`PromiseSettlementAck::acknowledge_promise_settlement`] on every ack.
pub trait PromiseSettler {
    /// The value type that the promise can be resolved with.
    type Value;

    /// The error returned by
    /// [`get_promise_settlements`](PromiseSettler::get_promise_settlements).
    type Error: std::fmt::Debug;

    /// The promise settlement acknowledgement type.
    type Ack: PromiseSettlementAck;

    /// Wait for one or more available promise settlements.
    ///
    /// `waiting_ids` provides the IDs of all currently waiting promises
    /// so the settler knows which promises it is expected to settle.
    ///
    /// # Cancellation safety
    ///
    /// The caller may drop the returned future at any time. Implementations
    /// **must** be cancel-safe: if the future is dropped before returning,
    /// no settlements already dequeued from the underlying source should
    /// be lost.
    fn get_promise_settlements(
        &mut self,
        // TODO: switch to`impl IntoNonEmptyIterator` when
        // https://github.com/rust-lang/rust/issues/100013 is resolved.
        waiting_promise_state_ids: NEVec<PromiseStateId>,
    ) -> impl Future<Output = Result<NEVec<PromiseSettlementFor<Self>>, Self::Error>> + Send + '_;
}

/// Acknowledges that a promise settlement has been durably consumed.
///
/// After the driver applies a settlement and persists the resulting VM
/// state it calls [`acknowledge_promise_settlement`] on each
/// [`PromiseSettlement::ack`] in the batch.  This lets the settler
/// reclaim resources, notify upstream systems, or confirm delivery.
pub trait PromiseSettlementAck {
    /// Acknowledge that the settlement has been durably applied.
    fn acknowledge_promise_settlement(self);
}

impl PromiseSettlementAck for () {
    fn acknowledge_promise_settlement(self) {}
}

impl<A, B> PromiseSettler for (A, B)
where
    B: PromiseSettler,
{
    type Value = B::Value;
    type Error = B::Error;
    type Ack = B::Ack;

    fn get_promise_settlements(
        &mut self,
        waiting_promise_state_ids: NEVec<PromiseStateId>,
    ) -> impl Future<Output = Result<NEVec<PromiseSettlementFor<Self>>, Self::Error>> + '_ {
        self.1.get_promise_settlements(waiting_promise_state_ids)
    }
}

#[cfg(feature = "tokio")]
impl PromiseSettlementAck for tokio::sync::oneshot::Sender<()> {
    fn acknowledge_promise_settlement(self) {
        let _ = self.send(());
    }
}

#[cfg(feature = "tokio")]
impl<Value, Ack> PromiseSettler for tokio::sync::mpsc::Receiver<PromiseSettlement<Value, Ack>>
where
    Value: Send,
    Ack: Send,
    Ack: PromiseSettlementAck,
{
    type Value = Value;
    type Error = ();
    type Ack = Ack;

    fn get_promise_settlements(
        &mut self,
        waiting_promise_state_ids: NEVec<PromiseStateId>,
    ) -> impl Future<Output = Result<NEVec<PromiseSettlementFor<Self>>, Self::Error>> + '_ {
        drop(waiting_promise_state_ids);
        async move {
            let first = self.recv().await.ok_or(())?;
            let mut bundle = NEVec::new(first);

            while let Ok(value) = self.try_recv() {
                bundle.push(value);
            }

            Ok(bundle)
        }
    }
}
