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
    /// [`acknowledge_promise_settlement`](PromiseSettlementAck::acknowledge_promise_settlement)
    /// is called after the settlement has been applied *and* the resulting
    /// VM state has been persisted, so the settler can reclaim resources or
    /// confirm delivery. Dropping this handle without acknowledging is a
    /// negative acknowledgement (Nack) — see [`PromiseSettlementAck`].
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
/// state it calls [`acknowledge_promise_settlement`](PromiseSettlementAck::acknowledge_promise_settlement)
/// on each [`PromiseSettlement::ack`] in the batch.  This lets the settler
/// reclaim resources, notify upstream systems, or confirm delivery.
///
/// # Acknowledgement is synchronous but may defer its work
///
/// [`acknowledge_promise_settlement`](PromiseSettlementAck::acknowledge_promise_settlement)
/// is deliberately **synchronous** and must not block the driver: it runs
/// on the driver's hot path between snapshot persistence and the next VM
/// step, and the VM must not wait on any upstream acknowledgement I/O.
///
/// Implementations that need to perform work (e.g. deleting a queue row or
/// notifying an upstream system) must **defer** it — hand the work off to a
/// background task, channel, or batch — and return immediately. Deferral is
/// safe because settlement delivery is idempotent: if a deferred
/// acknowledgement is lost, the settlement is simply redelivered and applied
/// again, and repeating an already-applied resolution is a no-op that can be
/// safely ignored (see [`PromiseSettler`]'s cancellation contract).
///
/// # Dropping without acknowledging is a Nack
///
/// The ack handle carries **negative acknowledgement on `Drop`**: if an
/// [`Ack`](PromiseSettlement::ack) is dropped without
/// [`acknowledge_promise_settlement`](PromiseSettlementAck::acknowledge_promise_settlement)
/// having been called, that signals the settlement was *not* durably
/// consumed and must be redelivered. The core relies on this — the driver
/// only calls `acknowledge_promise_settlement` after the resulting VM state
/// has been persisted, so an ack dropped on any earlier error or
/// cancellation path correctly Nacks the settlement. Implementations that
/// want Nack-on-failure semantics should encode them in the ack's `Drop`
/// impl (e.g. leave the queue row in place unless explicitly acknowledged).
pub trait PromiseSettlementAck {
    /// Acknowledge that the settlement has been durably applied.
    ///
    /// Must not block: defer any real work to the background. See the
    /// [trait docs](PromiseSettlementAck) for the deferral and Drop-Nack
    /// contract.
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
