//! Demand-driven settlement polling — a single shared loop polls the
//! backend for exactly the completions the VMs are currently waiting on
//! and settles their promises.
//!
//! Built on [`waymark_promise_settlement_demand_registry::registry`]:
//! the registry owns demand registration, item buffering, delivery, and
//! the waiter lifecycle — see its docs for the architecture and the
//! cancellation-safety story.  This module owns the backend query, the
//! execution-result decoding, and the mapping of delivered completions into
//! promise settlements, each carrying an [`Ack`] minted from the row's
//! own key: acknowledging (after the settlement has been applied and the
//! VM state persisted) pushes the key onto the ack channel the registry
//! was created with, from where the background
//! [`acker::run`](crate::acker::run) loop deletes the row.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use nonempty_collections::{IntoNonEmptyIterator as _, NESlice, NEVec, NonEmptyIterator as _};
use waymark_action_completions_reconciler_backend::{
    CompletionKey, CompletionRecord, PollCompletions,
};
use waymark_action_runtime_core::{ActionCallLossError, ActionCallOutcome};
use waymark_convert_core::Convert;
use waymark_promise_settlement_demand_registry::registry;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Error returned when the shared poll loop stops on a critical failure.
#[derive(Debug, thiserror::Error)]
pub enum Error<PollError, DecodeError> {
    /// The backend failed to poll for completions.
    #[error("polling completions: {0}")]
    Poll(#[source] PollError),

    /// A stored execution result could not be decoded — the blob we
    /// wrote cannot be read back, which is a bug.
    #[error("unable to decode a stored action-call execution result: {0}")]
    ExecutionResultDecode(#[source] DecodeError),
}

/// Error returned when polling a [`SettlementsHandle`] for action
/// settlements fails.
#[derive(Debug, thiserror::Error)]
pub enum PollActionSettlementsError {
    /// The shared [`run`] loop is gone; no settlements will ever be
    /// delivered again.
    #[error("demand poller gone")]
    PollerGone,
}

/// Settlement acknowledgement for durably-stored completions.
///
/// Minted by a [`SettlementsHandle`] from the delivered row's own key.
/// Acknowledging (after the settlement has been applied and the VM state
/// persisted) pushes the key onto the poller's ack channel, from where
/// the background [`acker::run`](crate::acker::run) loop deletes the row.
pub struct Ack<VmId> {
    key: CompletionKey<VmId>,
    ack_tx: tokio::sync::mpsc::UnboundedSender<CompletionKey<VmId>>,
}

impl<VmId> waymark_vm_driver_core::PromiseSettlementAck for Ack<VmId> {
    fn acknowledge_promise_settlement(self) {
        // A closed channel means the acker is shutting down; the row stays
        // in the backend and is re-settled (and re-acked) on revive.
        let _ = self.ack_tx.send(self.key);
    }
}

impl<VmId, SleepAck> From<Ack<VmId>> for waymark_extcall_reconciler_core::Ack<Ack<VmId>, SleepAck> {
    fn from(value: Ack<VmId>) -> Self {
        waymark_extcall_reconciler_core::Ack::Action(value)
    }
}

/// One decoded completion parked in a handle's buffer, keyed by the
/// promise it settles.
struct BufferedCompletion<Value, RaisedException> {
    promise_state_id: PromiseStateId,
    execution_result: Result<ActionCallOutcome<Value, RaisedException>, ActionCallLossError>,
}

impl<Value, RaisedException> registry::HasPromiseStateId
    for BufferedCompletion<Value, RaisedException>
{
    fn promise_state_id(&self) -> PromiseStateId {
        self.promise_state_id
    }
}

// ---------------------------------------------------------------------------
// DemandRegistrar
// ---------------------------------------------------------------------------

/// Create the shared poller state.
///
/// Returns the cloneable [`DemandRegistrar`] for subscribing VMs and the
/// opaque [`StateToken`] that [`run`] consumes.
///
/// `ack_tx` is where the settlements produced by the subscribed handles
/// push their keys on acknowledgement — pair it with an
/// [`acker::run`](crate::acker::run) loop driving the receiving half.
#[expect(
    clippy::type_complexity,
    reason = "the registrar and the token are the two halves of the state, handed back by name"
)]
pub fn state<VmId, Value, RaisedException, ValueConverter>(
    ack_tx: tokio::sync::mpsc::UnboundedSender<CompletionKey<VmId>>,
) -> (
    DemandRegistrar<VmId, Value, RaisedException, ValueConverter>,
    StateToken<VmId, Value, RaisedException>,
)
where
    VmId: Eq + std::hash::Hash,
{
    let (registrar, token) = registry::state(ack_tx);
    (
        DemandRegistrar {
            inner: registrar,
            _value_converter: std::marker::PhantomData,
        },
        StateToken { inner: token },
    )
}

/// The opaque token of the shared poller state.
///
/// Created by [`state`]; its only job is to be handed to [`run`] via
/// [`Params`].
pub struct StateToken<VmId, Value, RaisedException>
where
    VmId: Eq + std::hash::Hash,
{
    inner:
        registry::StateToken<VmId, BufferedCompletion<Value, RaisedException>, CompletionKey<VmId>>,
}

/// Subscribes VMs to the shared poller state.
///
/// Created by [`state`]; cloneable, so it can be handed out to every
/// place that wires up VMs.
pub struct DemandRegistrar<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Eq + std::hash::Hash,
{
    inner: registry::DemandRegistrar<
        VmId,
        BufferedCompletion<Value, RaisedException>,
        CompletionKey<VmId>,
    >,
    _value_converter: std::marker::PhantomData<fn() -> ValueConverter>,
}

impl<VmId, Value, RaisedException, ValueConverter> Clone
    for DemandRegistrar<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Eq + std::hash::Hash,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _value_converter: std::marker::PhantomData,
        }
    }
}

impl<VmId, Value, RaisedException, ValueConverter>
    DemandRegistrar<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Copy + Eq + std::hash::Hash,
{
    /// Subscribe a VM and return its demand-driven settlements handle.
    ///
    /// Subscribing a VM that already has a live entry replaces the entry:
    /// the previous handle keeps its buffer but will no longer receive
    /// deliveries, and its eventual drop does not disturb the new entry.
    pub fn subscribe(
        &self,
        vm_id: VmId,
    ) -> SettlementsHandle<VmId, Value, RaisedException, ValueConverter> {
        SettlementsHandle {
            inner: self.inner.subscribe(vm_id),
            _value_converter: std::marker::PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

/// Parameters for [`run`].
pub struct Params<Backend, Codec, Value, RaisedException>
where
    Backend: PollCompletions,
    Backend::VmId: Eq + std::hash::Hash,
{
    /// The durable completions backend to poll.
    pub backend: Arc<Backend>,

    /// The codec used to decode stored action-call execution results.
    pub codec: Codec,

    /// The token of the shared state to poll for and deliver to.
    pub state: StateToken<Backend::VmId, Value, RaisedException>,
}

/// Poll the backend for demanded completions until every registrar and
/// handle is gone, or a critical failure.
///
/// Drive this in a background task.  Parks while no demand is registered;
/// otherwise polls in a tight loop.  Errors are critical, and the caller
/// should stop the subsystem.  When the loop returns (or its future is
/// dropped after starting), the shared state is marked closed and all
/// waiting handles fail.
pub async fn run<Backend, Codec, Value, RaisedException>(
    params: Params<Backend, Codec, Value, RaisedException>,
) -> Result<(), Error<Backend::Error, Codec::Error>>
where
    Backend: PollCompletions,
    Backend::VmId: Copy + Eq + std::hash::Hash,
    Codec: waymark_vm_codec_core::DeserializerProvider,
    Value: serde::de::DeserializeOwned,
    RaisedException: serde::de::DeserializeOwned,
{
    let Params {
        backend,
        codec,
        state,
    } = params;
    let driver = state.inner.into_driver();

    loop {
        // Arm the wakeup before collecting so a registration racing the
        // collection is never missed.
        let registered = driver.demand_registered();

        let Some(demand) = driver.collect_demand(|vm_id, promise_state_id| CompletionKey {
            vm_id: *vm_id,
            promise_state_id,
        }) else {
            // An empty collection has two ways out: new demand, or every
            // registrar and handle gone.
            tokio::select! {
                () = registered => continue,
                () = driver.all_registrars_and_handles_gone() => return Ok(()),
            }
        };

        let records = backend
            .poll_completions(demand.as_nonempty_slice())
            .await
            .map_err(Error::Poll)?;

        for record in records {
            let CompletionRecord {
                vm_id,
                promise_state_id,
                effect_number: _,
                execution_result,
            } = record;
            let execution_result = codec
                .with_deserializer(&execution_result, |deserializer| {
                    serde::Deserialize::deserialize(deserializer)
                })
                .map_err(Error::ExecutionResultDecode)?;
            driver.deliver(
                &vm_id,
                BufferedCompletion {
                    promise_state_id,
                    execution_result,
                },
            );
        }
    }
}

// ---------------------------------------------------------------------------
// SettlementsHandle
// ---------------------------------------------------------------------------

/// Per-VM demand-driven promise settler over durably-stored completions.
///
/// Created by [`DemandRegistrar::subscribe`].  Implements
/// [`waymark_extcall_reconciler_core::ActionPromiseSettler`]: each call
/// registers the demanded promise ids, waits until the shared poller
/// delivers matching completions, and settles them with [`Ack`]s minted
/// from the rows' own keys.
pub struct SettlementsHandle<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Eq + std::hash::Hash,
{
    inner: registry::DemandHandle<
        VmId,
        BufferedCompletion<Value, RaisedException>,
        CompletionKey<VmId>,
    >,
    _value_converter: std::marker::PhantomData<fn() -> ValueConverter>,
}

impl<VmId, Value, RaisedException, ValueConverter> waymark_extcall_reconciler_core::SettlerAck
    for SettlementsHandle<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Eq + std::hash::Hash,
{
    type Ack = Ack<VmId>;
}

impl<VmId, Value, RaisedException, ValueConverter> waymark_extcall_reconciler_core::HasValue
    for SettlementsHandle<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Eq + std::hash::Hash,
{
    type Value = Value;
}

impl<VmId, Value, RaisedException, ValueConverter>
    waymark_extcall_reconciler_core::HasRaisedException
    for SettlementsHandle<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Eq + std::hash::Hash,
{
    type RaisedException = RaisedException;
}

impl<VmId, Value, RaisedException, ValueConverter, UnifiedAck>
    waymark_extcall_reconciler_core::ActionPromiseSettler<UnifiedAck>
    for SettlementsHandle<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Copy + Eq + std::hash::Hash + Send + Sync + 'static,
    Value: Send,
    RaisedException: Send,
    waymark_action_runtime_convert::Converter<ValueConverter>: Convert<
            Result<ActionCallOutcome<Value, RaisedException>, ActionCallLossError>,
            PromiseResolution<Value, RaisedException>,
        >,
    UnifiedAck: From<Ack<VmId>>,
{
    type Error = PollActionSettlementsError;

    async fn poll_action_settlements<'a>(
        &'a mut self,
        waiting_promise_state_ids: NESlice<'a, PromiseStateId>,
    ) -> Result<NEVec<PromiseSettlement<Self::Value, Self::RaisedException, UnifiedAck>>, Self::Error>
    where
        UnifiedAck: 'a,
    {
        let matched = self
            .inner
            .wait_for_matching(waiting_promise_state_ids)
            .await
            .map_err(|registry::RegistryClosedError| PollActionSettlementsError::PollerGone)?;
        Ok(self.settle(matched))
    }
}

impl<VmId, Value, RaisedException, ValueConverter>
    SettlementsHandle<VmId, Value, RaisedException, ValueConverter>
where
    VmId: Copy + Eq + std::hash::Hash,
    waymark_action_runtime_convert::Converter<ValueConverter>: Convert<
            Result<ActionCallOutcome<Value, RaisedException>, ActionCallLossError>,
            PromiseResolution<Value, RaisedException>,
        >,
{
    /// Turn buffered completions into settlements with key-carrying acks.
    fn settle<UnifiedAck>(
        &self,
        completions: NEVec<BufferedCompletion<Value, RaisedException>>,
    ) -> NEVec<PromiseSettlement<Value, RaisedException, UnifiedAck>>
    where
        UnifiedAck: From<Ack<VmId>>,
    {
        completions
            .into_nonempty_iter()
            .map(|completion| {
                let BufferedCompletion {
                    promise_state_id,
                    execution_result,
                } = completion;

                let resolution =
                    waymark_action_runtime_convert::Converter::<ValueConverter>::convert(
                        execution_result,
                    );

                PromiseSettlement {
                    promise_state_id,
                    resolution,
                    ack: Ack {
                        key: CompletionKey {
                            vm_id: *self.inner.vm_id(),
                            promise_state_id,
                        },
                        ack_tx: self.inner.ack_sender().clone(),
                    }
                    .into(),
                }
            })
            .collect()
    }
}
