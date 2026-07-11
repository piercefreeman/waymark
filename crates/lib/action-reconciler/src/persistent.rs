//! Persistent action call reconciliation.
//!
//! This module mirrors the crate-level [`Handler`](crate::Handler) /
//! [`Poller`](crate::Poller) pair, adding durable pending-call records via
//! a [`waymark_action_reconciler_backend`] backend so that action calls
//! survive a crash of the executing process:
//!
//! - [`Handler::request`] stores a pending-call record *before* dispatching,
//!   so a call can never be in flight without a durable record backing its
//!   re-dispatch.
//! - On the first poll of a session, [`Poller`] reconciles the stored
//!   records against the promises the VM is actually waiting on:
//!   still-waiting calls that this session did not dispatch are re-dispatched
//!   (they were in flight when the previous session died), while records for
//!   promises that are no longer waiting are stale (the settlement was
//!   persisted with the VM state but the removal acknowledgement was lost)
//!   and are removed.
//! - [`Ack`] removes the record once the driver has persisted the VM state
//!   that contains the settlement.
//!
//! Re-dispatch gives at-least-once execution semantics for actions: a call
//! whose completion did not make it into a persisted VM snapshot runs again
//! after a revive.

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use nonempty_collections::NEVec;
use waymark_action_core::ActionRef;
use waymark_action_reconciler_backend::PendingActionCall;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallOutcome, ActionCallRequest};
use waymark_action_runtime_metadata::{ActionCallCorrelated as _, ActionCallCorrelation};
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_promise_core::PromiseStateId;

/// The durable form of a dispatched action call, codec-encoded into the
/// pending-call record payload.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PersistedActionCall<Argument> {
    /// The action to invoke.
    pub action_ref: ActionRef,

    /// The arguments to pass to the action.
    pub arguments: Vec<Argument>,
}

/// The promises whose action calls were dispatched by this session, shared
/// between the [`Handler`] and the [`Poller`].
///
/// The [`Poller`]'s first-poll reconciliation uses this to tell a record
/// whose call is already in flight (dispatched by this session) from one
/// orphaned by a previous session. Once reconciliation completes the set is
/// taken out (`None`), releasing the memory — every later record is backed
/// by a live dispatch by construction.
type SessionDispatches = Arc<Mutex<Option<HashSet<PromiseStateId>>>>;

/// Settlement acknowledgement that removes the pending-call record.
///
/// [`PromiseSettlementAck::acknowledge_promise_settlement`] is called by
/// the driver after the VM state has been persisted — only then is the
/// record safe to remove.
///
/// [`PromiseSettlementAck::acknowledge_promise_settlement`]:
///     waymark_vm_driver_core::PromiseSettlementAck::acknowledge_promise_settlement
pub struct Ack<Backend>
where
    Backend: waymark_action_reconciler_backend::HasVmId,
{
    backend: Arc<Backend>,
    vm_id: Backend::VmId,
    promise_state_id: PromiseStateId,
}

impl<Backend> waymark_vm_driver_core::PromiseSettlementAck for Ack<Backend>
where
    Backend: waymark_action_reconciler_backend::RemovePendingActionCall + Send + Sync,
    Backend::VmId: Send + Sync,
{
    async fn acknowledge_promise_settlement(self) {
        // The settlement is already durably applied, so a removal failure
        // must not fail the driver; it merely leaves a stale record that
        // the next revive's reconciliation removes.
        let result = self
            .backend
            .remove_pending_action_call(&self.vm_id, self.promise_state_id)
            .await;
        if let Err(error) = result {
            tracing::warn!(
                ?error,
                promise_state_id = ?self.promise_state_id,
                "failed to remove the pending action call record of a settled promise"
            );
        }
    }
}

impl<Backend, SleepAck> From<Ack<Backend>>
    for waymark_extcall_reconciler_core::Ack<Ack<Backend>, SleepAck>
where
    Backend: waymark_action_reconciler_backend::HasVmId,
{
    fn from(value: Ack<Backend>) -> Self {
        waymark_extcall_reconciler_core::Ack::Action(value)
    }
}

/// Error returned when handling an action-call effect fails.
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError<CodecError, StoreError, ActionCallRequesterError> {
    /// Encoding the action call payload failed.
    #[error("encoding action call: {0:?}")]
    Codec(CodecError),

    /// The backend failed to store the pending-call record.
    #[error("storing pending action call: {0}")]
    StorePendingActionCall(#[source] StoreError),

    /// The action requester rejected the request.
    #[error("failed to request action call: {0}")]
    RequestActionCall(#[source] ActionCallRequesterError),
}

/// Error returned when polling for action settlements fails.
#[derive(Debug, thiserror::Error)]
pub enum PollError<ProviderError, LoadError, RemoveError, CodecError, ActionCallRequesterError> {
    /// Waiting for action call completions failed.
    #[error("waiting for completions: {0}")]
    Provider(#[source] ProviderError),

    /// The backend failed to load the pending action calls.
    #[error("loading pending action calls: {0}")]
    LoadPendingActionCalls(#[source] LoadError),

    /// The backend failed to remove a stale pending-call record.
    #[error("removing stale pending action call: {0}")]
    RemovePendingActionCall(#[source] RemoveError),

    /// Decoding a stored action call payload failed.
    #[error("decoding pending action call: {0:?}")]
    Codec(CodecError),

    /// Re-dispatching a pending action call failed.
    #[error("re-dispatching pending action call: {0}")]
    Redispatch(#[source] ActionCallRequesterError),
}

/// Dispatches action calls via an
/// [`ActionCallRequester`](waymark_action_runtime_core::ActionCallRequester),
/// durably recording each call before it goes out.
pub struct Handler<ActionCallRequester, Backend, Codec>
where
    Backend: waymark_action_reconciler_backend::HasVmId,
{
    requester: Arc<ActionCallRequester>,
    backend: Arc<Backend>,
    codec: Codec,
    vm_id: Backend::VmId,
    session_dispatches: SessionDispatches,
}

/// Polls an
/// [`ActionCallCompletionsProvider`](waymark_action_runtime_core::ActionCallCompletionsProvider)
/// and correlates outcomes with pending promise state IDs, reconciling the
/// durable pending-call records on the first poll of the session.
pub struct Poller<ActionCallCompletionsProvider, ActionCallRequester, Backend, Codec>
where
    Backend: waymark_action_reconciler_backend::HasVmId,
{
    provider: ActionCallCompletionsProvider,
    requester: Arc<ActionCallRequester>,
    backend: Arc<Backend>,
    codec: Codec,
    vm_id: Backend::VmId,
    session_dispatches: SessionDispatches,
}

/// The paired persistent action handler and poller created by [`new`].
pub type HandlerPollerPair<ActionCallRequester, ActionCallCompletionsProvider, Backend, Codec> = (
    Handler<ActionCallRequester, Backend, Codec>,
    Poller<ActionCallCompletionsProvider, ActionCallRequester, Backend, Codec>,
);

/// Create a paired persistent action handler and poller.
pub fn new<ActionCallRequester, ActionCallCompletionsProvider, Backend, Codec>(
    requester: ActionCallRequester,
    provider: ActionCallCompletionsProvider,
    backend: Arc<Backend>,
    codec: Codec,
    vm_id: Backend::VmId,
) -> HandlerPollerPair<ActionCallRequester, ActionCallCompletionsProvider, Backend, Codec>
where
    Backend: waymark_action_reconciler_backend::HasVmId,
    Backend::VmId: Clone,
    Codec: Clone,
{
    let requester = Arc::new(requester);
    let session_dispatches: SessionDispatches = Arc::new(Mutex::new(Some(HashSet::new())));
    let handler = Handler {
        requester: Arc::clone(&requester),
        backend: Arc::clone(&backend),
        codec: codec.clone(),
        vm_id: vm_id.clone(),
        session_dispatches: Arc::clone(&session_dispatches),
    };
    let poller = Poller {
        provider,
        requester,
        backend,
        codec,
        vm_id,
        session_dispatches,
    };
    (handler, poller)
}

impl<ActionCallRequester, Backend, Codec> Handler<ActionCallRequester, Backend, Codec>
where
    ActionCallRequester:
        waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>,
    ActionCallRequester::Argument: serde::Serialize,
    Backend: waymark_action_reconciler_backend::StorePendingActionCall,
    Codec: waymark_vm_codec_core::SerializerProvider,
{
    /// Durably record and dispatch an action call.
    pub async fn request(
        &self,
        effect_number: waymark_vm_runtime_effect::EffectNumber,
        promise_state_id: PromiseStateId,
        action_ref: ActionRef,
        arguments: Vec<ActionCallRequester::Argument>,
    ) -> Result<(), HandleEffectError<Codec::Error, Backend::Error, ActionCallRequester::Error>>
    {
        let correlation = ActionCallCorrelation {
            effect_number,
            promise_state_id,
        };

        let call = PersistedActionCall {
            action_ref,
            arguments,
        };
        let mut payload = Vec::new();
        self.codec
            .with_serializer(&mut payload, |serializer| {
                serde::Serialize::serialize(&call, serializer)
            })
            .map_err(HandleEffectError::Codec)?;

        // Store before dispatching, so a call can never be in flight
        // without a durable record backing its re-dispatch. A crash between
        // the store and the VM snapshot that captures this effect leaves a
        // record that deterministic re-execution overwrites with the same
        // value under the same key.
        self.backend
            .store_pending_action_call(&self.vm_id, correlation, &payload)
            .await
            .map_err(HandleEffectError::StorePendingActionCall)?;

        {
            let mut guard = self
                .session_dispatches
                .lock()
                .expect("session dispatches poisoned");
            if let Some(session_dispatches) = guard.as_mut() {
                session_dispatches.insert(promise_state_id);
            }
        }

        let PersistedActionCall {
            action_ref,
            arguments,
        } = call;
        let request = ActionCallRequest {
            action_ref,
            metadata: correlation,
            arguments,
        };
        self.requester
            .request_action_call(request)
            .await
            .map_err(HandleEffectError::RequestActionCall)?;

        Ok(())
    }
}

impl<ActionCallCompletionsProvider, ActionCallRequester, Backend, Codec>
    Poller<ActionCallCompletionsProvider, ActionCallRequester, Backend, Codec>
where
    ActionCallCompletionsProvider: waymark_action_runtime_core::ActionCallCompletionsProvider,
    ActionCallCompletionsProvider::Metadata: waymark_action_runtime_metadata::ActionCallCorrelated,
    ActionCallRequester:
        waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>,
    ActionCallRequester::Argument: serde::de::DeserializeOwned,
    Backend: waymark_action_reconciler_backend::LoadPendingActionCalls,
    Backend: waymark_action_reconciler_backend::RemovePendingActionCall,
    Backend::VmId: Clone,
    Codec: waymark_vm_codec_core::DeserializerProvider,
{
    /// Wait for the next batch of action-completion settlements.
    ///
    /// On the first call of the session this first reconciles the durable
    /// pending-call records against `waiting_promise_state_ids`.
    pub async fn poll<Ack>(
        &mut self,
        waiting_promise_state_ids: &NEVec<PromiseStateId>,
    ) -> Result<
        NEVec<PromiseSettlement<ActionCallCompletionsProvider::Value, Ack>>,
        PollError<
            ActionCallCompletionsProvider::Error,
            <Backend as waymark_action_reconciler_backend::LoadPendingActionCalls>::Error,
            <Backend as waymark_action_reconciler_backend::RemovePendingActionCall>::Error,
            Codec::Error,
            ActionCallRequester::Error,
        >,
    >
    where
        Ack: From<self::Ack<Backend>>,
    {
        self.reconcile(waiting_promise_state_ids).await?;

        loop {
            let completions = self
                .provider
                .wait_for_completions()
                .await
                .map_err(PollError::Provider)?;

            let mut settlements = Vec::new();
            for completion in completions {
                let ActionCallCompletion { metadata, outcome } = completion;
                let ActionCallCorrelation {
                    promise_state_id, ..
                } = metadata.call_correlation();

                if !waiting_promise_state_ids.contains(&promise_state_id) {
                    // The VM is not waiting on this promise, so settling it
                    // would fail the driver. This is a duplicate completion —
                    // the settlement was already applied and persisted. Drop
                    // it; any stale record is removed by the next revive's
                    // reconciliation.
                    tracing::warn!(
                        ?promise_state_id,
                        "dropping an action call completion for a promise that is not waiting"
                    );
                    continue;
                }

                let resolution = match outcome {
                    ActionCallOutcome::Value(value) => PromiseResolution::Resolved(value),
                    ActionCallOutcome::Exception(exception) => {
                        PromiseResolution::Rejected(exception)
                    }
                };

                settlements.push(PromiseSettlement {
                    promise_state_id,
                    resolution,
                    ack: Ack::from(self::Ack {
                        backend: Arc::clone(&self.backend),
                        vm_id: self.vm_id.clone(),
                        promise_state_id,
                    }),
                });
            }

            // Every completion of the batch may have been dropped as
            // duplicate — wait for the next batch instead of returning an
            // empty settlement list.
            if let Some(settlements) = NEVec::try_from_vec(settlements) {
                return Ok(settlements);
            }
        }
    }

    /// Reconcile the durable pending-call records against the promises the
    /// VM is currently waiting on. Runs fully once per session.
    ///
    /// This method is cancel-safe: the session is only marked reconciled
    /// after a complete pass, and every step of a partial pass (stale-record
    /// removal, re-dispatch recorded in the session dispatches) is one the
    /// next attempt observes and does not repeat.
    async fn reconcile(
        &mut self,
        waiting_promise_state_ids: &NEVec<PromiseStateId>,
    ) -> Result<
        (),
        PollError<
            ActionCallCompletionsProvider::Error,
            <Backend as waymark_action_reconciler_backend::LoadPendingActionCalls>::Error,
            <Backend as waymark_action_reconciler_backend::RemovePendingActionCall>::Error,
            Codec::Error,
            ActionCallRequester::Error,
        >,
    > {
        {
            let guard = self
                .session_dispatches
                .lock()
                .expect("session dispatches poisoned");
            if guard.is_none() {
                return Ok(());
            }
        }

        let pending = self
            .backend
            .load_pending_action_calls(&self.vm_id)
            .await
            .map_err(PollError::LoadPendingActionCalls)?;

        for record in pending {
            let PendingActionCall {
                correlation,
                payload,
            } = record;
            let promise_state_id = correlation.promise_state_id;

            if !waiting_promise_state_ids.contains(&promise_state_id) {
                // The promise settled and the settlement was persisted with
                // the VM state, but the record removal was lost.
                tracing::debug!(?promise_state_id, "removing a stale pending action call");
                self.backend
                    .remove_pending_action_call(&self.vm_id, promise_state_id)
                    .await
                    .map_err(PollError::RemovePendingActionCall)?;
                continue;
            }

            {
                let guard = self
                    .session_dispatches
                    .lock()
                    .expect("session dispatches poisoned");
                let session_dispatches = guard
                    .as_ref()
                    .expect("session dispatches taken while reconciliation is incomplete");
                if session_dispatches.contains(&promise_state_id) {
                    // The call is already in flight — this session
                    // dispatched (or re-dispatched) it.
                    continue;
                }
            }

            // The call was in flight when the previous session died —
            // re-dispatch it.
            tracing::info!(?promise_state_id, "re-dispatching a pending action call");
            let call: PersistedActionCall<ActionCallRequester::Argument> = self
                .codec
                .with_deserializer(&payload, |deserializer| {
                    serde::Deserialize::deserialize(deserializer)
                })
                .map_err(PollError::Codec)?;
            let request = ActionCallRequest {
                action_ref: call.action_ref,
                metadata: correlation,
                arguments: call.arguments,
            };
            self.requester
                .request_action_call(request)
                .await
                .map_err(PollError::Redispatch)?;

            let mut guard = self
                .session_dispatches
                .lock()
                .expect("session dispatches poisoned");
            let session_dispatches = guard
                .as_mut()
                .expect("session dispatches taken while reconciliation is incomplete");
            session_dispatches.insert(promise_state_id);
        }

        // A complete pass was made — mark the session reconciled and release
        // the dispatch-tracking memory.
        let mut guard = self
            .session_dispatches
            .lock()
            .expect("session dispatches poisoned");
        *guard = None;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// extcall-reconciler-core trait impls
// ---------------------------------------------------------------------------

impl<ActionCallRequester, Backend, Codec> waymark_extcall_reconciler_core::ActionEffectHandler
    for Handler<ActionCallRequester, Backend, Codec>
where
    ActionCallRequester:
        waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>,
    ActionCallRequester: Send + Sync,
    ActionCallRequester::Argument: serde::Serialize + Send,
    Backend: waymark_action_reconciler_backend::StorePendingActionCall + Send + Sync,
    Backend::VmId: Send + Sync,
    Codec: waymark_vm_codec_core::SerializerProvider + Send + Sync,
{
    type Error = HandleEffectError<Codec::Error, Backend::Error, ActionCallRequester::Error>;
    type Argument = ActionCallRequester::Argument;

    async fn request_action(
        &mut self,
        effect_number: waymark_vm_runtime_effect::EffectNumber,
        promise_state_id: PromiseStateId,
        action_ref: ActionRef,
        arguments: Vec<Self::Argument>,
    ) -> Result<(), Self::Error> {
        self.request(effect_number, promise_state_id, action_ref, arguments)
            .await
    }
}

impl<ActionCallCompletionsProvider, ActionCallRequester, Backend, Codec>
    waymark_extcall_reconciler_core::ActionPromiseSettler
    for Poller<ActionCallCompletionsProvider, ActionCallRequester, Backend, Codec>
where
    ActionCallCompletionsProvider:
        waymark_action_runtime_core::ActionCallCompletionsProvider + Send + Sync,
    ActionCallCompletionsProvider::Metadata: waymark_action_runtime_metadata::ActionCallCorrelated,
    ActionCallRequester:
        waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>,
    ActionCallRequester: Send + Sync,
    ActionCallRequester::Argument: serde::de::DeserializeOwned + Send,
    Backend: waymark_action_reconciler_backend::LoadPendingActionCalls,
    Backend: waymark_action_reconciler_backend::RemovePendingActionCall,
    Backend: Send + Sync,
    Backend::VmId: Clone + Send + Sync,
    Codec: waymark_vm_codec_core::DeserializerProvider + Send + Sync,
{
    type Value = ActionCallCompletionsProvider::Value;
    type Error = PollError<
        ActionCallCompletionsProvider::Error,
        <Backend as waymark_action_reconciler_backend::LoadPendingActionCalls>::Error,
        <Backend as waymark_action_reconciler_backend::RemovePendingActionCall>::Error,
        Codec::Error,
        ActionCallRequester::Error,
    >;
    type Ack = Ack<Backend>;

    async fn poll_action_settlements<UnifiedAck>(
        &mut self,
        waiting_promise_state_ids: &NEVec<PromiseStateId>,
    ) -> Result<NEVec<PromiseSettlement<Self::Value, UnifiedAck>>, Self::Error>
    where
        UnifiedAck: From<Self::Ack>,
    {
        self.poll::<UnifiedAck>(waiting_promise_state_ids).await
    }
}
