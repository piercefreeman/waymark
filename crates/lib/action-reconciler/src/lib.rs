//! Action call reconciler — dispatches action calls via an
//! [`ActionCallRequester`](waymark_action_runtime_core::ActionCallRequester)
//! and correlates completions from an
//! [`ActionCallCompletionsProvider`](waymark_action_runtime_core::ActionCallCompletionsProvider)
//! into promise settlements.

#![warn(missing_docs)]

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use waymark_action_core::ActionRef;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallOutcome, ActionCallRequest};
use waymark_action_runtime_metadata::{ActionCallCorrelated, ActionCallCorrelation};
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Settlement acknowledgement for action-call settlements.
///
/// [`PromiseSettlementAck::acknowledge_promise_settlement`] is called by
/// the driver after the VM state has been persisted.
pub struct Ack;

impl waymark_vm_driver_core::PromiseSettlementAck for Ack {
    fn acknowledge_promise_settlement(self) {}
}

impl<SleepAck> From<Ack> for waymark_extcall_reconciler_core::Ack<Ack, SleepAck> {
    fn from(value: Ack) -> Self {
        waymark_extcall_reconciler_core::Ack::Action(value)
    }
}

/// Error returned when handling an action-call effect fails.
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError<ActionCallRequesterError> {
    /// The action requester rejected the request.
    #[error("failed to request action call: {0}")]
    RequestActionCall(#[source] ActionCallRequesterError),
}

/// Dispatches action calls via an [`ActionCallRequester`].
pub struct Handler<ActionCallRequester> {
    requester: ActionCallRequester,
}

/// Polls an [`ActionCallCompletionsProvider`] and correlates outcomes with
/// pending promise state IDs.
pub struct Poller<ActionCallCompletionsProvider> {
    provider: ActionCallCompletionsProvider,
}

impl<ActionCallRequester> Handler<ActionCallRequester>
where
    ActionCallRequester:
        waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>,
{
    /// Dispatch an action call.
    pub async fn request(
        &self,
        effect_number: waymark_vm_runtime_effect::EffectNumber,
        promise_state_id: PromiseStateId,
        action_ref: ActionRef,
        arguments: Vec<ActionCallRequester::Argument>,
    ) -> Result<(), HandleEffectError<ActionCallRequester::Error>> {
        let request = ActionCallRequest {
            action_ref,
            metadata: ActionCallCorrelation {
                effect_number,
                promise_state_id,
            },
            arguments,
        };

        self.requester
            .request_action_call(request)
            .await
            .map_err(HandleEffectError::RequestActionCall)?;

        Ok(())
    }
}

impl<ActionCallCompletionsProvider> Poller<ActionCallCompletionsProvider>
where
    ActionCallCompletionsProvider: waymark_action_runtime_core::ActionCallCompletionsProvider,
    ActionCallCompletionsProvider::Metadata: ActionCallCorrelated,
{
    /// Wait for the next batch of action-completion settlements.
    pub async fn poll<Ack>(
        &mut self,
    ) -> Result<
        NEVec<PromiseSettlement<ActionCallCompletionsProvider::Value, Ack>>,
        ActionCallCompletionsProvider::Error,
    >
    where
        Ack: From<self::Ack>,
    {
        let completions = self.provider.wait_for_completions().await?;

        let settlements: NEVec<_> = completions
            .into_nonempty_iter()
            .map(|completion| {
                let ActionCallCompletion { metadata, outcome } = completion;
                let ActionCallCorrelation {
                    promise_state_id, ..
                } = metadata.call_correlation();

                let resolution = match outcome {
                    ActionCallOutcome::Value(value) => PromiseResolution::Resolved(value),
                    ActionCallOutcome::Exception(exception) => {
                        PromiseResolution::Rejected(exception)
                    }
                };

                PromiseSettlement {
                    promise_state_id,
                    resolution,
                    ack: Ack.into(),
                }
            })
            .collect();

        Ok(settlements)
    }
}

/// Create a paired action handler and poller.
pub fn new<ActionCallRequester, ActionCallCompletionsProvider>(
    requester: ActionCallRequester,
    provider: ActionCallCompletionsProvider,
) -> (
    Handler<ActionCallRequester>,
    Poller<ActionCallCompletionsProvider>,
)
where
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester,
    ActionCallCompletionsProvider: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    let handler = Handler { requester };
    let poller = Poller { provider };
    (handler, poller)
}

// ---------------------------------------------------------------------------
// extcall-reconciler-core trait impls
// ---------------------------------------------------------------------------

impl<Requester> waymark_extcall_reconciler_core::ActionEffectHandler for Handler<Requester>
where
    Requester: waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>
        + Send
        + Sync,
    Requester::Argument: Send,
{
    type Error = HandleEffectError<Requester::Error>;
    type Argument = Requester::Argument;

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

impl<Provider> waymark_extcall_reconciler_core::SettlerAck for Poller<Provider> {
    type Ack = Ack;
}

impl<Provider, UnifiedAck> waymark_extcall_reconciler_core::ActionPromiseSettler<UnifiedAck>
    for Poller<Provider>
where
    Provider: waymark_action_runtime_core::ActionCallCompletionsProvider + Send + Sync,
    Provider::Metadata: ActionCallCorrelated,
    UnifiedAck: From<Ack>,
{
    type Value = Provider::Value;
    type Error = Provider::Error;

    async fn poll_action_settlements(
        &mut self,
    ) -> Result<NEVec<PromiseSettlement<Self::Value, UnifiedAck>>, Self::Error> {
        self.poll::<UnifiedAck>().await
    }
}
