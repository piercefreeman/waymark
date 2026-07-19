//! Promise settling over an action-call completions provider.

use nonempty_collections::{IntoNonEmptyIterator as _, NESlice, NEVec, NonEmptyIterator as _};
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallOutcome};
use waymark_action_runtime_metadata::{ActionCallCorrelated, ActionCallCorrelation};
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Settlement acknowledgement for provider-backed settlements.
///
/// A no-op: the underlying completion sources have nothing to reclaim or
/// confirm once a settlement has been applied.
pub struct Ack;

impl waymark_vm_driver_core::PromiseSettlementAck for Ack {
    fn acknowledge_promise_settlement(self) {}
}

impl<SleepAck> From<Ack> for waymark_extcall_reconciler_core::Ack<Ack, SleepAck> {
    fn from(value: Ack) -> Self {
        waymark_extcall_reconciler_core::Ack::Action(value)
    }
}

/// Settles promises from an
/// [`waymark_action_runtime_core::ActionCallCompletionsProvider`].
///
/// Implements [`waymark_extcall_reconciler_core::ActionPromiseSettler`]:
/// completions are correlated into promise settlements carrying no-op
/// [`Ack`]s.
pub struct PromiseSettler<ActionCallCompletionsProvider> {
    provider: ActionCallCompletionsProvider,
}

impl<ActionCallCompletionsProvider> PromiseSettler<ActionCallCompletionsProvider> {
    /// Create a settler over the given completions provider.
    pub fn new(provider: ActionCallCompletionsProvider) -> Self {
        Self { provider }
    }
}

impl<ActionCallCompletionsProvider> waymark_extcall_reconciler_core::SettlerAck
    for PromiseSettler<ActionCallCompletionsProvider>
{
    type Ack = Ack;
}

impl<ActionCallCompletionsProvider> waymark_extcall_reconciler_core::HasValue
    for PromiseSettler<ActionCallCompletionsProvider>
where
    ActionCallCompletionsProvider: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    type Value = ActionCallCompletionsProvider::Value;
}

impl<ActionCallCompletionsProvider, UnifiedAck>
    waymark_extcall_reconciler_core::ActionPromiseSettler<UnifiedAck>
    for PromiseSettler<ActionCallCompletionsProvider>
where
    ActionCallCompletionsProvider:
        waymark_action_runtime_core::ActionCallCompletionsProvider + Send + Sync,
    ActionCallCompletionsProvider::Metadata: ActionCallCorrelated,
    UnifiedAck: From<Ack>,
{
    type Error = ActionCallCompletionsProvider::Error;

    async fn poll_action_settlements<'a>(
        &'a mut self,
        // The provider yields everything it has; the demand set is not
        // consulted.
        _waiting_promise_state_ids: NESlice<'a, PromiseStateId>,
    ) -> Result<NEVec<PromiseSettlement<Self::Value, UnifiedAck>>, Self::Error>
    where
        UnifiedAck: 'a,
    {
        let completions = self.provider.wait_for_completions().await?;

        let settlements = completions
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
