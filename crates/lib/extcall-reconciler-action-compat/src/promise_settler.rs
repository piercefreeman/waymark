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
/// [`Ack`]s.  A completion whose execution failed to produce an outcome
/// settles its promise raised, with `Converter` stating how the
/// provider's execution error converts to the raising exception.
pub struct PromiseSettler<ActionCallCompletionsProvider, Converter> {
    provider: ActionCallCompletionsProvider,
    _converter: core::marker::PhantomData<Converter>,
}

impl<ActionCallCompletionsProvider, Converter>
    PromiseSettler<ActionCallCompletionsProvider, Converter>
{
    /// Create a settler over the given completions provider.
    pub fn new(provider: ActionCallCompletionsProvider) -> Self {
        Self {
            provider,
            _converter: core::marker::PhantomData,
        }
    }
}

impl<ActionCallCompletionsProvider, Converter> waymark_extcall_reconciler_core::SettlerAck
    for PromiseSettler<ActionCallCompletionsProvider, Converter>
{
    type Ack = Ack;
}

impl<ActionCallCompletionsProvider, Converter> waymark_extcall_reconciler_core::HasValue
    for PromiseSettler<ActionCallCompletionsProvider, Converter>
where
    ActionCallCompletionsProvider: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    type Value = ActionCallCompletionsProvider::Value;
}

impl<ActionCallCompletionsProvider, Converter, UnifiedAck>
    waymark_extcall_reconciler_core::ActionPromiseSettler<UnifiedAck>
    for PromiseSettler<ActionCallCompletionsProvider, Converter>
where
    ActionCallCompletionsProvider:
        waymark_action_runtime_core::ActionCallCompletionsProvider + Send + Sync,
    ActionCallCompletionsProvider::Metadata: ActionCallCorrelated,
    Converter: waymark_convert_core::Convert<
            ActionCallCompletionsProvider::ActionExecutionError,
            waymark_vm_runtime_exception::Exception<ActionCallCompletionsProvider::Value>,
        > + Send
        + Sync,
    UnifiedAck: From<Ack>,
{
    type Error = ActionCallCompletionsProvider::WaitError;

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
                let ActionCallCompletion {
                    metadata,
                    execution_result,
                } = completion;
                let ActionCallCorrelation {
                    promise_state_id, ..
                } = metadata.call_correlation();

                let resolution = match execution_result {
                    Ok(ActionCallOutcome::Value(value)) => PromiseResolution::Resolved(value),
                    Ok(ActionCallOutcome::Exception(exception)) => {
                        PromiseResolution::Rejected(exception)
                    }
                    // The execution produced no outcome; the promise
                    // settles raised with the error's exception rendering.
                    Err(execution_error) => {
                        PromiseResolution::Rejected(Converter::convert(execution_error))
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
