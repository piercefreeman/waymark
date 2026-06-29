//! Action call extcall reconciler — requests action calls and correlates
//! completions into promise settlements.

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use waymark_action_core::ActionRef;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallOutcome, ActionCallRequest};
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{Ack, HandleEffectError};

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
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester,
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
            effect_number,
            promise_state_id,
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
{
    /// Wait for the next batch of action-completion settlements.
    pub async fn poll(
        &mut self,
    ) -> Option<NEVec<PromiseSettlement<ActionCallCompletionsProvider::Value, Ack>>> {
        let completions = self.provider.wait_for_completions().await.ok()?;

        let settlements: NEVec<_> = completions
            .into_nonempty_iter()
            .map(|completion| {
                let ActionCallCompletion {
                    effect_number: _,
                    promise_state_id,
                    outcome,
                } = completion;

                let resolution = match outcome {
                    ActionCallOutcome::Value(value) => PromiseResolution::Resolved(value),
                    ActionCallOutcome::Exception(exception) => {
                        PromiseResolution::Rejected(exception)
                    }
                };

                PromiseSettlement {
                    promise_state_id,
                    resolution,
                    ack: Ack::Action,
                }
            })
            .collect();

        Some(settlements)
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
