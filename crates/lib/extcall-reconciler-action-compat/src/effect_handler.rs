//! Action effect handling over an action-call requester.

use waymark_action_core::ActionRef;
use waymark_action_runtime_core::ActionCallRequest;
use waymark_action_runtime_metadata::ActionCallCorrelation;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Error returned when dispatching an action call fails.
#[derive(Debug, thiserror::Error)]
pub enum Error<ActionCallRequesterError> {
    /// The action requester rejected the request.
    #[error("failed to request action call: {0}")]
    RequestActionCall(#[source] ActionCallRequesterError),
}

/// Dispatches action-call effects via an
/// [`waymark_action_runtime_core::ActionCallRequester`].
///
/// Implements [`waymark_extcall_reconciler_core::ActionEffectHandler`]:
/// each effect is stamped with its [`ActionCallCorrelation`] and handed to
/// the requester.
pub struct EffectHandler<ActionCallRequester> {
    requester: ActionCallRequester,
}

impl<ActionCallRequester> EffectHandler<ActionCallRequester> {
    /// Create an effect handler dispatching via the given requester.
    pub fn new(requester: ActionCallRequester) -> Self {
        Self { requester }
    }
}

impl<ActionCallRequester> waymark_extcall_reconciler_core::ActionEffectHandler
    for EffectHandler<ActionCallRequester>
where
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>
        + Send
        + Sync,
    ActionCallRequester::Argument: Send,
{
    type Error = Error<ActionCallRequester::Error>;
    type Argument = ActionCallRequester::Argument;

    async fn request_action(
        &mut self,
        effect_number: EffectNumber,
        promise_state_id: PromiseStateId,
        action_ref: ActionRef,
        arguments: Vec<Self::Argument>,
    ) -> Result<(), Self::Error> {
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
            .map_err(Error::RequestActionCall)
    }
}
