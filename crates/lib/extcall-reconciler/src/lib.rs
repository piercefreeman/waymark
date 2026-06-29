//! Support for running extcalls emitted by the extcall interpreter.
//!
//! This crate bridges the VM extcall interpreter's effects
//! ([`waymark_vm_interpreter_extcallset::Effect`]) with
//! [`waymark_action_runtime_core::ActionCallRequester`] and
//! [`waymark_action_runtime_core::ActionCallCompletionsProvider`] implementations.

#![warn(missing_docs)]

mod action_call;
mod sleep;

#[cfg(test)]
mod tests;

use std::fmt::Debug;

use nonempty_collections::NEVec;
use waymark_action_core::ActionRef;
use waymark_vm_driver_core::{PromiseSettlement, PromiseSettlementAck};
use waymark_vm_interpreter_extcallset::Effect;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Settlement acknowledgement.
///
/// This is a dummy implementartion for now, see crate-level doc comment for
/// more info.
///
/// [`PromiseSettlementAck::acknowledge_promise_settlement`] is called by
/// the driver after the VM state has been persisted.
pub enum Ack {
    /// Action-call settlement.
    Action,

    /// Sleep settlement.
    Sleep,
}

impl PromiseSettlementAck for Ack {
    fn acknowledge_promise_settlement(self) {
        match self {
            Ack::Action => {}
            Ack::Sleep => {}
        }
    }
}

/// Error returned when there are no pending promises to wait for.
#[derive(Debug, thiserror::Error)]
pub enum NoSettlementsError {
    /// No pending actions or sleeps remain - nothing to wait for.
    #[error("no pending promises to settle")]
    NoPendingPromises,
}

/// Error returned when handling an extcall effect fails.
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError<ActionCallRequesterError> {
    /// The action requester rejected the request.
    #[error("failed to request action call: {0}")]
    RequestActionCall(#[source] ActionCallRequesterError),
}

/// Handles extcall effects emitted by the VM — dispatches actions and
/// records sleep deadlines.
pub struct EffectHandler<VmId, ActionCallRequester> {
    vm_id: VmId,
    action: action_call::Handler<ActionCallRequester>,
    sleep: sleep::Handler<VmId>,
}

/// Produces promise settlements from completed actions and elapsed
/// sleeps.
pub struct PromiseSettler<VmId, ActionCallCompletionsProvider> {
    action: action_call::Poller<ActionCallCompletionsProvider>,
    sleep: sleep::Poller<VmId>,
}

/// Create a paired handler and settler from an action requester and
/// outcomes provider.
pub fn new<VmId, ActionCallRequester, ActionCallCompletionsProvider>(
    vm_id: VmId,
    requester: ActionCallRequester,
    provider: ActionCallCompletionsProvider,
) -> (
    EffectHandler<VmId, ActionCallRequester>,
    PromiseSettler<VmId, ActionCallCompletionsProvider>,
)
where
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester,
    ActionCallCompletionsProvider: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    let (action_handler, action_poller) = action_call::new(requester, provider);
    let (sleep_handler, sleep_poller) = sleep::new();
    let handler = EffectHandler {
        vm_id,
        action: action_handler,
        sleep: sleep_handler,
    };
    let settler = PromiseSettler {
        action: action_poller,
        sleep: sleep_poller,
    };
    (handler, settler)
}

impl<VmId, ActionCallRequester> waymark_vm_driver_core::EffectHandler
    for EffectHandler<VmId, ActionCallRequester>
where
    VmId: Send + Clone,
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester,
    ActionCallRequester: Send + Sync,
    ActionCallRequester::Error: core::fmt::Debug,
    ActionCallRequester::Argument: Send,
{
    type Effect = Effect<ActionRef, ActionCallRequester::Argument>;
    type Error = HandleEffectError<ActionCallRequester::Error>;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        match emitted_effect.effect {
            Effect::ActionCall {
                promise_state_id,
                action_ref,
                args,
            } => {
                self.action
                    .request(emitted_effect.number, promise_state_id, action_ref, args)
                    .await?;
            }
            Effect::Sleep {
                promise_state_id,
                duration,
            } => {
                self.sleep
                    .record(self.vm_id.clone(), promise_state_id, duration);
            }
        }

        Ok(())
    }
}

impl<VmId, ActionCallCompletionsProvider> waymark_vm_driver_core::PromiseSettler
    for PromiseSettler<VmId, ActionCallCompletionsProvider>
where
    VmId: Send + core::fmt::Debug,
    ActionCallCompletionsProvider:
        waymark_action_runtime_core::ActionCallCompletionsProvider + Send,
    ActionCallCompletionsProvider::Value: From<()>,
{
    type Value = ActionCallCompletionsProvider::Value;
    type Error = NoSettlementsError;
    type Ack = Ack;

    async fn get_promise_settlements(
        &mut self,
        _waiting_ids: NEVec<PromiseStateId>,
    ) -> Result<NEVec<PromiseSettlement<Self::Value, Self::Ack>>, Self::Error> {
        Ok(tokio::select! {
            Some(settlements) = self.sleep.poll::<Self::Value>() => settlements,
            Some(settlements) = self.action.poll() => settlements,
            else => return Err(NoSettlementsError::NoPendingPromises),
        })
    }
}
