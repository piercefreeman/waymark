//! Support for running extcalls emitted by the extcall interpreter.
//!
//! This crate handles the VM extcall interpreter's effects
//! ([`waymark_vm_interpreter_extcallset::Effect`]) by delegating to
//! pluggable [`waymark_extcall_reconciler_core::ActionEffectHandler`],
//! [`waymark_extcall_reconciler_core::SleepEffectHandler`],
//! [`waymark_extcall_reconciler_core::ActionPromiseSettler`], and
//! [`waymark_extcall_reconciler_core::SleepPromiseSettler`] implementations.
//!
//! # To do
//!
//! - Retries / Timeouts / Persistence
//!   See the concrete reconciler crates.

#![warn(missing_docs)]

#[cfg(test)]
mod tests;

use nonempty_collections::NEVec;
use waymark_action_core::ActionRef;
use waymark_extcall_reconciler_core::Ack;
use waymark_vm_driver_core::{PromiseSettlement, PromiseSettlementAck};
use waymark_vm_interpreter_extcallset::Effect;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Error returned when handling an extcall effect fails.
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError<ActionEffectHandlerError, SleepEffectHandlerError> {
    /// The action effect handler rejected the request.
    #[error("failed to request action call: {0}")]
    Action(#[source] ActionEffectHandlerError),

    /// The sleep effect handler failed to record the sleep.
    #[error("failed to record sleep: {0}")]
    Sleep(#[source] SleepEffectHandlerError),
}

/// Error returned when [`PromiseSettler::get_promise_settlements`] fails.
#[derive(Debug, thiserror::Error)]
pub enum GetPromiseSettlementsError<ActionError, SleepError> {
    /// The action promise settler failed while polling for settlements.
    #[error("action: {0}")]
    Action(ActionError),

    /// The sleep promise settler failed while polling for settlements.
    #[error("sleep: {0}")]
    Sleep(SleepError),
}

/// Handles extcall effects emitted by the VM — dispatches actions and
/// records sleep deadlines.
pub struct EffectHandler<ActionHandler, SleepHandler> {
    /// Action effect handler — dispatches action calls to workers.
    pub action: ActionHandler,

    /// Sleep effect handler — records sleep deadlines.
    pub sleep: SleepHandler,
}

/// Produces promise settlements from completed actions and elapsed
/// sleeps.
pub struct PromiseSettler<ActionSettler, SleepSettler> {
    /// Action promise settler — polls for completed action settlements.
    pub action: ActionSettler,

    /// Sleep promise settler — polls for elapsed sleep settlements.
    pub sleep: SleepSettler,
}

/// Create a paired handler and settler from pre-built reconciler pairs.
pub fn new<ActionHandler, SleepHandler, ActionSettler, SleepSettler>(
    action_handler: ActionHandler,
    sleep_handler: SleepHandler,
    action_poller: ActionSettler,
    sleep_poller: SleepSettler,
) -> (
    EffectHandler<ActionHandler, SleepHandler>,
    PromiseSettler<ActionSettler, SleepSettler>,
) {
    let handler = EffectHandler {
        action: action_handler,
        sleep: sleep_handler,
    };
    let settler = PromiseSettler {
        action: action_poller,
        sleep: sleep_poller,
    };
    (handler, settler)
}

impl<ActionHandler, SleepHandler> waymark_vm_driver_core::EffectHandler
    for EffectHandler<ActionHandler, SleepHandler>
where
    ActionHandler: waymark_extcall_reconciler_core::ActionEffectHandler + Send,
    ActionHandler::Error: core::fmt::Debug,
    ActionHandler::Argument: Send,
    SleepHandler: waymark_extcall_reconciler_core::SleepEffectHandler + Send,
{
    type Effect = Effect<ActionRef, ActionHandler::Argument>;
    type Error = HandleEffectError<ActionHandler::Error, SleepHandler::Error>;

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
                    .request_action(emitted_effect.number, promise_state_id, action_ref, args)
                    .await
                    .map_err(HandleEffectError::Action)?;
            }
            Effect::Sleep {
                promise_state_id,
                duration,
            } => {
                self.sleep
                    .record_sleep(emitted_effect.number, promise_state_id, duration)
                    .await
                    .map_err(HandleEffectError::Sleep)?;
            }
        }

        Ok(())
    }
}

impl<ActionSettler, SleepSettler> waymark_vm_driver_core::PromiseSettler
    for PromiseSettler<ActionSettler, SleepSettler>
where
    ActionSettler: waymark_extcall_reconciler_core::SettlerAck + Send,
    SleepSettler: waymark_extcall_reconciler_core::SettlerAck + Send,
    ActionSettler: waymark_extcall_reconciler_core::ActionPromiseSettler<
            Ack<
                <ActionSettler as waymark_extcall_reconciler_core::SettlerAck>::Ack,
                <SleepSettler as waymark_extcall_reconciler_core::SettlerAck>::Ack,
            >,
        >,
    SleepSettler: waymark_extcall_reconciler_core::SleepPromiseSettler<
            Ack<
                <ActionSettler as waymark_extcall_reconciler_core::SettlerAck>::Ack,
                <SleepSettler as waymark_extcall_reconciler_core::SettlerAck>::Ack,
            >,
        >,
    SleepSettler: waymark_extcall_reconciler_core::HasValue<Value = ActionSettler::Value>,
    ActionSettler::Ack: PromiseSettlementAck,
    SleepSettler::Ack: PromiseSettlementAck,
    Ack<ActionSettler::Ack, SleepSettler::Ack>: From<ActionSettler::Ack>,
    Ack<ActionSettler::Ack, SleepSettler::Ack>: From<SleepSettler::Ack>,
{
    type Value = ActionSettler::Value;
    type Error = GetPromiseSettlementsError<ActionSettler::Error, SleepSettler::Error>;
    type Ack = Ack<ActionSettler::Ack, SleepSettler::Ack>;

    async fn get_promise_settlements(
        &mut self,
        waiting_ids: NEVec<PromiseStateId>,
    ) -> Result<NEVec<PromiseSettlement<Self::Value, Self::Ack>>, Self::Error> {
        tokio::select! {
            settlements = self.sleep.poll_sleep_settlements(waiting_ids.as_nonempty_slice()) => {
                settlements.map_err(GetPromiseSettlementsError::Sleep)
            }
            settlements = self.action.poll_action_settlements(waiting_ids.as_nonempty_slice()) => {
                settlements.map_err(GetPromiseSettlementsError::Action)
            }
        }
    }
}
