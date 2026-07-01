//! Core traits for extcall reconcilers.
//!
//! These traits abstract the handling and settlement of external calls
//! (action invocations and sleeps) emitted by the VM interpreter.
//! Implementations live in the concrete reconciler crates
//! ([`waymark_action_reconciler`], [`waymark_sleep_reconciler`]).

#![warn(missing_docs)]

use nonempty_collections::NEVec;
use waymark_action_core::ActionRef;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_driver_core::{PromiseSettlement, PromiseSettlementAck};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Settlement acknowledgement — aggregates action and sleep acks.
pub enum Ack<ActionAck, SleepAck> {
    /// Action-call settlement.
    Action(ActionAck),

    /// Sleep settlement.
    Sleep(SleepAck),
}

impl<ActionAck, SleepAck> PromiseSettlementAck for Ack<ActionAck, SleepAck>
where
    ActionAck: PromiseSettlementAck,
    SleepAck: PromiseSettlementAck,
{
    fn acknowledge_promise_settlement(self) {
        match self {
            Ack::Action(ack) => ack.acknowledge_promise_settlement(),
            Ack::Sleep(ack) => ack.acknowledge_promise_settlement(),
        }
    }
}

/// Handles action-call effects by dispatching them for execution.
pub trait ActionEffectHandler {
    /// The error type returned when dispatching fails.
    type Error;

    /// The type of a single argument passed to the action.
    type Argument;

    /// Request that an action be dispatched.
    fn request_action(
        &mut self,
        effect_number: EffectNumber,
        promise_state_id: PromiseStateId,
        action_ref: ActionRef,
        arguments: Vec<Self::Argument>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Handles sleep effects by recording wake deadlines.
pub trait SleepEffectHandler {
    /// Record a sleep deadline for the given promise.
    fn record_sleep(&mut self, promise_state_id: PromiseStateId, duration: NonZeroDuration);
}

/// Produces promise settlements from completed action calls.
pub trait ActionPromiseSettler {
    /// The type of a successful action result value.
    type Value;

    /// The acknowledgement type produced by this settler.
    type Ack;

    /// Poll for the next batch of action-completion settlements.
    fn poll_action_settlements<UnifiedAck>(
        &mut self,
    ) -> impl Future<Output = Option<NEVec<PromiseSettlement<Self::Value, UnifiedAck>>>> + Send
    where
        UnifiedAck: From<Self::Ack>;
}

/// Produces promise settlements from elapsed sleeps.
pub trait SleepPromiseSettler {
    /// The acknowledgement type produced by this settler.
    type Ack;

    /// Poll for the next batch of elapsed-sleep settlements.
    fn poll_sleep_settlements<UnifiedAck, Value>(
        &mut self,
    ) -> impl Future<Output = Option<NEVec<PromiseSettlement<Value, UnifiedAck>>>> + Send
    where
        Value: From<()>,
        UnifiedAck: From<Self::Ack>;
}
