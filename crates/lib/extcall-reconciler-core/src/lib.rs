//! Core traits for extcall reconcilers.
//!
//! These traits abstract the handling and settlement of external calls
//! (action invocations and sleeps) emitted by the VM interpreter.
//! Implementations live in the concrete reconciler crates.

#![warn(missing_docs)]

use nonempty_collections::{NESlice, NEVec};
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
    type Error: std::fmt::Debug;

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
    /// The error type returned when recording a sleep fails.
    type Error: std::fmt::Debug;

    /// Record a sleep deadline for the given promise.
    ///
    /// When `skip_allowed` is false, the sleep's duration is load-bearing
    /// and the handler must not settle the sleep early.
    fn record_sleep(
        &mut self,
        effect_number: EffectNumber,
        promise_state_id: PromiseStateId,
        duration: NonZeroDuration,
        skip_allowed: bool,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

/// Exposes the acknowledgement type a settler produces natively, independent
/// of the unified ack it is ultimately polled into.
///
/// Kept as a separate, non-generic trait so that `Self::Ack` can be named
/// without committing to (and cyclically depending on) a particular
/// unified-ack type parameter.
pub trait SettlerAck {
    /// The acknowledgement type produced by this settler.
    type Ack;
}

/// Exposes the settlement value type a settler produces.
///
/// Shared by [`ActionPromiseSettler`] and [`SleepPromiseSettler`], so a
/// unified settler can require that all its extcall sources settle into
/// one value type.
pub trait HasValue {
    /// The type of a settlement resolution value.
    type Value;
}

/// Produces promise settlements from completed action calls.
pub trait ActionPromiseSettler<UnifiedAck>: SettlerAck + HasValue
where
    UnifiedAck: From<Self::Ack>,
{
    /// The error type returned when polling for action settlements fails.
    type Error: std::fmt::Debug;

    /// Poll for the next batch of action-completion settlements.
    ///
    /// `waiting_promise_state_ids` provides the IDs of all promises the
    /// caller is currently waiting on — see
    /// [`waymark_vm_driver_core::PromiseSettler::get_promise_settlements`],
    /// where the demand originates.
    fn poll_action_settlements<'a>(
        &'a mut self,
        waiting_promise_state_ids: NESlice<'a, PromiseStateId>,
    ) -> impl Future<Output = Result<NEVec<PromiseSettlement<Self::Value, UnifiedAck>>, Self::Error>>
    + Send
    + 'a
    where
        UnifiedAck: 'a;
}

/// Produces promise settlements from elapsed sleeps.
pub trait SleepPromiseSettler<UnifiedAck>: SettlerAck + HasValue
where
    UnifiedAck: From<Self::Ack>,
{
    /// The error type returned when polling for sleep settlements fails.
    type Error: std::fmt::Debug;

    /// Poll for the next batch of elapsed-sleep settlements.
    ///
    /// `waiting_promise_state_ids` provides the IDs of all promises the
    /// caller is currently waiting on — see
    /// [`waymark_vm_driver_core::PromiseSettler::get_promise_settlements`],
    /// where the demand originates.
    fn poll_sleep_settlements<'a>(
        &'a mut self,
        waiting_promise_state_ids: NESlice<'a, PromiseStateId>,
    ) -> impl Future<Output = Result<NEVec<PromiseSettlement<Self::Value, UnifiedAck>>, Self::Error>>
    + Send
    + 'a
    where
        UnifiedAck: 'a;
}
