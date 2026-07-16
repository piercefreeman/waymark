//! Durable handling of action-call effects — the persistent-path
//! replacement for the transient action effect handler.
//!
//! Emitted action-call effects are recorded as durable request rows
//! (born locked by this process) before the call is delivered to the
//! local worker pool; at VM revival, pending rows whose locks lapsed are
//! relocked and their calls redelivered.  A request row is removed by
//! the store itself the moment its completion is durably recorded, so
//! the subsystem never observes outcomes — it is purely an
//! at-least-once delivery machine.
//!
//! Issuance is VM-residency-scoped: calls are only ever (re)issued from
//! the VM execution flow — at effect emission
//! ([`EffectHandler`]) and at revival reconcile
//! ([`ReconcilingFactory`], a [`waymark_state_manager_core::Factory`]
//! decorator running before the VM is produced).  There is no background
//! issuance of any kind.  Attempt execution and lock ownership are
//! process-scoped: evicting a VM neither cancels its in-flight attempts
//! nor releases their locks — the [`renewal`] heartbeat keeps them alive
//! until the outcome is recorded.  A held lock is the authorization to
//! be executing its attempt (never a scheduling signal): a lock that can
//! no longer be renewed in time is a fence breach, force-terminating the
//! local attempts with the process — see [`renewal::run`].

#![warn(missing_docs)]

pub mod action_call_request_payload;
pub mod effect_handler;
mod issuance;
pub mod reconciling_factory;
pub mod renewal;

#[cfg(test)]
mod test_support;

pub use self::action_call_request_payload::ActionCallRequestPayload;
pub use self::effect_handler::EffectHandler;
pub use self::reconciling_factory::ReconcilingFactory;
