//! Durable sleeps — the persistent-path replacement for the transient
//! in-memory sleep reconciler.
//!
//! # Architecture
//!
//! Sleep requests are stored durably as the VM emits them and removed
//! only once their promise settlements have been durably applied,
//! mirroring the durable action-call completions flow — minus the second
//! table (the ack deletes the recorded request itself) and minus the
//! codec (an elapsed sleep resolves to the value minted by a
//! [`waymark_sleep_core::SleepValueProvider`]).  Two background loops
//! plus per-VM pieces cooperate:
//!
//! 1. [`handler::EffectHandler`] (per VM) implements
//!    [`waymark_extcall_reconciler_core::SleepEffectHandler`]: it
//!    computes the absolute wake deadline once and records it through
//!    the backend before the effect counts as handled.  A revival replay
//!    re-records the same key and is silently ignored — the original
//!    deadline stands.
//! 2. [`poller::run`] drives a single shared loop that polls the backend
//!    for exactly the sleeps the VMs are currently waiting on, as they
//!    come due.  Per-VM [`poller::SettlementsHandle`]s (created via
//!    [`poller::DemandRegistrar::subscribe`]) implement
//!    [`waymark_extcall_reconciler_core::SleepPromiseSettler`], settling
//!    the due sleeps with [`poller::Ack`]s minted from the rows' own
//!    keys.
//! 3. [`acker::run`] drains acknowledged sleep keys from a channel and
//!    batch-deletes their rows.

#![warn(missing_docs)]

pub mod acker;
pub mod handler;
pub mod poller;

#[cfg(test)]
mod test_support;

pub use self::handler::EffectHandler;
pub use self::poller::{DemandRegistrar, SettlementsHandle};
