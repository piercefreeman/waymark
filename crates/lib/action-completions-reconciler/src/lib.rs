//! Durable action-call completions — the persistent-path replacement for
//! the in-memory completions router.
//!
//! # Architecture
//!
//! Completions are stored durably as they arrive from the worker pool and
//! removed only once their promise settlements have been durably applied,
//! mirroring the store-before-dispatch model on the dispatch side.  Three
//! background loops plus a per-VM settler cooperate:
//!
//! 1. [`writer::run`] ingests completion batches from an
//!    [`waymark_action_runtime_core::ActionCallCompletionsProvider`]
//!    (typically the worker-pool provider), encodes each execution result, and
//!    records everything through the backend.
//! 2. [`poller::run`] drives a single shared loop that polls the
//!    backend for exactly the completions the VMs are currently waiting
//!    on.  Per-VM [`poller::SettlementsHandle`]s (created via
//!    [`poller::DemandRegistrar::subscribe`]) implement
//!    [`waymark_extcall_reconciler_core::ActionPromiseSettler`], settling
//!    the delivered completions with [`poller::Ack`]s minted from the
//!    rows' own keys.
//! 3. [`acker::run`] drains acknowledged completion keys from a channel
//!    and batch-deletes their rows.

#![warn(missing_docs)]

pub mod acker;
pub mod poller;
pub mod writer;

#[cfg(test)]
mod test_support;

pub use self::poller::{DemandRegistrar, SettlementsHandle};
