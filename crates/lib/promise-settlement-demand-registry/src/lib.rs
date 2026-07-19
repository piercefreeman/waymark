//! Shared machinery for demand-driven promise-settlement polling.
//!
//! The durable reconcilers (action-call completions, sleeps) share one
//! shape: rows are stored durably, a single background loop polls the
//! backend for exactly the promise settlements the VMs are currently
//! demanding, and rows are deleted only once their settlements have been
//! durably applied (acked).  This crate hosts the domain-agnostic parts
//! of that shape:
//!
//! - [`registry`] — per-VM demand registration, item buffering, and
//!   delivery between the shared poll loop and the per-VM waiters.
//! - [`acker`] — the background drain that batch-deletes acknowledged
//!   settlement keys.
//!
//! What stays in the domain crates: the poll loop itself (its pacing and
//! its backend query), the mapping of delivered items into promise
//! settlements, and the settlement-acknowledgement types (their
//! conversions into the unified extcall ack are orphan-rule-bound to the
//! domain crates).

#![warn(missing_docs)]

pub mod acker;
pub mod registry;
