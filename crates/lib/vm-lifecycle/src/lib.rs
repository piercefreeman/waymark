//! VM Lifecycle management.
//!
//! This crate provides policy-driven decisions for when a VM runtime should
//! be persisted (snapshotted) and when a VM should be evicted from memory.
//!
//! # Key concepts
//!
//! **Persistence** is primarily extcall-driven: the driver sends a state
//! dump after processing external calls. The [`PeriodicSnapshotPolicy`]
//! serves as a safety net to guarantee snapshots at a minimum interval.
//!
//! **Eviction** removes idle VMs from memory after a configurable period.
//!
//! **Terminology:**
//! - *waking* — activating a continuation on an in-memory VM runner that
//!   has no ready frames.
//! - *reviving* — instantiating a new VM runner instance from a persisted
//!   state snapshot.
//!
//! # Architecture
//!
//! - [`VmState`] tracks metadata about a running VM instance.
//! - [`LifecyclePolicy`] is a trait for pluggable policies that evaluate
//!   VM state and produce a [`LifecycleDecision`].
//!
//! # Built-in policies
//!
//! - [`IdleEvictionPolicy`] — evict VMs that have been idle too long.
//! - [`PeriodicSnapshotPolicy`] — safety-net periodic snapshots.
//! - [`CompositePolicy`] — combine multiple policies with AND/OR semantics.

#![warn(missing_docs)]

mod decision;
mod policies;
mod state;

pub use decision::LifecycleDecision;
pub use policies::{CompositePolicy, IdleEvictionPolicy, LifecyclePolicy, PeriodicSnapshotPolicy};
pub use state::VmState;
