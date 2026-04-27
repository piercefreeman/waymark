//! Core backend traits for waymark.
//!
//! This crate defines the backend contract used by the run loop to move
//! workflow instances through their lifecycle. Rather than requiring a single
//! large trait, it splits the contract into focused capabilities that can be
//! implemented and tested independently:
//!
//! - [`InstanceEnqueue`] inserts newly created workflow instances.
//! - [`InstanceQueuePoll`] claims runnable instances from the queue.
//! - [`InstanceQueueLocksKeepalive`] and [`InstanceQueueLocksRelease`] manage
//!   queue lock ownership.
//! - [`ExecutionStatePersistence`] stores updated execution graph snapshots.
//! - [`ActionsDonePersistence`] records completed action attempts.
//! - [`InstanceQueueCompletion`] marks workflow instances as finished.

#![warn(missing_docs)]

#[cfg(feature = "either")]
mod either;

mod common;
mod traits;

pub use self::common::*;
pub use self::traits::*;

type Timestamp = chrono::DateTime<chrono::Utc>;
