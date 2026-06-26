//! Implementations of [`waymark_action_runtime_core::ActionCallRequester`]
//! and [`waymark_action_runtime_core::ActionCallOutcomesProvider`] backed by
//! tokio channels, suitable for the bridge's transient executor.
//!
//! Action dispatches are sent as protobuf [`WorkflowStreamResponse`] messages
//! on an mpsc channel. Action results are received via a dedicated mpsc
//! channel and surfaced as [`ActionCallOutcome`]s.

#![warn(missing_docs)]

mod dispatcher;
mod receiver;

pub use crate::dispatcher::ActionDispatchSender;
pub use crate::receiver::ActionResultReceiver;
