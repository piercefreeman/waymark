//! Implementations of [`waymark_action_runtime_core::ActionCallRequester`]
//! and [`waymark_action_runtime_core::ActionCallOutcomesProvider`] backed by
//! tokio channels, suitable for the bridge's transient executor.
//!
//! Action dispatches are sent as protobuf [`WorkflowStreamResponse`] messages
//! on an mpsc channel. Action results are received via a dedicated mpsc
//! channel and surfaced as [`ActionCallOutcome`]s.

#![warn(missing_docs)]

mod completions_provider;
mod requester;

pub use self::completions_provider::WorkerStreamActionCallCompletionsProvider;
pub use self::requester::WorkerStreamActionRequester;
