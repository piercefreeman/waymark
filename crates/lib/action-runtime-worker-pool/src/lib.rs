//! Implementations of [`waymark_action_runtime_core::ActionCallRequester`]
//! and [`waymark_action_runtime_core::ActionCallCompletionsProvider`] backed by
//! a [`waymark_worker_core::BaseWorkerPool`].

#![warn(missing_docs)]

mod completions_provider;
mod requester;

pub use self::completions_provider::*;
pub use self::requester::*;
