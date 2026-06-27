//! Implementations of [`waymark_action_runtime_core::ActionCallRequester`]
//! and [`waymark_action_runtime_core::ActionCallOutcomesProvider`] backed by
//! a [`waymark_worker_core::BaseWorkerPool`].

#![warn(missing_docs)]

mod outcomes_provider;
mod requester;

pub use self::outcomes_provider::*;
pub use self::requester::*;
