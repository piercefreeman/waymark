//! Implementations of [`waymark_action_runtime_core::ActionCallRequester`]
//! and [`waymark_action_runtime_core::ActionCallCompletionsProvider`] backed by
//! a worker pool: the requester queues through
//! [`waymark_worker_core::QueueActionDispatch`], the completions provider
//! polls through [`waymark_worker_core::PollActionResults`].

#![warn(missing_docs)]

mod completions_provider;
mod requester;

pub use self::completions_provider::WorkerPoolActionCallCompletionsProvider;
pub use self::requester::WorkerPoolActionRequester;
