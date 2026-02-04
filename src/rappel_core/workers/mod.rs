//! Worker pool implementations.

mod base;
mod inline;

pub use base::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};
pub use inline::{ActionCallable, InlineWorkerPool};
