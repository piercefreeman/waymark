//! Core error type shared across VM-value converter crates.

#![warn(missing_docs)]

/// Error returned when attempting to convert a pending promise value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot convert pending promise {0:?}")]
pub struct PendingPromiseError(pub waymark_vm_runtime_promise_core::PromiseStateId);
