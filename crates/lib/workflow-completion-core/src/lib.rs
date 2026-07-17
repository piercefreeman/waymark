//! Core types shared by the workflow-completion handlers.
//!
//! Holds the typed [`Outcome`] reused by the direct (in-memory) completion
//! handler and the outcome-polling service, so the two don't each define an
//! identical enum.

#![warn(missing_docs)]

use waymark_vm_runtime_exception::Exception;

/// A typed workflow execution outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome<Value> {
    /// The workflow completed successfully with this value.
    Completion(Value),

    /// The workflow terminated with an unhandled exception.
    Exception(Exception<Value>),
}
