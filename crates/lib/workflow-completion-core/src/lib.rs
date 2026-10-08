//! Core types shared by the workflow-completion handlers.
//!
//! Holds the typed [`Outcome`] reused by the direct (in-memory) completion
//! handler and the outcome-polling service, so the two don't each define an
//! identical enum.

#![warn(missing_docs)]

/// A typed workflow execution outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome<Value, RaisedException> {
    /// The workflow completed successfully with this value.
    Completion(Value),

    /// The workflow terminated with an unhandled exception.
    Exception(RaisedException),
}
