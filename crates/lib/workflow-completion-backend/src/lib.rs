//! Backend traits for persisting workflow completion results.
//!
//! Defines the interfaces for recording when a workflow execution completes
//! (successfully or with an unhandled exception).

#![warn(missing_docs)]

/// Common base: every completion backend is associated with a VM identifier
/// type.
pub trait HasVmId {
    /// The VM / workflow identifier type.
    type VmId;
}

/// Records successful workflow completions.
pub trait RecordCompletion: HasVmId {
    /// The error type for completion operations.
    type Error: core::fmt::Debug;

    /// Record that a workflow completed successfully with the given value.
    fn record_completion<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        value: impl AsRef<[u8]> + Send + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Records workflow terminations due to unhandled exceptions.
pub trait RecordException: HasVmId {
    /// The error type for exception-recording operations.
    type Error: core::fmt::Debug;

    /// Record that a workflow terminated with an unhandled exception.
    fn record_exception<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        exception: impl AsRef<[u8]> + Send + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// The outcome of a completed workflow execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The workflow completed successfully.
    Completion(Vec<u8>),
    /// The workflow terminated with an unhandled exception.
    Exception(Vec<u8>),
}

/// Polls for the outcome of a workflow execution.
///
/// Returns `Ok(None)` if no outcome has been recorded yet, allowing the
/// caller to retry after a delay.
pub trait PollOutcome: HasVmId {
    /// The error type for poll operations.
    type Error: core::fmt::Debug;

    /// Check whether an outcome is available for the given VM.
    fn poll_outcome<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
    ) -> impl Future<Output = Result<Option<Outcome>, Self::Error>> + Send + 'a;
}

/// Convenience trait: a backend that includes all traits from this crate.
pub trait WorkflowCompletionBackend: RecordCompletion + RecordException + PollOutcome {}

impl<T> WorkflowCompletionBackend for T where T: RecordCompletion + RecordException + PollOutcome {}
