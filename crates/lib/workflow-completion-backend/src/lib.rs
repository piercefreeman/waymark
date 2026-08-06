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

/// The outcome of a completed workflow execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The workflow completed successfully.
    Completion(Vec<u8>),
    /// The workflow terminated with an unhandled exception.
    Exception(Vec<u8>),
}

/// One VM's terminal outcome to record, passed to
/// [`RecordOutcomes::record_outcomes`].
#[derive(Debug)]
pub struct RecordOutcomesItem<'a, VmId> {
    /// The VM whose outcome this is.
    pub vm_id: &'a VmId,

    /// The serialized terminal outcome.
    pub outcome: &'a Outcome,
}

// Both fields are references, so the item is copyable for any `VmId` — no
// `VmId: Copy`/`Clone` bound, unlike what `derive` would impose.
impl<VmId> Clone for RecordOutcomesItem<'_, VmId> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<VmId> Copy for RecordOutcomesItem<'_, VmId> {}

/// Error classification for [`RecordOutcomes`].
pub mod record_outcomes {
    /// Classification interface for outcome-recording errors.
    pub trait Error {
        /// Classify this error into a stable [`ErrorKind`].
        fn kind(&self) -> ErrorKind;
    }

    /// Stable categories for outcome-recording failures.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ErrorKind {
        /// The batch itself is invalid — the same statement fails
        /// identically on every attempt (e.g. a duplicate vm_id making
        /// the upsert affect one row twice).  Never retry.
        InvalidBatch,

        /// An internal backend failure (database, connection, etc.) —
        /// retryable.
        Internal,
    }

    impl Error for core::convert::Infallible {
        fn kind(&self) -> ErrorKind {
            match *self {}
        }
    }
}

/// Records workflow terminal outcomes — completions and unhandled-exception
/// terminations — in a batch.
///
/// Recording is first-write-wins and idempotent per VM: a VM whose stored
/// outcome is byte-identical to the incoming one is silently accepted (a
/// revived VM replaying its terminal effect).  A VM whose stored outcome
/// differs is a **per-row** condition, not a failure of the recording: its
/// row is left untouched and the key is reported via
/// [`RecordingSuccess::SomeConflicted`] — the caller decides that VM's
/// fate.  An `Err` means the recording itself failed (database,
/// connection, etc.) and nothing of the batch landed; whether retrying
/// can help is classified by [`record_outcomes::Error::kind`].
pub trait RecordOutcomes: HasVmId {
    /// The error type for outcome-recording operations.
    type Error: record_outcomes::Error + core::fmt::Debug;

    /// Durably record the given terminal outcomes in one batch.
    fn record_outcomes<'a>(
        &'a self,
        outcomes: nonempty_collections::NESlice<'a, RecordOutcomesItem<'a, Self::VmId>>,
    ) -> impl Future<Output = Result<RecordingSuccess<Self::VmId>, Self::Error>> + Send + 'a;
}

/// The successful outcome of recording a batch of terminal outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingSuccess<VmId> {
    /// Every outcome was freshly recorded or identically re-recorded.
    AllRecorded,

    /// The batch was fully processed, but these VMs already hold
    /// **different** stored outcomes — first-write-wins kept the stored
    /// value and the incoming one was discarded.  Every VM not named here
    /// was durably recorded.
    SomeConflicted(nonempty_collections::NEVec<VmId>),
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
#[waymark_blanket_impl_macros::blanket_impl]
pub trait WorkflowCompletionBackend: RecordOutcomes + PollOutcome {}
