//! Completion recording trait and error classification.

use nonempty_collections::{NESlice, NEVec};

use super::common::{CompletionKey, CompletionRecord, HasVmId};

/// Backend capability for durably recording action-call completions.
pub trait RecordCompletions: HasVmId {
    /// The error type for record operations.
    type Error: Error + core::fmt::Debug;

    /// Durably record a batch of completions.
    ///
    /// Recording is idempotent per key: a record whose key already exists
    /// with byte-identical data is silently accepted.  A record whose key
    /// exists with the same effect number but a **conflicting outcome** is
    /// expected under at-least-once redelivery of non-deterministic
    /// actions: the first recorded outcome wins, the redelivered one is
    /// discarded, and the key is reported via
    /// [`RecordingSuccess::SomeConflictingOutcomes`].
    ///
    /// A record whose key exists with a **different effect number**
    /// violates the "same effect ⇒ same pair" invariant and must fail
    /// with [`ErrorKind::DivergentEffectNumber`] — a data-integrity bug
    /// that must never be retried, unlike [`ErrorKind::Internal`]
    /// failures which are retryable.  When such an error is returned,
    /// every record not named in it has already been durably recorded —
    /// callers need not (and must not) retry the batch.
    fn record_completions<'a>(
        &'a self,
        records: NESlice<'a, CompletionRecord<Self::VmId>>,
    ) -> impl Future<Output = Result<RecordingSuccess<Self::VmId>, Self::Error>> + Send + 'a;
}

/// The successful outcome of recording a batch of completions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingSuccess<VmId> {
    /// Every record was recorded, or already existed with identical data.
    AllRecorded,

    /// The batch was fully processed, but these keys already existed with
    /// the same effect number and a conflicting outcome.  First write wins:
    /// the redelivered outcomes were discarded.  Expected under
    /// at-least-once redelivery of non-deterministic actions; callers
    /// typically log the keys and move on.
    SomeConflictingOutcomes(NEVec<CompletionKey<VmId>>),
}

/// Classification interface for completion-recording errors.
pub trait Error {
    /// Classify this error into a stable [`ErrorKind`].
    fn kind(&self) -> ErrorKind;
}

/// Stable categories for completion-recording failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// A record's key already exists with a different effect number — the
    /// "same effect ⇒ same pair" invariant is broken.  A data-integrity
    /// violation — never retry.
    DivergentEffectNumber,

    /// An internal backend failure (database, connection, etc.) —
    /// retryable.
    Internal,
}

impl Error for core::convert::Infallible {
    fn kind(&self) -> ErrorKind {
        match *self {}
    }
}
