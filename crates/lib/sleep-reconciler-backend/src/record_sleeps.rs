//! Sleep recording trait and error classification.

use nonempty_collections::NESlice;

use super::common::{HasTimestamp, HasVmId, SleepRecord};

/// Backend capability for durably recording sleep requests.
pub trait RecordSleeps: HasVmId + HasTimestamp {
    /// The error type for record operations.
    type Error: Error + core::fmt::Debug;

    /// Durably record a batch of sleep requests.
    ///
    /// Recording is idempotent per key: a record whose key already
    /// exists is silently ignored and the originally recorded deadline
    /// stands.  A re-emitted sleep effect recomputes its deadline
    /// relative to the replay time, so accepting a re-record would walk
    /// the deadline forward.
    ///
    /// A record whose key exists with a **different effect number**
    /// violates the "same effect ⇒ same pair" invariant and must fail
    /// with [`ErrorKind::DivergentEffectNumber`] — a data-integrity bug
    /// that must never be retried, unlike [`ErrorKind::Internal`]
    /// failures which are retryable.  When such an error is returned,
    /// every record not named in it has already been durably recorded —
    /// callers need not (and must not) retry the batch.
    fn record_sleeps<'a>(
        &'a self,
        records: NESlice<'a, SleepRecord<Self::VmId, Self::Timestamp>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Classification interface for sleep-recording errors.
pub trait Error {
    /// Classify this error into a stable [`ErrorKind`].
    fn kind(&self) -> ErrorKind;
}

/// Stable categories for sleep-recording failures.
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
