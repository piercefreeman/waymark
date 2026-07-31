//! Request recording trait and error classification.

use nonempty_collections::{NESlice, NEVec};

use super::common::{
    ActionCallRequestKey, ActionCallRequestRecord, HasLockOwnerId, HasTimestamp, HasVmId,
    RequestLockFor,
};

/// Backend capability for durably recording action-call requests.
pub trait RecordActionCallRequests: HasVmId + HasLockOwnerId + HasTimestamp {
    /// The error type for record operations.
    type Error: Error + core::fmt::Debug;

    /// Durably record a batch of requests, born locked with `lock`.
    ///
    /// Freshly inserted rows are owned by `lock.owner` until
    /// `lock.expires_at`; the caller is expected to deliver exactly those
    /// calls to its local worker pool.
    ///
    /// `now` is the caller-clock instant `lock.expires_at` was computed
    /// against.  Implementations keep expiry on the store's clock alone:
    /// the born lock's expiry is stored as the store's now plus the
    /// remaining duration `expires_at - now` — a difference of two
    /// caller-clock values, so no cross-node clock agreement is needed.
    ///
    /// Recording is idempotent per key: a record whose key already exists
    /// with a byte-identical payload is silently accepted, its row is left
    /// **untouched** (including its lock), and the key is reported via
    /// [`RecordingSuccess::SomeAlreadyRecorded`] — the caller must NOT
    /// deliver those calls (an existing row means the VM is replaying a
    /// previously emitted effect, and delivery of the existing row was
    /// already decided by the revival reconcile).
    ///
    /// A record whose key exists with a **different payload** violates
    /// replay determinism (VM replay or codec nondeterminism) and must
    /// fail with [`ErrorKind::DivergentPayload`] — a data-integrity bug
    /// that must never be retried, unlike [`ErrorKind::Internal`]
    /// failures which are retryable.  When such an error is returned,
    /// every record not named in it has already been durably recorded —
    /// callers need not (and must not) retry the batch.
    fn record_action_call_requests<'a>(
        &'a self,
        now: Self::Timestamp,
        lock: RequestLockFor<Self>,
        records: NESlice<'a, ActionCallRequestRecord<Self::VmId>>,
    ) -> impl Future<Output = Result<RecordingSuccess<Self::VmId>, Self::Error>> + Send + 'a;
}

/// The successful outcome of recording a batch of requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingSuccess<VmId> {
    /// Every record was freshly inserted, born locked with the given lock.
    AllRecorded,

    /// The batch was fully processed, but these keys already existed with
    /// byte-identical payloads and were left untouched.  The caller must
    /// not deliver the corresponding calls.
    SomeAlreadyRecorded(NEVec<ActionCallRequestKey<VmId>>),
}

/// Classification interface for request-recording errors.
pub trait Error {
    /// Classify this error into a stable [`ErrorKind`].
    fn kind(&self) -> ErrorKind;
}

/// Stable categories for request-recording failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// A record's key already exists with a different payload — replay
    /// determinism is broken.  A data-integrity violation — never retry.
    DivergentPayload,

    /// An internal backend failure (database, connection, etc.) —
    /// retryable.
    Internal,
}

impl Error for core::convert::Infallible {
    fn kind(&self) -> ErrorKind {
        match *self {}
    }
}
