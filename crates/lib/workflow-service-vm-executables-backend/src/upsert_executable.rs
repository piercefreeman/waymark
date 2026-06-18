//! VM executable upsert trait and error classification.

use super::HasExecutableId;

/// Backend capability for atomically inserting an executable with
/// deduplication by `(name, version)`.
///
/// If an executable with the same name and version already exists and
/// has the same bytecode, returns the existing id. If the bytecode
/// differs, returns an error with [`ErrorKind::Conflict`].
pub trait UpsertExecutable: HasExecutableId {
    /// The error type for store operations.
    type Error: Error + std::fmt::Debug;

    /// Atomically upsert an executable, deduplicating by `(name, version)`.
    fn upsert_executable<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        bytes: &'a [u8],
    ) -> impl Future<Output = Result<Self::ExecutableId, Self::Error>> + Send + 'a;
}

/// Classification interface for [`UpsertExecutable`] backend errors.
pub trait Error {
    /// Classify this error into a stable [`ErrorKind`].
    fn kind(&self) -> ErrorKind;
}

/// Stable categories for [`UpsertExecutable`] backend failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// An executable with this name and version already exists but with
    /// different bytecode.
    Conflict,

    /// An internal backend failure occurred.
    Internal,
}

impl Error for core::convert::Infallible {
    fn kind(&self) -> ErrorKind {
        match *self {}
    }
}
