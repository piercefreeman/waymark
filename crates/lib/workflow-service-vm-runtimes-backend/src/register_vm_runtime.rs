//! VM runtime registration trait and error classification.

use super::common::{HasExecutableId, HasVmId};

/// Backend capability for registering VM runtimes.
pub trait RegisterVmRuntime: HasVmId + HasExecutableId {
    /// The error type for service operations.
    type Error: Error + std::fmt::Debug;

    /// Register a VM runtime with its associated executable and initial
    /// snapshot.
    ///
    /// Creates a row in the snapshot table and a matching row in the
    /// workload pinning table. Returns an error if this VM is already
    /// registered.
    fn register_vm_runtime(
        &self,
        vm_id: &Self::VmId,
        executable_id: &Self::ExecutableId,
        snapshot: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Classification interface for VM runtime registration errors.
pub trait Error {
    /// Classify this error into a stable [`ErrorKind`].
    fn kind(&self) -> ErrorKind;
}

/// Stable categories for VM runtime registration failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// A VM runtime is already registered for this VM id.
    AlreadyRegistered,

    /// An internal backend failure (database, serialization, etc.).
    Internal,
}

impl Error for core::convert::Infallible {
    fn kind(&self) -> ErrorKind {
        match *self {}
    }
}
