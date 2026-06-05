//! Persistence operations for VM runtime snapshots.
//!
//! Each operation is its own trait with its own error type, so
//! implementations can use different error types for storing vs loading.
//! Both extend [`HasVmId`] so the VM identifier type is shared.
//!
//! For convenience, [`SnapshotBackend`] is a blanket supertrait that
//! requires both — use it when a single type fulfills both roles.

#![warn(missing_docs)]

/// Common base: every snapshot backend is associated with a VM identifier type.
pub trait HasVmId {
    /// The VM identifier type.
    type VmId;
}

/// Persist a snapshot for a VM.
///
/// Called from a driver thread — implementations may block.
pub trait StoreSnapshot: HasVmId {
    /// Error type for store operations.
    type Error;

    /// Persist a snapshot for the given VM.
    fn store_snapshot(&self, vm_id: &Self::VmId, data: Vec<u8>) -> Result<(), Self::Error>;
}

/// Load a previously-stored snapshot for a VM.
pub trait LoadSnapshot: HasVmId {
    /// Error type for load operations.
    type Error;

    /// Load a previously-stored snapshot for the given VM.
    fn load_snapshot(
        &self,
        vm_id: &Self::VmId,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, Self::Error>> + Send;
}

/// Convenience trait: a backend that supports both store and load.
///
/// Blanket-implemented for any type that implements both [`StoreSnapshot`]
/// and [`LoadSnapshot`].
pub trait SnapshotBackend: StoreSnapshot + LoadSnapshot {}

impl<T> SnapshotBackend for T where T: StoreSnapshot + LoadSnapshot {}
