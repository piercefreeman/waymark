//! Persistence operations for VM runtimes state management.

#![warn(missing_docs)]

/// Common base: every snapshot backend is associated with a VM identifier type.
pub trait HasVmId {
    /// The VM identifier type.
    type VmId;
}

/// Common base: every snapshot backend is associated with an executable
/// identifier type.
pub trait HasExecutableId {
    /// The executable identifier type.
    type ExecutableId;
}

/// Persist a snapshot for a VM.
///
/// Called from a driver thread — implementations may block.
pub trait StoreSnapshot: HasVmId {
    /// Error type for store operations.
    type Error: std::fmt::Debug;

    /// Persist a snapshot for the given VM.
    fn store_snapshot<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        data: &'a [u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Payload returned by [`LoadForRevive`], containing both the serialized
/// snapshot and the id of the executable needed to revive the VM.
#[derive(Debug, Clone)]
pub struct RevivePayload<ExecutableId> {
    /// The serialized VM runtime snapshot.
    pub snapshot: Vec<u8>,

    /// The id of the bytecode executable that the VM was running.
    pub executable_id: ExecutableId,
}

/// Load a previously-stored snapshot and the associated executable id
/// for reviving a VM.
pub trait LoadForRevive: HasVmId + HasExecutableId {
    /// Error type for load operations.
    type Error: std::fmt::Debug;

    /// Load a previously-stored snapshot for the given VM, returning both
    /// the snapshot bytes and the id of the executable needed to revive it.
    fn load_for_revive<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
    ) -> impl std::future::Future<Output = Result<RevivePayload<Self::ExecutableId>, Self::Error>>
    + Send
    + 'a;
}

/// Convenience trait: a backend that supports both store and load.
///
/// Blanket-implemented for any type that implements both [`StoreSnapshot`]
/// and [`LoadForRevive`].
pub trait VmRuntimesStateBackend: StoreSnapshot + LoadForRevive {}

impl<T> VmRuntimesStateBackend for T where T: StoreSnapshot + LoadForRevive {}
