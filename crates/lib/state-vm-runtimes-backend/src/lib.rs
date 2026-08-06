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

/// One VM's snapshot to persist, passed to [`StoreSnapshots::store_snapshots`].
#[derive(Debug)]
pub struct StoreSnapshotsItem<'a, VmId> {
    /// The VM whose snapshot this is.
    pub vm_id: &'a VmId,

    /// The serialized snapshot bytes.
    pub snapshot: &'a [u8],
}

// Both fields are references, so the item is copyable for any `VmId` — no
// `VmId: Copy`/`Clone` bound, unlike what `derive` would impose.
impl<VmId> Clone for StoreSnapshotsItem<'_, VmId> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<VmId> Copy for StoreSnapshotsItem<'_, VmId> {}

/// Persist snapshots for VMs in a batch.
///
/// Called on behalf of driver threads — implementations may block.
pub trait StoreSnapshots: HasVmId {
    /// Error type for store operations.
    type Error: std::fmt::Debug;

    /// Persist the given snapshots in one batch.
    ///
    /// A VM whose row is gone (already completed or deleted) is a benign
    /// no-op; the batch as a whole succeeds or fails.
    fn store_snapshots<'a>(
        &'a self,
        snapshots: &'a [StoreSnapshotsItem<'a, Self::VmId>],
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
/// Blanket-implemented for any type that implements both [`StoreSnapshots`]
/// and [`LoadForRevive`].
#[waymark_blanket_impl_macros::blanket_impl]
pub trait VmRuntimesStateBackend: StoreSnapshots + LoadForRevive {}
