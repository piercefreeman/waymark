//! VM runtime registration trait.

use super::common::{HasExecutableId, HasVmId};

/// One VM runtime to register, passed to
/// [`RegisterVmRuntimes::register_vm_runtimes`].
#[derive(Debug)]
pub struct RegisterVmRuntimesItem<'a, VmId, ExecutableId> {
    /// The id to register the VM runtime under.
    pub vm_id: &'a VmId,

    /// The bytecode executable the VM is running.
    pub executable_id: &'a ExecutableId,

    /// The serialized initial runtime snapshot.
    pub snapshot: &'a [u8],
}

// All fields are references, so the item is copyable for any id types — no
// `Copy`/`Clone` bounds, unlike what `derive` would impose.
impl<VmId, ExecutableId> Clone for RegisterVmRuntimesItem<'_, VmId, ExecutableId> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<VmId, ExecutableId> Copy for RegisterVmRuntimesItem<'_, VmId, ExecutableId> {}

/// Backend capability for registering VM runtimes, in a batch.
///
/// Registering creates a row in the snapshot table and a matching row in
/// the workload pinning table for each VM runtime, atomically for the
/// whole batch.
///
/// An already-registered VM runtime is a **per-row** condition, not a
/// failure of the registration: its rows are left untouched (no workload
/// row is added either) and the id is reported via
/// [`RegistrationSuccess::SomeAlreadyRegistered`] — every VM runtime not
/// named there was durably registered.  An `Err` means the registration
/// itself failed; nothing of the batch landed and the whole batch is
/// retryable.
pub trait RegisterVmRuntimes: HasVmId + HasExecutableId {
    /// The error type for registration operations.
    type Error: std::fmt::Debug;

    /// Durably register the given VM runtimes in one batch.
    fn register_vm_runtimes<'a>(
        &'a self,
        runtimes: nonempty_collections::NESlice<
            'a,
            RegisterVmRuntimesItem<'a, Self::VmId, Self::ExecutableId>,
        >,
    ) -> impl Future<Output = Result<RegistrationSuccess<Self::VmId>, Self::Error>> + Send + 'a;
}

/// The successful outcome of registering a batch of VM runtimes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationSuccess<VmId> {
    /// Every VM runtime was freshly registered.
    AllRegistered,

    /// The batch was fully processed, but VM runtimes were already
    /// registered under these ids and left untouched.  Every VM runtime
    /// not named here was durably registered.
    SomeAlreadyRegistered(nonempty_collections::NEVec<VmId>),
}
