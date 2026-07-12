//! Per-VM completion purge trait.

use super::common::HasVmId;

/// Backend capability for purging all completions of a VM.
pub trait PurgeVmCompletions: HasVmId {
    /// The error type for purge operations.
    type Error: core::fmt::Debug;

    /// Remove every recorded completion belonging to `vm_id`.
    ///
    /// Called when the VM reaches its terminal state; unclaimed rows must
    /// not outlive the workload they belong to.  Purging a VM with no rows
    /// is a no-op.
    fn purge_vm_completions<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
