//! Per-VM request purge trait.

use super::common::HasVmId;

/// Backend capability for purging all requests of a VM.
pub trait PurgeVmActionCallRequests: HasVmId {
    /// The error type for purge operations.
    type Error: core::fmt::Debug;

    /// Remove every recorded request belonging to `vm_id`.
    ///
    /// Called when the VM reaches its terminal state; pending rows must
    /// not outlive the workload they belong to.  Purging a VM with no rows
    /// is a no-op.
    fn purge_vm_action_call_requests<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
