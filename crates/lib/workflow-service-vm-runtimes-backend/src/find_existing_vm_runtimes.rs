//! Batch existence query for registered VM runtimes.

use nonempty_collections::NESlice;

use super::common::HasVmId;

/// Backend capability for checking which VM runtimes are registered.
///
/// This is the read-side counterpart to
/// [`RegisterVmRuntime`](super::RegisterVmRuntime): it answers "which of these
/// VM runtimes exist?" against the same registration state, without touching
/// the registration write path.
pub trait FindExistingVmRuntimes: HasVmId {
    /// The error type for the query.
    type Error: core::fmt::Debug;

    /// Return the subset of `vm_ids` that have a registered VM runtime.
    ///
    /// The result may be empty (none are registered), may be smaller than the
    /// input, and its order is unspecified.
    ///
    /// Takes a borrowed [`NESlice`] — a concrete type — rather than a generic
    /// iterator. A generic parameter (named or `impl Trait`) would be captured
    /// by the `impl Future` return and trip rust-lang/rust#100013 when the
    /// future is awaited through an `#[async_trait]` consumer; a slice also
    /// binds directly to the `= ANY($1)` query with no intermediate allocation.
    fn find_existing_vm_runtimes<'a>(
        &'a self,
        vm_ids: NESlice<'a, Self::VmId>,
    ) -> impl Future<Output = Result<Vec<Self::VmId>, Self::Error>> + Send + 'a;
}
