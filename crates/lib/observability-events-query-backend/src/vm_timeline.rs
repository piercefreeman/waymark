//! One VM's events, across every node it ran on, oldest first.

use crate::PageFor;

/// Parameters of a timeline read.
#[derive(Debug)]
pub struct Params<VmId, Cursor> {
    /// The VM whose events to read.
    pub vm_id: VmId,

    /// At most this many events, within the cap the type names.
    pub limit: waymark_query_limit::Limit<1000>,

    /// The last position already returned; the read resumes past it in
    /// timeline order. `None` starts from the VM's oldest event the store
    /// holds.
    pub after: Option<Cursor>,
}

/// Read one VM's events in timeline order: by time, then by node, then by
/// position in the node's stream.
///
/// A VM's runs hop between nodes, so its timeline is time-merged across
/// their streams; within one node's stream the position keeps the order
/// exact even when the clock does not.
pub trait VmTimeline: crate::HasNodeId + crate::HasPayload + crate::HasVmId {
    /// A position in this read's order, in the backend's own shape, with
    /// the codec every cursor must have.
    type Cursor: waymark_cursor_core::Cursor + Send;

    /// Error type for the read.
    type Error: std::fmt::Debug;

    /// One page of the VM's events, oldest first; `None` when nothing
    /// (more) is held for it.
    fn vm_timeline(
        &self,
        params: Params<Self::VmId, Self::Cursor>,
    ) -> impl Future<Output = Result<Option<PageFor<Self, Self::Cursor>>, Self::Error>> + Send + '_;
}
