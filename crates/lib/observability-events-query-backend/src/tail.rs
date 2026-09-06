//! One node's stream, followed forward.

use std::num::NonZeroUsize;

use crate::PageFor;

/// Parameters of a tail read.
#[derive(Debug)]
pub struct Params<NodeId, Cursor> {
    /// The node whose stream to follow.
    pub node_id: NodeId,

    /// At most this many events.
    pub limit: NonZeroUsize,

    /// The last position already returned; the read resumes past it in
    /// stream order. `None` starts from the oldest event the store
    /// holds.
    pub after: Option<Cursor>,
}

/// Follow one node's stream in its own order, by position.
///
/// Per node rather than time-merged: a time cursor misses rows that land
/// late with an earlier time, while a position cursor cannot — and a
/// jump in position is how the follower sees a dropped or a late
/// emission.
pub trait Tail: crate::HasNodeId + crate::HasPayload {
    /// A position in this read's order, in the backend's own shape, with
    /// the codec every cursor must have.
    type Cursor: waymark_cursor_core::Cursor;

    /// Error type for the read.
    type Error: std::fmt::Debug;

    /// One page of the node's stream, ascending by position; `None` when
    /// nothing (more) has landed.
    fn tail(
        &self,
        params: Params<Self::NodeId, Self::Cursor>,
    ) -> impl Future<Output = Result<Option<PageFor<Self, Self::Cursor>>, Self::Error>> + Send + '_;
}
