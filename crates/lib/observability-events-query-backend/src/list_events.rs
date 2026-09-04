//! Events across every node, time-merged, newest first.

use std::num::NonZeroUsize;

use crate::PageFor;

/// Parameters of a list read.
#[derive(Debug)]
pub struct Params<Cursor> {
    /// Inclusive start of the time range.
    pub from: chrono::DateTime<chrono::Utc>,

    /// Exclusive end of the time range.
    pub to: chrono::DateTime<chrono::Utc>,

    /// At most this many events.
    pub limit: NonZeroUsize,

    /// The last position already returned; the read resumes past it in
    /// its own order, so towards older events. `None` starts from the
    /// newest.
    pub after: Option<Cursor>,
}

/// Read events across every node, time-merged, newest first.
///
/// The order is total — by time, then by node, then by position in the
/// node's stream — so paging never skips or repeats a row.
pub trait ListEvents: crate::HasNodeId + crate::HasPayload {
    /// A position in this read's order, in the backend's own shape, with
    /// the codec every cursor must have.
    type Cursor: waymark_cursor_core::Cursor;

    /// Error type for the read.
    type Error: std::fmt::Debug;

    /// One page of events in the time range, newest first; `None` when
    /// nothing (more) is in range.
    fn list_events(
        &self,
        params: Params<Self::Cursor>,
    ) -> impl Future<Output = Result<Option<PageFor<Self, Self::Cursor>>, Self::Error>> + Send + '_;
}
