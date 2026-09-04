//! Vocabulary shared by the reads.

use nonempty_collections::NEVec;

/// The node identity type of a backend's events.
pub trait HasNodeId {
    /// The node identity type.
    type NodeId;
}

/// The payload type of a backend's events.
///
/// The backend's own: a store lays the payload out — it indexes its
/// access paths and derives state from its kinds — so it names the
/// payload it serves rather than accepting any.
pub trait HasPayload {
    /// The payload type.
    type Payload;
}

/// One non-empty page of a read; a read with nothing to give returns no
/// page at all, so a page always has events and always has a position.
#[derive(Debug)]
pub struct Page<NodeId, Payload, Cursor> {
    /// The events of this page, in the read's order.
    pub events: NEVec<waymark_observability_events_core::Event<NodeId, Payload>>,

    /// Where to resume from for what follows this page in the read's
    /// order: the position of the page's last event.
    pub next: Cursor,
}

/// The page type of a backend's read, for that read's cursor.
pub type PageFor<Backend, Cursor> =
    Page<<Backend as HasNodeId>::NodeId, <Backend as HasPayload>::Payload, Cursor>;
