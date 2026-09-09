//! Backend trait for the observability-events sink: appending events to
//! a store.

#![warn(missing_docs)]

use nonempty_collections::NESlice;
use waymark_observability_events_core::Event;

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

/// The event type of a backend: its node identity, its payload.
pub type EventFor<Backend> =
    Event<<Backend as HasNodeId>::NodeId, <Backend as HasPayload>::Payload>;

/// Append a batch of events to the store.
pub trait AppendEvents: HasNodeId + HasPayload {
    /// Error type for append operations.
    type Error: std::fmt::Debug;

    /// Append the given events in one batch.
    fn append_events<'a>(
        &'a self,
        events: NESlice<'a, EventFor<Self>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
