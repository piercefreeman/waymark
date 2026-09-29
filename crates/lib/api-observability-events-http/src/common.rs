//! Wire types shared by the operations.

/// One event, as served on the wire.
//
// Generic over the backend's payload: the payload is served as the source
// emitted it and described by its own schema, and its kind travels as the
// tag. Naming the kind in a field needs the `Kinded` bound on the struct
// itself; the rest of the bounds live on the impls, and neither serde nor
// schemars can infer them, since they are about the kind, not the payload.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(bound(serialize = "Payload: serde::Serialize"))]
#[schemars(bound = "Payload: schemars::JsonSchema")]
pub struct Event<Payload>
where
    Payload: waymark_observability_events_core::Kinded,
{
    /// The emitting node's id: one identity per node boot.
    pub node_id: waymark_http_api_types::UuidId<waymark_ids::NodeId>,

    /// The event's position in its node's stream.
    pub node_sequence: u64,

    /// When the emitter stamped the event.
    pub at: chrono::DateTime<chrono::Utc>,

    /// The event's kind, as its stable tag.
    pub kind: waymark_api_observability_events_http_types::KindTag<
        Payload::Kind,
        waymark_api_observability_events_http_types::EventKind,
    >,

    /// The event's payload, as the source emitted it.
    pub payload: Payload,
}

fn event<Payload>(
    event: waymark_observability_events_core::Event<waymark_ids::NodeId, Payload>,
) -> Event<Payload>
where
    Payload: waymark_observability_events_core::Kinded,
{
    let kind = waymark_observability_events_core::Kinded::kind(&event.payload);

    Event {
        node_id: event.node_id.into(),
        node_sequence: event.node_sequence.get(),
        at: event.at,
        kind: kind.into(),
        payload: event.payload,
    }
}

/// The wire page of a read's outcome: an absent page is an empty one
/// with no cursor.
pub(crate) fn page<Payload, Cursor>(
    page: Option<
        waymark_observability_events_query_backend::Page<waymark_ids::NodeId, Payload, Cursor>,
    >,
) -> waymark_http_api_types::Page<Event<Payload>, Cursor>
where
    Payload: waymark_observability_events_core::Kinded,
{
    let Some(page) = page else {
        return waymark_http_api_types::Page {
            items: Vec::new(),
            next: None,
        };
    };

    let items = page.events.into_iter().map(event).collect();

    waymark_http_api_types::Page {
        items,
        next: Some(page.next.into()),
    }
}
