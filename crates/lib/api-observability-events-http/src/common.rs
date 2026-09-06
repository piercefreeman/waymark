//! Wire vocabulary shared by the operations.

/// One event, as served on the wire.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct Event {
    /// The emitting node's id (a UUID; one identity per node boot).
    pub node_id: String,

    /// The event's position in its node's stream.
    pub node_sequence: u64,

    /// When the emitter stamped the event.
    pub at: chrono::DateTime<chrono::Utc>,

    /// The event's kind, as its stable tag.
    pub kind: String,

    /// The event's payload, as the source emitted it.
    pub payload: serde_json::Value,
}

/// One page of a read, as served on the wire.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct Page {
    /// The events of this page, in the read's order; empty when the read
    /// had nothing (more) to give.
    pub events: Vec<Event>,

    /// Where to resume from for what follows this page, as an opaque
    /// cursor for the read's `after`; absent when the page is empty.
    pub next: Option<String>,
}

fn event<Payload>(
    event: waymark_observability_events_core::Event<waymark_ids::NodeId, Payload>,
) -> Result<Event, serde_json::Error>
where
    Payload: waymark_observability_events_core::Kinded + serde::Serialize,
{
    let kind = waymark_observability_events_core::Kinded::kind(&event.payload);
    let kind = waymark_observability_events_core::kind::Tagged::tag(kind);
    let payload = serde_json::to_value(&event.payload)?;

    Ok(Event {
        node_id: event.node_id.to_string(),
        node_sequence: event.node_sequence.get(),
        at: event.at,
        kind: kind.to_owned(),
        payload,
    })
}

/// The wire page of a read's outcome: an absent page is an empty one
/// with no cursor.
pub(crate) fn page<Payload, Cursor>(
    page: Option<
        waymark_observability_events_query_backend::Page<waymark_ids::NodeId, Payload, Cursor>,
    >,
) -> Result<Page, serde_json::Error>
where
    Payload: waymark_observability_events_core::Kinded + serde::Serialize,
    Cursor: waymark_cursor_core::EncodeCursor,
{
    let Some(page) = page else {
        return Ok(Page {
            events: Vec::new(),
            next: None,
        });
    };

    let next = waymark_cursor_core::EncodeCursor::encode(&page.next);

    let events = page
        .events
        .into_iter()
        .map(event)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Page {
        events,
        next: Some(next),
    })
}
