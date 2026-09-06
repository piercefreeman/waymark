//! Vocabulary shared by the sink and query sides.

use sqlx::Row as _;

/// The event type this store appends and reads: the production payload.
pub(crate) type Event = waymark_observability_events_core::Event<
    waymark_ids::NodeId,
    waymark_observability_events_payload::Payload,
>;

/// The `observability_events` column list, in [`Event`] field order with
/// the payload's kind pulled out beside it.
pub(crate) const EVENT_COLUMNS: &str = "node_id, node_sequence, at, kind, payload";

/// A stored `node_sequence` outside the domain of positions: the column
/// is a `bigint`, positions are unsigned, and a negative value is
/// corruption — never clamped, always surfaced.
///
/// Surfaces as [`sqlx::Error::Decode`], downcastable to this type.
#[derive(Debug, thiserror::Error)]
#[error("node_sequence: negative position {value}")]
pub struct NegativeNodeSequenceError {
    /// The stored value.
    pub value: i64,
}

/// Read one event from a row shaped like [`EVENT_COLUMNS`].
///
/// The position comes back through the persisted-log pathway — it was
/// minted by a counter before it was stored.
pub(crate) fn decode_event(row: &sqlx::postgres::PgRow) -> Result<Event, sqlx::Error> {
    let node_id = row.try_get("node_id")?;
    let node_sequence: i64 = row.try_get("node_sequence")?;
    let node_sequence = u64::try_from(node_sequence).map_err(|_| {
        sqlx::Error::Decode(Box::new(NegativeNodeSequenceError {
            value: node_sequence,
        }))
    })?;
    let node_sequence = waymark_node_sequence::NodeSequence::from_persisted(node_sequence);
    let at = row.try_get("at")?;
    let sqlx::types::Json(payload) = row.try_get("payload")?;

    Ok(Event {
        node_id,
        node_sequence,
        at,
        payload,
    })
}
