//! Shared by the sink and query sides.

use sqlx::Row as _;

/// The event type this store appends and reads: the production payload.
pub(crate) type Event = waymark_observability_events_core::Event<
    waymark_ids::NodeId,
    waymark_observability_events_payload::Payload,
>;

/// The `observability_events` column list, in [`Event`] field order with
/// the payload's kind pulled out beside it.
pub(crate) const EVENT_COLUMNS: &str = "node_id, node_sequence, at, kind, payload";

/// The VM an event names, as the store indexes it: every query wanting
/// the VM timeline index spells exactly this expression …
pub(crate) const VM_ID_EXPRESSION: &str = "((payload->>'vm_id')::uuid)";

/// … together with exactly this predicate, which is the index's own.
pub(crate) const VM_ID_PRESENT: &str = "payload ? 'vm_id'";

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

/// A stored `run_sequence` outside the domain of positions: the payload
/// carries an unsigned position, and a negative value is corruption —
/// never clamped, always surfaced.
///
/// Surfaces as [`sqlx::Error::Decode`], downcastable to this type.
#[derive(Debug, thiserror::Error)]
#[error("run_sequence: negative position {value}")]
pub struct NegativeRunSequenceError {
    /// The stored value.
    pub value: i64,
}

/// A stored `kind` that names no kind of the production payload, or not
/// the kind a derivation expects there: the column is text, the set is
/// closed, and a mismatch is corruption — surfaced, never skipped.
///
/// Surfaces as [`sqlx::Error::Decode`], downcastable to this type.
#[derive(Debug, thiserror::Error)]
#[error("kind: unexpected tag {tag:?}")]
pub struct UnexpectedKindError {
    /// The stored tag.
    pub tag: String,
}

/// Bring a stored `node_sequence` back through the persisted-log pathway
/// — it was minted by a counter before it was stored.
pub(crate) fn decode_node_sequence(
    node_sequence: i64,
) -> Result<waymark_node_sequence::NodeSequence, sqlx::Error> {
    let node_sequence = u64::try_from(node_sequence).map_err(|_| {
        sqlx::Error::Decode(Box::new(NegativeNodeSequenceError {
            value: node_sequence,
        }))
    })?;

    Ok(waymark_node_sequence::NodeSequence::from_persisted(
        node_sequence,
    ))
}

/// Bring a stored `run_sequence` back into its domain.
pub(crate) fn decode_run_sequence(run_sequence: i64) -> Result<u64, sqlx::Error> {
    u64::try_from(run_sequence).map_err(|_| {
        sqlx::Error::Decode(Box::new(NegativeRunSequenceError {
            value: run_sequence,
        }))
    })
}

/// The production kind a stored tag names.
pub(crate) fn decode_kind(
    tag: &str,
) -> Result<waymark_observability_events_payload::Kind, sqlx::Error> {
    waymark_observability_events_core::kind::FromTag::from_tag(tag).ok_or_else(|| {
        sqlx::Error::Decode(Box::new(UnexpectedKindError {
            tag: tag.to_owned(),
        }))
    })
}

/// Read one event from a row shaped like [`EVENT_COLUMNS`].
pub(crate) fn decode_event(row: &sqlx::postgres::PgRow) -> Result<Event, sqlx::Error> {
    let node_id = row.try_get("node_id")?;
    let node_sequence = decode_node_sequence(row.try_get("node_sequence")?)?;
    let at = row.try_get("at")?;
    let sqlx::types::Json(payload) = row.try_get("payload")?;

    Ok(Event {
        node_id,
        node_sequence,
        at,
        payload,
    })
}
