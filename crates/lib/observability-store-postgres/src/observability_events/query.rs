//! The query side: reading events back, and the positions the reads
//! page by.

use nonempty_collections::NEVec;
use waymark_observability_events_query_backend::{list_events, tail};

use super::common::{EVENT_COLUMNS, decode_event};
use crate::Store;
use crate::common::to_bigint_saturating;

/// A position in the list order — the row last returned, by the columns
/// the order is over.
#[derive(Debug)]
pub struct ListCursor {
    /// The row's `at`.
    at: chrono::DateTime<chrono::Utc>,

    /// The row's `node_id`.
    node_id: waymark_ids::NodeId,

    /// The row's `node_sequence`, as stored.
    node_sequence: i64,
}

/// A position in a node's stream — the row last returned, by its
/// position column.
#[derive(Debug)]
pub struct TailCursor {
    /// The row's `node_sequence`, as stored.
    node_sequence: i64,
}

/// A cursor's wire form could not be read back into a position.
///
/// The wire form is opaque to callers: it is whatever this store wrote
/// as a page's `next`, and only that.
#[derive(Debug, thiserror::Error)]
#[error("not a cursor: {text:?}")]
pub struct ParseCursorError {
    /// The text that was offered.
    pub text: String,
}

impl waymark_cursor_core::EncodeCursor for ListCursor {
    fn encode(&self) -> String {
        format!(
            "{}/{}/{}",
            self.at.timestamp_micros(),
            self.node_id,
            self.node_sequence
        )
    }
}

impl waymark_cursor_core::DecodeCursor for ListCursor {
    type Error = ParseCursorError;

    fn decode(text: &str) -> Result<Self, ParseCursorError> {
        let not_a_cursor = || ParseCursorError {
            text: text.to_owned(),
        };

        let mut parts = text.splitn(3, '/');
        let at = parts.next().ok_or_else(not_a_cursor)?;
        let node_id = parts.next().ok_or_else(not_a_cursor)?;
        let node_sequence = parts.next().ok_or_else(not_a_cursor)?;

        let at: i64 = at.parse().map_err(|_| not_a_cursor())?;
        let at = chrono::DateTime::from_timestamp_micros(at).ok_or_else(not_a_cursor)?;
        let node_id = node_id.parse().map_err(|_| not_a_cursor())?;
        let node_sequence = node_sequence.parse().map_err(|_| not_a_cursor())?;

        Ok(Self {
            at,
            node_id,
            node_sequence,
        })
    }
}

impl waymark_cursor_core::EncodeCursor for TailCursor {
    fn encode(&self) -> String {
        self.node_sequence.to_string()
    }
}

impl waymark_cursor_core::DecodeCursor for TailCursor {
    type Error = ParseCursorError;

    fn decode(text: &str) -> Result<Self, ParseCursorError> {
        let node_sequence = text.parse().map_err(|_| ParseCursorError {
            text: text.to_owned(),
        })?;

        Ok(Self { node_sequence })
    }
}

/// Bring a page size into the `LIMIT` domain, capped at its top: a
/// caller asking for more rows than `i64::MAX` gets the most the query
/// can express, not an error.
fn to_limit(limit: std::num::NonZeroUsize) -> i64 {
    let limit = u64::try_from(limit.get()).unwrap_or(u64::MAX);
    to_bigint_saturating(limit)
}

impl waymark_observability_events_query_backend::HasNodeId for Store {
    type NodeId = waymark_ids::NodeId;
}

impl waymark_observability_events_query_backend::HasPayload for Store {
    type Payload = waymark_observability_events_payload::Payload;
}

impl waymark_observability_events_query_backend::ListEvents for Store {
    type Cursor = ListCursor;

    type Error = sqlx::Error;

    async fn list_events(
        &self,
        params: list_events::Params<ListCursor>,
    ) -> Result<
        Option<waymark_observability_events_query_backend::PageFor<Self, ListCursor>>,
        sqlx::Error,
    > {
        let mut query = sqlx::QueryBuilder::new(format!(
            r#"
            SELECT {EVENT_COLUMNS}
            FROM observability_events
            WHERE at >= "#,
        ));
        query.push_bind(params.from);
        query.push(" AND at < ");
        query.push_bind(params.to);
        // Keyset: strictly before the position in the (descending) order.
        if let Some(after) = params.after {
            query.push(" AND (at, node_id, node_sequence) < (");
            query.push_bind(after.at);
            query.push(", ");
            query.push_bind(after.node_id);
            query.push(", ");
            query.push_bind(after.node_sequence);
            query.push(")");
        }
        query.push(" ORDER BY at DESC, node_id DESC, node_sequence DESC LIMIT ");
        query.push_bind(to_limit(params.limit));

        let rows = query.build().fetch_all(&self.pool).await?;
        let events = rows
            .iter()
            .map(decode_event)
            .collect::<Result<Vec<_>, _>>()?;
        let Some(events) = NEVec::try_from_vec(events) else {
            return Ok(None);
        };
        let last = events.last();
        let next = ListCursor {
            at: last.at,
            node_id: last.node_id,
            node_sequence: to_bigint_saturating(last.node_sequence.get()),
        };

        Ok(Some(waymark_observability_events_query_backend::Page {
            events,
            next,
        }))
    }
}

impl waymark_observability_events_query_backend::Tail for Store {
    type Cursor = TailCursor;

    type Error = sqlx::Error;

    async fn tail(
        &self,
        params: tail::Params<waymark_ids::NodeId, TailCursor>,
    ) -> Result<
        Option<waymark_observability_events_query_backend::PageFor<Self, TailCursor>>,
        sqlx::Error,
    > {
        let mut query = sqlx::QueryBuilder::new(format!(
            r#"
            SELECT {EVENT_COLUMNS}
            FROM observability_events
            WHERE node_id = "#,
        ));
        query.push_bind(params.node_id);
        // Keyset: strictly past the position in stream order.
        if let Some(after) = params.after {
            query.push(" AND node_sequence > ");
            query.push_bind(after.node_sequence);
        }
        query.push(" ORDER BY node_sequence LIMIT ");
        query.push_bind(to_limit(params.limit));

        let rows = query.build().fetch_all(&self.pool).await?;
        let events = rows
            .iter()
            .map(decode_event)
            .collect::<Result<Vec<_>, _>>()?;
        let Some(events) = NEVec::try_from_vec(events) else {
            return Ok(None);
        };
        let next = TailCursor {
            node_sequence: to_bigint_saturating(events.last().node_sequence.get()),
        };

        Ok(Some(waymark_observability_events_query_backend::Page {
            events,
            next,
        }))
    }
}

#[cfg(test)]
mod tests;
