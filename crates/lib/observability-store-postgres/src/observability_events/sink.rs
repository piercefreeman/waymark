//! The sink side: appending events.

use nonempty_collections::NESlice;

use super::common::{EVENT_COLUMNS, Event};
use crate::Store;
use crate::common::to_bigint_saturating;

/// Bind one event's columns, in [`EVENT_COLUMNS`] order.
///
/// Generic over the payload — the binding does not depend on it, and
/// the store's own payload may have no variants yet, which would leave
/// a concrete body with nothing live after the kind is derived.
fn push_event<'args, Payload>(
    row: &mut sqlx::query_builder::Separated<'_, 'args, sqlx::Postgres, &'static str>,
    event: &'args waymark_observability_events_core::Event<waymark_ids::NodeId, Payload>,
) where
    Payload: waymark_observability_events_core::Kinded + serde::Serialize,
{
    let kind = waymark_observability_events_core::Kinded::kind(&event.payload);
    let kind = waymark_observability_events_core::kind::Tagged::tag(kind);
    row.push_bind(event.node_id)
        .push_bind(to_bigint_saturating(event.node_sequence.get()))
        .push_bind(event.at)
        .push_bind(kind)
        .push_bind(sqlx::types::Json(&event.payload));
}

impl waymark_observability_events_sink_backend::HasNodeId for Store {
    type NodeId = waymark_ids::NodeId;
}

impl waymark_observability_events_sink_backend::HasPayload for Store {
    type Payload = waymark_observability_events_payload::Payload;
}

impl waymark_observability_events_sink_backend::AppendEvents for Store {
    type Error = sqlx::Error;

    async fn append_events(&self, events: NESlice<'_, Event>) -> Result<(), sqlx::Error> {
        let mut query = sqlx::QueryBuilder::new(format!(
            r#"
            INSERT INTO observability_events ({EVENT_COLUMNS})
            "#,
        ));
        query.push_values(events.iter(), |mut row, event| push_event(&mut row, event));
        query.build().execute(&self.pool).await?;
        Ok(())
    }
}
