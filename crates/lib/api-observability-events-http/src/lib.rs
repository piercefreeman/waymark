//! The observability-events API HTTP transport: routes over the
//! observability-events query backend, with transport-owned wire types.

#![warn(missing_docs)]

use std::sync::Arc;

mod common;
mod list_events;
mod tail;

pub use self::common::*;
pub use self::list_events::ListEventsQuery;
pub use self::tail::{TailPath, TailQuery};

/// The routes of the observability-events domain, over `backend`, under
/// the domain's own `/observability-events` prefix.
pub fn router<Backend>(backend: Arc<Backend>) -> aide::axum::ApiRouter
where
    Backend: waymark_observability_events_query_backend::ListEvents,
    Backend: waymark_observability_events_query_backend::Tail,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: waymark_observability_events_core::Kinded + serde::Serialize,
    Backend: Send + Sync + 'static,
{
    let routes = aide::axum::ApiRouter::new()
        .merge(list_events::router())
        .merge(tail::router());

    aide::axum::ApiRouter::new()
        .nest("/observability-events", routes)
        .with_state(backend)
}

#[cfg(test)]
mod tests;
