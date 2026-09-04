//! The list operation: events across every node, newest first.

use std::sync::Arc;

use aide::axum::routing::*;

use crate::common::Page;

/// Query parameters of a list read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ListEventsQuery {
    /// Inclusive start of the time range.
    pub from: chrono::DateTime<chrono::Utc>,

    /// Exclusive end of the time range.
    pub to: chrono::DateTime<chrono::Utc>,

    /// At most this many events.
    pub limit: usize,

    /// The `next` of the previous page, to resume past it; absent for
    /// the first page.
    pub after: Option<String>,
}

async fn handler<Backend>(
    axum::extract::Query(query): axum::extract::Query<ListEventsQuery>,
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<axum::Json<Page>, axum::http::StatusCode>
where
    Backend: waymark_observability_events_query_backend::ListEvents,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: waymark_observability_events_core::Kinded + serde::Serialize,
{
    let Some(limit) = std::num::NonZeroUsize::new(query.limit) else {
        return Err(axum::http::StatusCode::BAD_REQUEST);
    };

    let after = match query.after {
        None => None,
        Some(text) => match waymark_cursor_core::DecodeCursor::decode(&text) {
            Ok(cursor) => Some(cursor),
            Err(_) => return Err(axum::http::StatusCode::BAD_REQUEST),
        },
    };

    let params = waymark_observability_events_query_backend::list_events::Params {
        from: query.from,
        to: query.to,
        limit,
        after,
    };

    let page = match backend.list_events(params).await {
        Ok(page) => page,
        Err(error) => {
            tracing::error!(?error, "failed to list events");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let page = match crate::common::page(page) {
        Ok(page) => page,
        Err(error) => {
            tracing::error!(?error, "failed to serialize an event payload");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(axum::Json(page))
}

/// The route of the list operation, relative to the domain.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_observability_events_query_backend::ListEvents,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: waymark_observability_events_core::Kinded + serde::Serialize,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("Events across every node in a time range, newest first, one page at a time.")
        .response::<400, ()>()
        .response::<500, ()>()
}
