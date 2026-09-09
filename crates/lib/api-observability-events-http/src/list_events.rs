//! The list operation: events across every node, newest first.

use std::sync::Arc;

use aide::axum::routing::*;

use crate::common::Event;

/// Query parameters of a list read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(bound(deserialize = "Cursor: waymark_cursor_core::DecodeCursor"))]
#[schemars(bound = "")]
pub struct ListEventsQuery<Cursor> {
    /// Inclusive start of the time range.
    pub from: chrono::DateTime<chrono::Utc>,

    /// Exclusive end of the time range.
    pub to: chrono::DateTime<chrono::Utc>,

    /// At most this many events.
    pub limit: waymark_http_api_types::Limit<1000>,

    /// The `next` of the previous page, to resume past it; absent for
    /// the first page.
    pub after: Option<waymark_http_api_types::Cursor<Cursor>>,
}

async fn handler<Backend>(
    axum::extract::Query(query): axum::extract::Query<ListEventsQuery<Backend::Cursor>>,
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<
    axum::Json<waymark_http_api_types::Page<Event<Backend::Payload>, Backend::Cursor>>,
    axum::http::StatusCode,
>
where
    Backend: waymark_observability_events_query_backend::ListEvents,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload:
        waymark_observability_events_core::Kinded + serde::Serialize + schemars::JsonSchema,
{
    let params = waymark_observability_events_query_backend::list_events::Params {
        from: query.from,
        to: query.to,
        limit: query.limit.into(),
        after: query
            .after
            .map(|waymark_http_api_types::Cursor(after)| after),
    };

    let page = match backend.list_events(params).await {
        Ok(page) => page,
        Err(error) => {
            tracing::error!(?error, "failed to list events");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let page = crate::common::page(page);

    Ok(axum::Json(page))
}

/// The route of the list operation, relative to the domain.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_observability_events_query_backend::ListEvents,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload:
        waymark_observability_events_core::Kinded + serde::Serialize + schemars::JsonSchema,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("Events across every node in a time range, newest first, one page at a time.")
        .response_with::<400, String, _>(|response| {
            response.description("A parameter could not be read; the reason, as text.")
        })
        .response::<500, ()>()
}
