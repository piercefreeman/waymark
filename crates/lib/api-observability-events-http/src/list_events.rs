//! The list operation: events across every node, newest first.

use std::sync::Arc;

use aide::axum::routing::*;

use crate::common::Event;

/// Query parameters of a list read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(bound(deserialize = "Cursor: waymark_http_api_types::DecodeCursor"))]
#[schemars(bound = "")]
pub struct ListEventsQuery<Cursor> {
    /// Inclusive start of the time range.
    pub from: waymark_http_api_types::Timestamp,

    /// Exclusive end of the time range.
    pub to: waymark_http_api_types::Timestamp,

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
    Backend::Cursor: waymark_http_api_types::CursorCodec,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: crate::PayloadBounds,
{
    let params = waymark_observability_events_query_backend::list_events::Params {
        from: query.from.into(),
        to: query.to.into(),
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
    Backend::Cursor: waymark_http_api_types::CursorCodec,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: crate::PayloadBounds,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("Events across every node in a time range, newest first, one page at a time.")
        .description(
            "Ordered by time, then by node, then by position in the node's stream, so paging \
             never skips or repeats an event. Keep `from` and `to` fixed while paging: a cursor \
             only means something with the range it was read with. Pages are not a live \
             follow: an event that arrives late, dated inside a page already read, is not \
             returned by later pages; to follow a node's stream, tail it.",
        )
        .response_with::<400, String, _>(|response| {
            response.description("A parameter could not be read; the reason, as text.")
        })
        .response_with::<500, (), _>(|response| {
            response.description("The events could not be read from the store.")
        })
}
