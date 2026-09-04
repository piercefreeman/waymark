//! The tail operation: one node's stream, followed forward.

use std::sync::Arc;

use aide::axum::routing::*;

use crate::common::Event;

/// Query parameters of a tail read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(bound(deserialize = "Cursor: waymark_cursor_core::DecodeCursor"))]
#[schemars(bound = "")]
pub struct TailQuery<Cursor> {
    /// At most this many events.
    pub limit: waymark_http_api_types::Limit<1000>,

    /// The `next` of the previous page, to resume past it; absent to
    /// start from the oldest event the store holds.
    pub after: Option<waymark_http_api_types::Cursor<Cursor>>,
}

/// Path parameters of a tail read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TailPath {
    /// The node's id.
    pub node_id: waymark_http_api_types::UuidId<waymark_ids::NodeId>,
}

async fn handler<Backend>(
    axum::extract::Path(path): axum::extract::Path<TailPath>,
    axum::extract::Query(query): axum::extract::Query<TailQuery<Backend::Cursor>>,
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<
    axum::Json<waymark_http_api_types::Page<Event<Backend::Payload>, Backend::Cursor>>,
    axum::http::StatusCode,
>
where
    Backend: waymark_observability_events_query_backend::Tail,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload:
        waymark_observability_events_core::Kinded + serde::Serialize + schemars::JsonSchema,
{
    let params = waymark_observability_events_query_backend::tail::Params {
        node_id: path.node_id.into(),
        limit: query.limit.into(),
        after: query
            .after
            .map(|waymark_http_api_types::Cursor(after)| after),
    };

    let page = match backend.tail(params).await {
        Ok(page) => page,
        Err(error) => {
            tracing::error!(?error, "failed to tail a node's events");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let page = crate::common::page(page);

    Ok(axum::Json(page))
}

/// The route of the tail operation, relative to the domain.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_observability_events_query_backend::Tail,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload:
        waymark_observability_events_core::Kinded + serde::Serialize + schemars::JsonSchema,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/nodes/{node_id}/tail", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("One node's stream in its own order, one page at a time.")
        .response_with::<400, String, _>(|response| {
            response.description("A parameter could not be read; the reason, as text.")
        })
        .response::<500, ()>()
}
