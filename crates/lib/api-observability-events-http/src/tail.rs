//! The tail operation: one node's stream, followed forward.

use std::sync::Arc;

use aide::axum::routing::*;

use crate::common::Page;

/// Query parameters of a tail read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TailQuery {
    /// At most this many events.
    pub limit: usize,

    /// The `next` of the previous page, to resume past it; absent to
    /// start from the oldest event the store holds.
    pub after: Option<String>,
}

/// Path parameters of a tail read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TailPath {
    /// The node's id (a UUID).
    pub node_id: String,
}

async fn handler<Backend>(
    axum::extract::Path(path): axum::extract::Path<TailPath>,
    axum::extract::Query(query): axum::extract::Query<TailQuery>,
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<axum::Json<Page>, axum::http::StatusCode>
where
    Backend: waymark_observability_events_query_backend::Tail,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: waymark_observability_events_core::EventKind + serde::Serialize,
    <Backend::Payload as waymark_observability_events_core::EventKind>::Kind: Into<&'static str>,
{
    let node_id = match path.node_id.parse::<waymark_ids::NodeId>() {
        Ok(node_id) => node_id,
        Err(_) => return Err(axum::http::StatusCode::BAD_REQUEST),
    };

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

    let params = waymark_observability_events_query_backend::tail::Params {
        node_id,
        limit,
        after,
    };

    let page = match backend.tail(params).await {
        Ok(page) => page,
        Err(error) => {
            tracing::error!(?error, "failed to tail a node's events");
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

/// The route of the tail operation, relative to the domain.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_observability_events_query_backend::Tail,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: waymark_observability_events_core::EventKind + serde::Serialize,
    <Backend::Payload as waymark_observability_events_core::EventKind>::Kind: Into<&'static str>,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/nodes/{node_id}/tail", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("One node's stream in its own order, one page at a time.")
        .response::<400, ()>()
        .response::<500, ()>()
}
