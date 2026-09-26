//! The timeline operation: one VM's events, across every node it ran on,
//! oldest first.

use std::sync::Arc;

use aide::axum::routing::*;

use crate::common::Event;

/// Query parameters of a timeline read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(bound(deserialize = "Cursor: waymark_http_api_types::DecodeCursor"))]
#[schemars(bound = "")]
pub struct VmTimelineQuery<Cursor> {
    /// At most this many events.
    pub limit: waymark_http_api_types::Limit<1000>,

    /// The `next` of the previous page, to resume past it; absent to
    /// start from the VM's oldest event the store holds.
    pub after: Option<waymark_http_api_types::Cursor<Cursor>>,
}

/// Path parameters of a timeline read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct VmTimelinePath {
    /// The VM's id.
    pub vm_id: waymark_http_api_types::UuidId<waymark_ids::InstanceId>,
}

async fn handler<Backend>(
    axum::extract::Path(path): axum::extract::Path<VmTimelinePath>,
    axum::extract::Query(query): axum::extract::Query<VmTimelineQuery<Backend::Cursor>>,
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<
    axum::Json<waymark_http_api_types::Page<Event<Backend::Payload>, Backend::Cursor>>,
    axum::http::StatusCode,
>
where
    Backend: waymark_observability_events_query_backend::VmTimeline,
    Backend::Cursor: waymark_http_api_types::CursorCodec,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: crate::PayloadBounds,
{
    let params = waymark_observability_events_query_backend::vm_timeline::Params {
        vm_id: path.vm_id.into(),
        limit: query.limit.into(),
        after: query
            .after
            .map(|waymark_http_api_types::Cursor(after)| after),
    };

    let page = match backend.vm_timeline(params).await {
        Ok(page) => page,
        Err(error) => {
            tracing::error!(?error, "failed to read a VM's timeline");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let page = crate::common::page(page);

    Ok(axum::Json(page))
}

/// The route of the timeline operation, relative to the domain.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_observability_events_query_backend::VmTimeline,
    Backend::Cursor: waymark_http_api_types::CursorCodec,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend::Payload: crate::PayloadBounds,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/vms/{vm_id}/timeline", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("One VM's events across the nodes it ran on, oldest first, one page at a time.")
        .description(
            "Ordered by time, then by node, then by position in the node's stream: the VM's \
             runs may sit on different nodes, and their streams are merged by time. Paging \
             never skips or repeats an event. Pages are not a live follow: an event that \
             arrives late, dated inside a page already read, is not returned by later pages.",
        )
        .response_with::<400, String, _>(|response| {
            response.description("A parameter could not be read; the reason, as text.")
        })
        .response_with::<500, (), _>(|response| {
            response.description("The events could not be read from the store.")
        })
}
