//! The list operation: every VM last active in a time range, most
//! recently active first.

use std::sync::Arc;

use aide::axum::routing::*;

use crate::common::Instance;

/// Query parameters of a list read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(bound(deserialize = "Cursor: waymark_cursor_core::DecodeCursor"))]
#[schemars(bound = "")]
pub struct ListInstancesQuery<Cursor> {
    /// Inclusive start of the time range a VM's last activity must fall
    /// in to be listed.
    pub from: chrono::DateTime<chrono::Utc>,

    /// Exclusive end of that time range.
    pub to: chrono::DateTime<chrono::Utc>,

    /// At most this many instances.
    pub limit: waymark_http_api_types::Limit<100>,

    /// The `next` of the previous page, to resume past it; absent for
    /// the first page.
    pub after: Option<waymark_http_api_types::Cursor<Cursor>>,
}

async fn handler<Backend>(
    axum::extract::Query(query): axum::extract::Query<ListInstancesQuery<Backend::Cursor>>,
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<
    axum::Json<waymark_http_api_types::Page<Instance, Backend::Cursor>>,
    axum::http::StatusCode,
>
where
    Backend: waymark_observability_state_query_backend::ListInstances,
{
    let params = waymark_observability_state_query_backend::list_instances::Params {
        from: query.from,
        to: query.to,
        limit: query.limit.into(),
        after: query
            .after
            .map(|waymark_http_api_types::Cursor(after)| after),
    };

    let page = match backend.list_instances(params).await {
        Ok(page) => page,
        Err(error) => {
            tracing::error!(?error, "failed to list instances");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(axum::Json(crate::common::page(page)))
}

/// The route of the list operation, relative to the domain.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_observability_state_query_backend::ListInstances,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/instances", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary(
        "Every VM last active in a time range, most recently active first, one page at a time.",
    )
    .response_with::<400, String, _>(|response| {
        response.description("A parameter could not be read; the reason, as text.")
    })
    .response::<500, ()>()
}
