//! The latest operation: every node's newest sample.

use std::sync::Arc;

use aide::axum::routing::*;

use super::common::NodeSample;

async fn handler<Backend>(
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<axum::Json<Vec<NodeSample>>, axum::http::StatusCode>
where
    Backend: waymark_essential_metrics_query_backend::Latest,
    Backend: waymark_essential_metrics_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
{
    let samples = match backend.latest().await {
        Ok(samples) => samples,
        Err(error) => {
            tracing::error!(?error, "failed to read the latest node samples");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let samples = samples
        .into_iter()
        .map(super::common::node_sample)
        .collect();
    Ok(axum::Json(samples))
}

/// The route of the latest operation, relative to the nodes resource.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_essential_metrics_query_backend::Latest,
    Backend: waymark_essential_metrics_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/latest", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("Every node's most recent sample.")
        .response::<500, ()>()
}
