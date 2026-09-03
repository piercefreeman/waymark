//! The series operation: one node's samples over a time range,
//! bucketed.

use std::sync::Arc;

use aide::axum::routing::*;

use super::common::NodeSample;

/// Query parameters of a series read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SeriesQuery {
    /// Inclusive start of the time range.
    pub from: chrono::DateTime<chrono::Utc>,

    /// Exclusive end of the time range.
    pub to: chrono::DateTime<chrono::Utc>,

    /// Bucket width in seconds; samples within one bucket are
    /// aggregated.
    pub bucket_seconds: u64,
}

/// Path parameters of a series read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SeriesPath {
    /// The node's id (a UUID).
    pub node_id: String,
}

async fn handler<Backend>(
    axum::extract::Path(path): axum::extract::Path<SeriesPath>,
    axum::extract::Query(query): axum::extract::Query<SeriesQuery>,
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<axum::Json<Vec<NodeSample>>, axum::http::StatusCode>
where
    Backend: waymark_essential_metrics_query_backend::Series,
    Backend: waymark_essential_metrics_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
{
    let node_id = match path.node_id.parse::<uuid::Uuid>() {
        Ok(uuid) => uuid,
        Err(_) => return Err(axum::http::StatusCode::BAD_REQUEST),
    };
    let node_id = match waymark_ids::NodeId::try_from(node_id) {
        Ok(node_id) => node_id,
        Err(_) => return Err(axum::http::StatusCode::BAD_REQUEST),
    };

    let Some(bucket) = waymark_nonzero_duration::NonZeroDuration::from_secs(query.bucket_seconds)
    else {
        return Err(axum::http::StatusCode::BAD_REQUEST);
    };

    let params = waymark_essential_metrics_query_backend::series::Params {
        node_id,
        from: query.from,
        to: query.to,
        bucket,
    };

    let samples = match backend.series(params).await {
        Ok(samples) => samples,
        Err(error) => {
            tracing::error!(?error, "failed to read a node's sample series");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let samples = samples
        .into_iter()
        .map(super::common::node_sample)
        .collect();
    Ok(axum::Json(samples))
}

/// The route of the series operation, relative to the nodes resource.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_essential_metrics_query_backend::Series,
    Backend: waymark_essential_metrics_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/{node_id}/series", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("One node's samples over a time range, bucketed.")
        .response::<400, ()>()
        .response::<500, ()>()
}
