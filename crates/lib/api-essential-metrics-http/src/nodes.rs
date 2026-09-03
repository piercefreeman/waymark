//! The nodes resource: per-node sample reads.

use std::sync::Arc;

mod common;
mod latest;
mod series;

pub use self::common::*;

/// The routes of the nodes resource, relative to its mount point.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_essential_metrics_query_backend::Latest,
    Backend: waymark_essential_metrics_query_backend::Series,
    Backend: waymark_essential_metrics_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new()
        .merge(latest::router())
        .merge(series::router())
}
