//! The essential-metrics API HTTP transport: routes over the
//! essential-metrics query backend, with transport-owned wire types.

#![warn(missing_docs)]

use std::sync::Arc;

pub mod nodes;

/// The routes of the essential-metrics domain, over `backend`, under
/// the domain's own `/essential-metrics` prefix.
pub fn router<Backend>(backend: Arc<Backend>) -> aide::axum::ApiRouter
where
    Backend: waymark_essential_metrics_query_backend::Latest,
    Backend: waymark_essential_metrics_query_backend::Series,
    Backend: waymark_essential_metrics_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: Send + Sync + 'static,
{
    let routes = aide::axum::ApiRouter::new().nest("/nodes", nodes::router());

    aide::axum::ApiRouter::new()
        .nest("/essential-metrics", routes)
        .with_state(backend)
}

#[cfg(test)]
mod tests;
