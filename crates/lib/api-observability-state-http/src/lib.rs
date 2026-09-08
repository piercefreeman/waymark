//! The observability-state API HTTP transport: routes over the
//! observability-state query backend, with transport-owned wire types.

#![warn(missing_docs)]

use std::sync::Arc;

mod common;
mod get_instance;
mod list_instances;

pub use self::common::*;
pub use self::get_instance::GetInstancePath;
pub use self::list_instances::ListInstancesQuery;

/// The routes of the observability-state domain, over `backend`, under
/// the domain's own `/observability-state` prefix.
pub fn router<Backend>(backend: Arc<Backend>) -> aide::axum::ApiRouter
where
    Backend: waymark_observability_state_query_backend::ListInstances,
    Backend: waymark_observability_state_query_backend::GetInstance,
    Backend: Send + Sync + 'static,
{
    let routes = aide::axum::ApiRouter::new()
        .merge(list_instances::router())
        .merge(get_instance::router());

    aide::axum::ApiRouter::new()
        .nest("/observability-state", routes)
        .with_state(backend)
}

#[cfg(test)]
mod tests;
