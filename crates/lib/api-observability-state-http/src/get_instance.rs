//! The get operation: one VM, by id.

use std::sync::Arc;

use aide::axum::routing::*;

use crate::common::Instance;

/// Path parameters of a get read.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetInstancePath {
    /// The VM's id.
    pub vm_id: waymark_http_api_types::UuidId<waymark_ids::InstanceId>,
}

async fn handler<Backend>(
    axum::extract::Path(path): axum::extract::Path<GetInstancePath>,
    axum::extract::State(backend): axum::extract::State<Arc<Backend>>,
) -> Result<axum::Json<Instance>, axum::http::StatusCode>
where
    Backend: waymark_observability_state_query_backend::GetInstance,
{
    let state = match backend.get_instance(path.vm_id.into()).await {
        Ok(Some(state)) => state,
        Ok(None) => return Err(axum::http::StatusCode::NOT_FOUND),
        Err(error) => {
            tracing::error!(?error, "failed to read a VM's instance state");
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(axum::Json(state.into()))
}

/// The route of the get operation, relative to the domain.
pub fn router<Backend>() -> aide::axum::ApiRouter<Arc<Backend>>
where
    Backend: waymark_observability_state_query_backend::GetInstance,
    Backend: Send + Sync + 'static,
{
    aide::axum::ApiRouter::new().api_route("/instances/{vm_id}", get_with(handler, docs))
}

fn docs(op: aide::transform::TransformOperation) -> aide::transform::TransformOperation {
    op.summary("What the events show about one VM.")
        .response_with::<400, String, _>(|response| {
            response.description("A parameter could not be read; the reason, as text.")
        })
        .response::<404, ()>()
        .response::<500, ()>()
}
