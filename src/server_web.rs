//! HTTP API server for workflow registration and monitoring.

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result as AnyResult;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::error;

use crate::{
    instances,
    server_client::{HEALTH_PATH, REGISTER_PATH, WAIT_PATH, sanitize_interval},
    store::Store,
};

#[derive(Clone)]
pub struct HttpState {
    pub service_name: &'static str,
    pub http_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    pub database_url: Arc<String>,
    pub store: Arc<Store>,
}

impl HttpState {
    pub fn new(
        service_name: &'static str,
        http_addr: SocketAddr,
        grpc_addr: SocketAddr,
        database_url: Arc<String>,
        store: Arc<Store>,
    ) -> Self {
        Self {
            service_name,
            http_addr,
            grpc_addr,
            database_url,
            store,
        }
    }
}

pub async fn run_http_server(listener: TcpListener, state: HttpState) -> AnyResult<()> {
    let app = Router::new()
        .route(HEALTH_PATH, get(healthz))
        .route(REGISTER_PATH, post(register_workflow_http))
        .route(WAIT_PATH, post(wait_for_instance_http))
        .with_state(state);
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    http_port: u16,
    grpc_port: u16,
}

#[derive(Debug, Deserialize)]
struct RegisterWorkflowHttpRequest {
    registration_b64: String,
}

#[derive(Debug, Serialize)]
struct RegisterWorkflowHttpResponse {
    workflow_version_id: String,
    workflow_instance_id: String,
}

#[derive(Debug, Deserialize)]
struct WaitForInstanceHttpRequest {
    instance_id: String,
    poll_interval_secs: Option<f64>,
}

#[derive(Debug, Serialize)]
struct WaitForInstanceHttpResponse {
    payload_b64: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponseBody {
    message: String,
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }

    fn internal(err: anyhow::Error) -> Self {
        error!(?err, "request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal server error".to_string(),
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let body = Json(ErrorResponseBody {
            message: self.message,
        });
        (self.status, body).into_response()
    }
}

async fn healthz(State(state): State<HttpState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: state.service_name,
        http_port: state.http_addr.port(),
        grpc_port: state.grpc_addr.port(),
    })
}

async fn register_workflow_http(
    State(state): State<HttpState>,
    Json(payload): Json<RegisterWorkflowHttpRequest>,
) -> Result<Json<RegisterWorkflowHttpResponse>, HttpError> {
    if payload.registration_b64.is_empty() {
        return Err(HttpError::bad_request("registration payload missing"));
    }
    let bytes = general_purpose::STANDARD
        .decode(payload.registration_b64)
        .map_err(|_| HttpError::bad_request("invalid base64 payload"))?;
    let database_url = state.database_url.clone();
    let (version_id, instance_id) = instances::run_instance_payload(database_url.as_ref(), &bytes)
        .await
        .map_err(HttpError::internal)?;
    Ok(Json(RegisterWorkflowHttpResponse {
        workflow_version_id: version_id.to_string(),
        workflow_instance_id: instance_id.to_string(),
    }))
}

async fn wait_for_instance_http(
    State(state): State<HttpState>,
    Json(request): Json<WaitForInstanceHttpRequest>,
) -> Result<Json<WaitForInstanceHttpResponse>, HttpError> {
    let interval = sanitize_interval(request.poll_interval_secs);
    let instance_id = request
        .instance_id
        .parse()
        .map_err(|_| HttpError::bad_request("invalid instance_id"))?;
    let database_url = state.database_url.clone();
    let payload = instances::wait_for_instance_poll(database_url.as_ref(), instance_id, interval)
        .await
        .map_err(HttpError::internal)?;
    let Some(bytes) = payload else {
        return Err(HttpError::not_found("no workflow instance available"));
    };
    let encoded = general_purpose::STANDARD.encode(bytes);
    Ok(Json(WaitForInstanceHttpResponse {
        payload_b64: encoded,
    }))
}
