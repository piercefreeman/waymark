use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose};
use carabiner::{
    instances,
    messages::proto::{
        self,
        workflow_service_server::{WorkflowService, WorkflowServiceServer},
    },
    server,
};
use clap::Parser;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response as GrpcResponse, Status, transport::Server};
use tracing::{error, info};

const REGISTER_PATH: &str = "/v1/workflows/register";
const WAIT_PATH: &str = "/v1/workflows/wait";

#[derive(Parser, Debug)]
#[command(
    name = "carabiner-server",
    about = "Expose carabiner workflow operations over HTTP and gRPC"
)]
struct Args {
    /// HTTP address to bind, e.g. 127.0.0.1:24117
    #[arg(long)]
    http_addr: Option<SocketAddr>,
    /// gRPC address to bind, defaults to HTTP port + 1 when omitted.
    #[arg(long)]
    grpc_addr: Option<SocketAddr>,
}

#[derive(Clone)]
struct HttpState {
    http_addr: SocketAddr,
    grpc_addr: SocketAddr,
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
    database_url: String,
    registration_b64: String,
}

#[derive(Debug, Serialize)]
struct RegisterWorkflowHttpResponse {
    workflow_version_id: i64,
}

#[derive(Debug, Deserialize)]
struct WaitForInstanceHttpRequest {
    database_url: String,
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

#[derive(Clone)]
struct WorkflowGrpcService;

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        http_addr,
        grpc_addr,
    } = Args::parse();
    tracing_subscriber::fmt::init();

    let http_addr = http_addr.unwrap_or_else(server::default_http_addr);
    let grpc_addr = grpc_addr
        .unwrap_or_else(|| SocketAddr::new(http_addr.ip(), http_addr.port().saturating_add(1)));

    let http_listener = TcpListener::bind(http_addr)
        .await
        .with_context(|| format!("failed to bind http listener on {http_addr}"))?;
    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .with_context(|| format!("failed to bind grpc listener on {grpc_addr}"))?;

    info!(?http_addr, ?grpc_addr, "carabiner server listening");

    let http_state = HttpState {
        http_addr,
        grpc_addr,
    };

    let http_task = tokio::spawn(run_http_server(http_listener, http_state));
    let grpc_task = tokio::spawn(run_grpc_server(grpc_listener));

    tokio::select! {
        result = http_task => result??,
        result = grpc_task => result??,
    }

    Ok(())
}

async fn run_http_server(listener: TcpListener, state: HttpState) -> Result<()> {
    let app = Router::new()
        .route(server::HEALTH_PATH, get(healthz))
        .route(REGISTER_PATH, post(register_workflow_http))
        .route(WAIT_PATH, post(wait_for_instance_http))
        .with_state(state);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_grpc_server(listener: TcpListener) -> Result<()> {
    let incoming = TcpListenerStream::new(listener);
    Server::builder()
        .add_service(WorkflowServiceServer::new(WorkflowGrpcService))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

async fn healthz(State(state): State<HttpState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: server::SERVICE_NAME,
        http_port: state.http_addr.port(),
        grpc_port: state.grpc_addr.port(),
    })
}

async fn register_workflow_http(
    Json(payload): Json<RegisterWorkflowHttpRequest>,
) -> Result<Json<RegisterWorkflowHttpResponse>, HttpError> {
    if payload.registration_b64.is_empty() {
        return Err(HttpError::bad_request("registration payload missing"));
    }
    let bytes = general_purpose::STANDARD
        .decode(payload.registration_b64)
        .map_err(|_| HttpError::bad_request("invalid base64 payload"))?;
    let version_id = instances::run_instance_payload(&payload.database_url, &bytes)
        .await
        .map_err(HttpError::internal)?;
    Ok(Json(RegisterWorkflowHttpResponse {
        workflow_version_id: version_id,
    }))
}

async fn wait_for_instance_http(
    Json(request): Json<WaitForInstanceHttpRequest>,
) -> Result<Json<WaitForInstanceHttpResponse>, HttpError> {
    let interval = sanitize_interval(request.poll_interval_secs);
    let payload = instances::wait_for_instance_poll(&request.database_url, interval)
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

#[tonic::async_trait]
impl WorkflowService for WorkflowGrpcService {
    async fn register_workflow(
        &self,
        request: Request<proto::RegisterWorkflowRequest>,
    ) -> Result<GrpcResponse<proto::RegisterWorkflowResponse>, Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| Status::invalid_argument("registration missing"))?;
        let payload = registration.encode_to_vec();
        let version_id = instances::run_instance_payload(&inner.database_url, &payload)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(GrpcResponse::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id,
        }))
    }

    async fn wait_for_instance(
        &self,
        request: Request<proto::WaitForInstanceRequest>,
    ) -> Result<GrpcResponse<proto::WaitForInstanceResponse>, Status> {
        let inner = request.into_inner();
        let interval = sanitize_interval(Some(inner.poll_interval_secs));
        let payload = instances::wait_for_instance_poll(&inner.database_url, interval)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let bytes = payload.ok_or_else(|| Status::not_found("no workflow instance available"))?;
        Ok(GrpcResponse::new(proto::WaitForInstanceResponse {
            payload: bytes,
        }))
    }
}

fn sanitize_interval(value: Option<f64>) -> Duration {
    let raw = value.unwrap_or(1.0);
    let clamped = raw.clamp(0.1, 30.0);
    Duration::from_secs_f64(clamped)
}
