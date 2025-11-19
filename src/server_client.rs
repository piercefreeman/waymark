use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tera::Tera;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response as GrpcResponse, Status, async_trait, transport::Server};
use tracing::info;

use crate::{
    db::Database,
    instances,
    messages::proto::{self, workflow_service_server::WorkflowServiceServer},
    server_web,
};
use prost::Message;

pub const SERVICE_NAME: &str = "carabiner";
pub const HEALTH_PATH: &str = "/healthz";

pub const REGISTER_PATH: &str = "/v1/workflows/register";
pub const WAIT_PATH: &str = "/v1/workflows/wait";

pub fn health_url(host: &str, port: u16) -> String {
    format!("http://{host}:{port}{HEALTH_PATH}")
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub http_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    pub database_url: String,
}

pub async fn run_servers(config: ServerConfig) -> Result<()> {
    let ServerConfig {
        http_addr,
        grpc_addr,
        database_url,
    } = config;
    let database_url = Arc::new(database_url);
    let database = Database::connect(database_url.as_ref())
        .await
        .with_context(|| {
            format!(
                "failed to connect to dashboard database at {}",
                database_url.as_ref()
            )
        })?;
    let mut templates = Tera::new("templates/**/*.html")
        .context("failed to initialize templates from templates/ directory")?;
    templates.autoescape_on(vec![".html", ".tera"]);
    let templates = Arc::new(templates);

    let http_listener = TcpListener::bind(http_addr)
        .await
        .with_context(|| format!("failed to bind http listener on {http_addr}"))?;
    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .with_context(|| format!("failed to bind grpc listener on {grpc_addr}"))?;

    info!(?http_addr, ?grpc_addr, "carabiner server listening");

    let http_state = server_web::HttpState::new(
        SERVICE_NAME,
        http_addr,
        grpc_addr,
        Arc::clone(&database_url),
        database,
        templates,
    );

    let http_task = tokio::spawn(server_web::run_http_server(http_listener, http_state));
    let grpc_task = tokio::spawn(run_grpc_server(grpc_listener, database_url));

    tokio::select! {
        result = http_task => result??,
        result = grpc_task => result??,
    }

    Ok(())
}
#[derive(Clone)]
struct WorkflowGrpcService {
    database_url: Arc<String>,
}

impl WorkflowGrpcService {
    fn new(database_url: Arc<String>) -> Self {
        Self { database_url }
    }
}

async fn run_grpc_server(listener: TcpListener, database_url: Arc<String>) -> Result<()> {
    let incoming = TcpListenerStream::new(listener);
    let service = WorkflowGrpcService::new(database_url);
    Server::builder()
        .add_service(WorkflowServiceServer::new(service))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

#[async_trait]
impl proto::workflow_service_server::WorkflowService for WorkflowGrpcService {
    async fn register_workflow(
        &self,
        request: Request<proto::RegisterWorkflowRequest>,
    ) -> Result<GrpcResponse<proto::RegisterWorkflowResponse>, Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| Status::invalid_argument("registration missing"))?;
        let payload = registration.encode_to_vec();
        let (version_id, instance_id) =
            instances::run_instance_payload(self.database_url.as_ref(), &payload)
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
        Ok(GrpcResponse::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_id: instance_id.to_string(),
        }))
    }

    async fn wait_for_instance(
        &self,
        request: Request<proto::WaitForInstanceRequest>,
    ) -> Result<GrpcResponse<proto::WaitForInstanceResponse>, Status> {
        let inner = request.into_inner();
        let instance_id = inner
            .instance_id
            .parse()
            .map_err(|err| Status::invalid_argument(format!("invalid instance_id: {err}")))?;
        let interval = sanitize_interval(Some(inner.poll_interval_secs));
        let payload =
            instances::wait_for_instance_poll(self.database_url.as_ref(), instance_id, interval)
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
        let bytes = payload.ok_or_else(|| Status::not_found("no workflow instance available"))?;
        Ok(GrpcResponse::new(proto::WaitForInstanceResponse {
            payload: bytes,
        }))
    }
}

pub(crate) fn sanitize_interval(value: Option<f64>) -> Duration {
    let raw = value.unwrap_or(1.0);
    let clamped = raw.clamp(0.1, 30.0);
    Duration::from_secs_f64(clamped)
}
