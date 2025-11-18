use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Serialize};
use tera::{Context as TeraContext, Tera};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response as GrpcResponse, Status, async_trait, transport::Server};
use tracing::{error, info};

use crate::{
    WorkflowVersionId,
    db::{Database, WorkflowVersionDetail, WorkflowVersionSummary},
    instances,
    messages::proto::{self, workflow_service_server::WorkflowServiceServer},
};
use prost::Message;

pub const SERVICE_NAME: &str = "carabiner";
pub const HEALTH_PATH: &str = "/healthz";

const REGISTER_PATH: &str = "/v1/workflows/register";
const WAIT_PATH: &str = "/v1/workflows/wait";

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

    let http_state = HttpState {
        http_addr,
        grpc_addr,
        database_url: Arc::clone(&database_url),
        database,
        templates,
    };

    let http_task = tokio::spawn(run_http_server(http_listener, http_state));
    let grpc_task = tokio::spawn(run_grpc_server(grpc_listener, database_url));

    tokio::select! {
        result = http_task => result??,
        result = grpc_task => result??,
    }

    Ok(())
}

#[derive(Clone)]
struct HttpState {
    http_addr: SocketAddr,
    grpc_addr: SocketAddr,
    database_url: Arc<String>,
    database: Database,
    templates: Arc<Tera>,
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
}

#[derive(Debug, Deserialize)]
struct WaitForInstanceHttpRequest {
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
struct WorkflowGrpcService {
    database_url: Arc<String>,
}

impl WorkflowGrpcService {
    fn new(database_url: Arc<String>) -> Self {
        Self { database_url }
    }
}

async fn run_http_server(listener: TcpListener, state: HttpState) -> Result<()> {
    let app = Router::new()
        .route("/", get(list_workflows))
        .route("/workflow/:workflow_version_id", get(workflow_detail))
        .route(HEALTH_PATH, get(healthz))
        .route(REGISTER_PATH, post(register_workflow_http))
        .route(WAIT_PATH, post(wait_for_instance_http))
        .with_state(state);
    axum::serve(listener, app).await?;
    Ok(())
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

async fn healthz(State(state): State<HttpState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: SERVICE_NAME,
        http_port: state.http_addr.port(),
        grpc_port: state.grpc_addr.port(),
    })
}

async fn list_workflows(State(state): State<HttpState>) -> Html<String> {
    let templates = state.templates.clone();
    let database = state.database.clone();
    match database.list_workflow_versions().await {
        Ok(workflows) => Html(render_home_page(&templates, &workflows)),
        Err(err) => {
            error!(?err, "failed to load workflow summaries");
            Html(render_error_page(
                &templates,
                "Unable to load workflows",
                "We couldn't fetch workflow versions. Please try again after checking the database connection.",
            ))
        }
    }
}

async fn workflow_detail(
    State(state): State<HttpState>,
    Path(version_id): Path<WorkflowVersionId>,
) -> Html<String> {
    let templates = state.templates.clone();
    let database = state.database.clone();
    match database.load_workflow_version(version_id).await {
        Ok(Some(workflow)) => Html(render_workflow_detail_page(&templates, &workflow)),
        Ok(None) => Html(render_error_page(
            &templates,
            "Workflow not found",
            "The requested workflow version could not be located.",
        )),
        Err(err) => {
            error!(
                ?err,
                workflow_version_id = %version_id,
                "failed to load workflow detail"
            );
            Html(render_error_page(
                &templates,
                "Unable to load workflow",
                "We hit an unexpected error when loading this workflow. Please try again shortly.",
            ))
        }
    }
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
    let version_id = instances::run_instance_payload(database_url.as_ref(), &bytes)
        .await
        .map_err(HttpError::internal)?;
    Ok(Json(RegisterWorkflowHttpResponse {
        workflow_version_id: version_id.to_string(),
    }))
}

async fn wait_for_instance_http(
    State(state): State<HttpState>,
    Json(request): Json<WaitForInstanceHttpRequest>,
) -> Result<Json<WaitForInstanceHttpResponse>, HttpError> {
    let interval = sanitize_interval(request.poll_interval_secs);
    let database_url = state.database_url.clone();
    let payload = instances::wait_for_instance_poll(database_url.as_ref(), interval)
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
        let version_id = instances::run_instance_payload(self.database_url.as_ref(), &payload)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(GrpcResponse::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
        }))
    }

    async fn wait_for_instance(
        &self,
        request: Request<proto::WaitForInstanceRequest>,
    ) -> Result<GrpcResponse<proto::WaitForInstanceResponse>, Status> {
        let inner = request.into_inner();
        let interval = sanitize_interval(Some(inner.poll_interval_secs));
        let payload = instances::wait_for_instance_poll(self.database_url.as_ref(), interval)
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

fn render_home_page(templates: &Tera, workflows: &[WorkflowVersionSummary]) -> String {
    let items = workflows
        .iter()
        .map(|workflow| HomeWorkflowContext {
            id: workflow.id.to_string(),
            name: workflow.workflow_name.clone(),
            hash: workflow.dag_hash.clone(),
            created_at: workflow
                .created_at
                .format("%Y-%m-%d %H:%M:%S UTC")
                .to_string(),
        })
        .collect();
    let context = HomePageContext {
        title: "Carabiner • Workflows".to_string(),
        workflows: items,
    };
    render_template(templates, "home.html", &context)
}

fn render_workflow_detail_page(templates: &Tera, workflow: &WorkflowVersionDetail) -> String {
    let nodes = workflow
        .dag
        .nodes
        .iter()
        .map(|node| WorkflowNodeTemplateContext {
            id: node.id.clone(),
            module: if node.module.is_empty() {
                "default".to_string()
            } else {
                node.module.clone()
            },
            action: if node.action.is_empty() {
                "action".to_string()
            } else {
                node.action.clone()
            },
            guard: if node.guard.is_empty() {
                "None".to_string()
            } else {
                node.guard.clone()
            },
            depends_on_display: format_dependencies(&node.depends_on),
            waits_for_display: format_dependencies(&node.wait_for_sync),
        })
        .collect();
    let info = WorkflowDetailMetadata {
        id: workflow.id.to_string(),
        name: workflow.workflow_name.clone(),
        hash: workflow.dag_hash.clone(),
        created_at: workflow
            .created_at
            .format("%Y-%m-%d %H:%M:%S UTC")
            .to_string(),
        concurrency_label: if workflow.concurrent {
            "Concurrent".to_string()
        } else {
            "Serial".to_string()
        },
    };
    let context = WorkflowDetailPageContext {
        title: format!("{} • Workflow Detail", workflow.workflow_name),
        workflow: info,
        nodes,
        has_nodes: !workflow.dag.nodes.is_empty(),
        graph_data: build_graph_data(workflow),
    };
    render_template(templates, "workflow.html", &context)
}

fn render_error_page(templates: &Tera, title: &str, message: &str) -> String {
    let context = ErrorPageContext {
        title: title.to_string(),
        message: message.to_string(),
    };
    render_template(templates, "error.html", &context)
}

fn render_template<T: Serialize>(templates: &Tera, template: &str, data: &T) -> String {
    let context = match TeraContext::from_serialize(data) {
        Ok(ctx) => ctx,
        Err(err) => {
            error!(?err, "failed to serialize template context");
            TeraContext::new()
        }
    };
    match templates.render(template, &context) {
        Ok(html) => html,
        Err(err) => {
            error!(?err, template = template, "failed to render template");
            "<!DOCTYPE html><html lang=\"en\"><body><h1>Template error</h1></body></html>"
                .to_string()
        }
    }
}

fn format_dependencies(items: &[String]) -> String {
    if items.is_empty() {
        "None".to_string()
    } else {
        items.join(", ")
    }
}

#[derive(Serialize)]
struct WorkflowGraphData {
    nodes: Vec<WorkflowGraphNode>,
}

#[derive(Serialize)]
struct WorkflowGraphNode {
    id: String,
    action: String,
    module: String,
    depends_on: Vec<String>,
}

#[derive(Serialize)]
struct HomePageContext {
    title: String,
    workflows: Vec<HomeWorkflowContext>,
}

#[derive(Serialize)]
struct HomeWorkflowContext {
    id: String,
    name: String,
    hash: String,
    created_at: String,
}

#[derive(Serialize)]
struct WorkflowDetailPageContext {
    title: String,
    workflow: WorkflowDetailMetadata,
    nodes: Vec<WorkflowNodeTemplateContext>,
    has_nodes: bool,
    graph_data: WorkflowGraphData,
}

#[derive(Serialize)]
struct WorkflowDetailMetadata {
    id: String,
    name: String,
    hash: String,
    created_at: String,
    concurrency_label: String,
}

#[derive(Serialize)]
struct WorkflowNodeTemplateContext {
    id: String,
    module: String,
    action: String,
    guard: String,
    depends_on_display: String,
    waits_for_display: String,
}

#[derive(Serialize)]
struct ErrorPageContext {
    title: String,
    message: String,
}

fn build_graph_data(workflow: &WorkflowVersionDetail) -> WorkflowGraphData {
    let nodes = workflow
        .dag
        .nodes
        .iter()
        .map(|node| WorkflowGraphNode {
            id: node.id.clone(),
            action: if node.action.is_empty() {
                "action".to_string()
            } else {
                node.action.clone()
            },
            module: if node.module.is_empty() {
                "workflow".to_string()
            } else {
                node.module.clone()
            },
            depends_on: node.depends_on.clone(),
        })
        .collect();
    WorkflowGraphData { nodes }
}
