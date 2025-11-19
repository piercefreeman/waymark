use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

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
    WorkflowInstanceId, WorkflowVersionId,
    db::{
        Database, WorkflowInstanceActionDetail, WorkflowInstanceDetail, WorkflowInstanceSummary,
        WorkflowVersionDetail, WorkflowVersionSummary,
    },
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
        .route(
            "/workflow/:workflow_version_id/run/:instance_id",
            get(workflow_run_detail),
        )
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
        Ok(Some(workflow)) => {
            let runs = match database.recent_instances_for_version(version_id, 10).await {
                Ok(list) => list,
                Err(err) => {
                    error!(?err, workflow_version_id = %version_id, "failed to load recent runs");
                    Vec::new()
                }
            };
            Html(render_workflow_detail_page(&templates, &workflow, &runs))
        }
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

async fn workflow_run_detail(
    State(state): State<HttpState>,
    Path((version_id, instance_id)): Path<(WorkflowVersionId, WorkflowInstanceId)>,
) -> Html<String> {
    let templates = state.templates.clone();
    let database = state.database.clone();
    let instance_detail = match database.load_instance_with_actions(instance_id).await {
        Ok(Some(detail)) => detail,
        Ok(None) => {
            return Html(render_error_page(
                &templates,
                "Run not found",
                "The requested workflow run could not be located.",
            ));
        }
        Err(err) => {
            error!(?err, workflow_instance_id = %instance_id, "failed to load workflow run");
            return Html(render_error_page(
                &templates,
                "Unable to load run",
                "We encountered a database error while loading this workflow run.",
            ));
        }
    };
    let Some(instance_version_id) = instance_detail.instance.workflow_version_id else {
        return Html(render_error_page(
            &templates,
            "Run not linked",
            "This workflow run is missing its version metadata.",
        ));
    };
    if instance_version_id != version_id {
        return Html(render_error_page(
            &templates,
            "Workflow mismatch",
            "This workflow run does not belong to the requested workflow version.",
        ));
    }
    let workflow = match database.load_workflow_version(version_id).await {
        Ok(Some(workflow)) => workflow,
        Ok(None) => {
            return Html(render_error_page(
                &templates,
                "Workflow not found",
                "The requested workflow version could not be located.",
            ));
        }
        Err(err) => {
            error!(
                ?err,
                workflow_version_id = %version_id,
                "failed to load workflow detail"
            );
            return Html(render_error_page(
                &templates,
                "Unable to load workflow",
                "We hit an unexpected error when loading this workflow. Please try again shortly.",
            ));
        }
    };
    Html(render_workflow_run_page(
        &templates,
        &workflow,
        &instance_detail,
    ))
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

fn render_workflow_detail_page(
    templates: &Tera,
    workflow: &WorkflowVersionDetail,
    runs: &[WorkflowInstanceSummary],
) -> String {
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
    let info = build_workflow_metadata(workflow);
    let run_items = runs
        .iter()
        .map(|run| WorkflowInstanceListContext {
            id: run.id.to_string(),
            created_at: run.created_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            status: describe_run_status(run.has_result, run.completed_actions, run.total_actions),
            progress: format_run_progress(run.completed_actions, run.total_actions),
            url: format!("/workflow/{}/run/{}", workflow.id, run.id),
        })
        .collect();
    let context = WorkflowDetailPageContext {
        title: format!("{} • Workflow Detail", workflow.workflow_name),
        workflow: info,
        nodes,
        has_nodes: !workflow.dag.nodes.is_empty(),
        graph_data: build_graph_data(workflow),
        recent_runs: run_items,
        has_runs: !runs.is_empty(),
    };
    render_template(templates, "workflow.html", &context)
}

fn describe_run_status(has_result: bool, completed: i64, total: i64) -> String {
    if has_result {
        "Completed".to_string()
    } else if completed > 0 {
        if total > 0 && completed >= total {
            "Awaiting Finalization".to_string()
        } else {
            "In Progress".to_string()
        }
    } else {
        "Pending".to_string()
    }
}

fn format_run_progress(completed: i64, total: i64) -> String {
    if total > 0 {
        format!("{}/{}", completed, total)
    } else {
        "-".to_string()
    }
}

fn render_workflow_run_page(
    templates: &Tera,
    workflow: &WorkflowVersionDetail,
    detail: &WorkflowInstanceDetail,
) -> String {
    let workflow_meta = build_workflow_metadata(workflow);
    let total_nodes = workflow.dag.nodes.len() as i64;
    let completed_nodes = detail
        .actions
        .iter()
        .filter(|action| action.status.eq_ignore_ascii_case("completed"))
        .count() as i64;
    let status_label = describe_run_status(
        detail.instance.result_payload.is_some(),
        completed_nodes,
        total_nodes,
    );
    let progress = format_run_progress(completed_nodes, total_nodes);
    let input_payload = format_optional_payload(&detail.instance.input_payload);
    let result_payload = format_optional_payload(&detail.instance.result_payload);
    let instance_meta = WorkflowRunMetadataContext {
        id: detail.instance.id.to_string(),
        created_at: detail
            .instance
            .created_at
            .format("%Y-%m-%d %H:%M:%S UTC")
            .to_string(),
        status: status_label,
        progress,
        input_payload,
        result_payload,
    };
    let mut action_map: HashMap<String, &WorkflowInstanceActionDetail> = HashMap::new();
    for action in &detail.actions {
        if let Some(node_id) = &action.workflow_node_id {
            action_map.insert(node_id.clone(), action);
        }
    }
    let nodes = workflow
        .dag
        .nodes
        .iter()
        .map(|node| {
            if let Some(record) = action_map.get(&node.id) {
                WorkflowRunNodeContext {
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
                    status: describe_action_status(record),
                    request_payload: format_payload(&record.payload),
                    response_payload: format_optional_payload(&record.result_payload),
                }
            } else {
                WorkflowRunNodeContext {
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
                    status: "Pending".to_string(),
                    request_payload: "-".to_string(),
                    response_payload: "-".to_string(),
                }
            }
        })
        .collect();
    let context = WorkflowRunPageContext {
        title: format!("Run {} • {}", detail.instance.id, workflow.workflow_name),
        workflow: workflow_meta,
        instance: instance_meta,
        nodes,
    };
    render_template(templates, "workflow_run.html", &context)
}

fn describe_action_status(action: &WorkflowInstanceActionDetail) -> String {
    let status = action.status.to_lowercase();
    if status == "completed" {
        if action.success.unwrap_or(true) {
            "Completed".to_string()
        } else {
            "Failed".to_string()
        }
    } else if status == "dispatched" {
        "Dispatched".to_string()
    } else if status == "queued" {
        "Queued".to_string()
    } else {
        action.status.clone()
    }
}

fn format_payload(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "-".to_string();
    }
    if let Ok(text) = std::str::from_utf8(bytes) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(text)
            && let Ok(pretty) = serde_json::to_string_pretty(&value)
        {
            return pretty;
        }
        let trimmed = text.trim();
        return if trimmed.is_empty() {
            "-".to_string()
        } else {
            trimmed.to_string()
        };
    }
    general_purpose::STANDARD.encode(bytes)
}

fn format_optional_payload(value: &Option<Vec<u8>>) -> String {
    match value {
        Some(bytes) => format_payload(bytes),
        None => "-".to_string(),
    }
}

fn build_workflow_metadata(workflow: &WorkflowVersionDetail) -> WorkflowDetailMetadata {
    WorkflowDetailMetadata {
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
    }
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
    recent_runs: Vec<WorkflowInstanceListContext>,
    has_runs: bool,
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
struct WorkflowInstanceListContext {
    id: String,
    created_at: String,
    status: String,
    progress: String,
    url: String,
}

#[derive(Serialize)]
struct WorkflowRunPageContext {
    title: String,
    workflow: WorkflowDetailMetadata,
    instance: WorkflowRunMetadataContext,
    nodes: Vec<WorkflowRunNodeContext>,
}

#[derive(Serialize)]
struct WorkflowRunMetadataContext {
    id: String,
    created_at: String,
    status: String,
    progress: String,
    input_payload: String,
    result_payload: String,
}

#[derive(Serialize)]
struct WorkflowRunNodeContext {
    id: String,
    action: String,
    module: String,
    status: String,
    request_payload: String,
    response_payload: String,
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
