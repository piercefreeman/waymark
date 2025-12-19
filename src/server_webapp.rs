//! Web application server for the Rappel workflow dashboard.
//!
//! This module provides a human-readable web UI for inspecting workflows,
//! viewing workflow versions, and monitoring workflow instances.
//!
//! The webapp is disabled by default and can be enabled via environment variables:
//! - `RAPPEL_WEBAPP_ENABLED`: Set to "true" or "1" to enable
//! - `RAPPEL_WEBAPP_ADDR`: Address to bind to (default: 0.0.0.0:24119)

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde::Serialize;
use tera::{Context as TeraContext, Tera};
use tokio::net::TcpListener;
use tracing::{error, info};
use uuid::Uuid;

use crate::config::WebappConfig;
use crate::db::{Database, ScheduleId, WorkflowVersionId, WorkflowVersionSummary};

/// Webapp server handle
pub struct WebappServer {
    addr: SocketAddr,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl WebappServer {
    /// Start the webapp server
    ///
    /// Returns None if the webapp is disabled via configuration.
    pub async fn start(config: WebappConfig, database: Arc<Database>) -> Result<Option<Self>> {
        if !config.enabled {
            info!("webapp disabled (set RAPPEL_WEBAPP_ENABLED=true to enable)");
            return Ok(None);
        }

        let bind_addr = config.bind_addr();
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("failed to bind webapp listener on {bind_addr}"))?;

        let actual_addr = listener.local_addr()?;

        // Initialize templates
        let templates = init_templates()?;

        let state = WebappState {
            database,
            templates: Arc::new(templates),
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Spawn the server task
        tokio::spawn(run_server(listener, state, shutdown_rx));

        info!(addr = %actual_addr, "webapp server started");

        Ok(Some(Self {
            addr: actual_addr,
            shutdown_tx,
        }))
    }

    /// Get the address the server is bound to
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Shutdown the server
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

// Embed templates at compile time so they're included in the binary
const TEMPLATE_BASE: &str = include_str!("../templates/base.html");
const TEMPLATE_MACROS: &str = include_str!("../templates/macros.html");
const TEMPLATE_HOME: &str = include_str!("../templates/home.html");
const TEMPLATE_ERROR: &str = include_str!("../templates/error.html");
const TEMPLATE_WORKFLOW: &str = include_str!("../templates/workflow.html");
const TEMPLATE_WORKFLOW_RUN: &str = include_str!("../templates/workflow_run.html");
const TEMPLATE_SCHEDULED: &str = include_str!("../templates/scheduled.html");
const TEMPLATE_SCHEDULE_DETAIL: &str = include_str!("../templates/schedule_detail.html");

/// Initialize Tera templates from embedded strings
fn init_templates() -> Result<Tera> {
    let mut tera = Tera::default();

    // Add templates in order - base and macros first since others use them
    tera.add_raw_template("base.html", TEMPLATE_BASE)
        .context("failed to add base.html template")?;
    tera.add_raw_template("macros.html", TEMPLATE_MACROS)
        .context("failed to add macros.html template")?;
    tera.add_raw_template("home.html", TEMPLATE_HOME)
        .context("failed to add home.html template")?;
    tera.add_raw_template("error.html", TEMPLATE_ERROR)
        .context("failed to add error.html template")?;
    tera.add_raw_template("workflow.html", TEMPLATE_WORKFLOW)
        .context("failed to add workflow.html template")?;
    tera.add_raw_template("workflow_run.html", TEMPLATE_WORKFLOW_RUN)
        .context("failed to add workflow_run.html template")?;
    tera.add_raw_template("scheduled.html", TEMPLATE_SCHEDULED)
        .context("failed to add scheduled.html template")?;
    tera.add_raw_template("schedule_detail.html", TEMPLATE_SCHEDULE_DETAIL)
        .context("failed to add schedule_detail.html template")?;

    tera.autoescape_on(vec![".html", ".tera"]);
    Ok(tera)
}

// ============================================================================
// Internal Server State
// ============================================================================

#[derive(Clone)]
struct WebappState {
    database: Arc<Database>,
    templates: Arc<Tera>,
}

async fn run_server(
    listener: TcpListener,
    state: WebappState,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    use axum::routing::post;

    let app = Router::new()
        .route("/", get(list_workflows))
        .route("/workflow/:workflow_version_id", get(workflow_detail))
        .route(
            "/workflow/:workflow_version_id/run/:instance_id",
            get(workflow_run_detail),
        )
        .route("/scheduled", get(list_schedules))
        .route("/scheduled/:schedule_id", get(schedule_detail))
        .route("/scheduled/:schedule_id/pause", post(pause_schedule))
        .route("/scheduled/:schedule_id/resume", post(resume_schedule))
        .route("/scheduled/:schedule_id/delete", post(delete_schedule))
        .route("/healthz", get(healthz))
        .with_state(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await
        .ok();
}

// ============================================================================
// Handlers
// ============================================================================

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "rappel-webapp",
    })
}

async fn list_workflows(State(state): State<WebappState>) -> impl IntoResponse {
    match state.database.list_workflow_versions().await {
        Ok(workflows) => Html(render_home_page(&state.templates, &workflows)),
        Err(err) => {
            error!(?err, "failed to load workflow summaries");
            Html(render_error_page(
                &state.templates,
                "Unable to load workflows",
                "We couldn't fetch workflow versions. Please check the database connection.",
            ))
        }
    }
}

async fn workflow_detail(
    State(state): State<WebappState>,
    Path(version_id): Path<Uuid>,
) -> impl IntoResponse {
    // Load workflow version
    let version = match state
        .database
        .get_workflow_version(WorkflowVersionId(version_id))
        .await
    {
        Ok(v) => v,
        Err(err) => {
            error!(?err, %version_id, "failed to load workflow version");
            return Html(render_error_page(
                &state.templates,
                "Workflow not found",
                "The requested workflow version could not be located.",
            ));
        }
    };

    // Load recent instances for this version
    let instances = state
        .database
        .list_instances_for_version(WorkflowVersionId(version_id), 20)
        .await
        .unwrap_or_default();

    Html(render_workflow_detail_page(
        &state.templates,
        &version,
        &instances,
    ))
}

async fn workflow_run_detail(
    State(state): State<WebappState>,
    Path((version_id, instance_id)): Path<(Uuid, Uuid)>,
) -> impl IntoResponse {
    // Load workflow version
    let version = match state
        .database
        .get_workflow_version(WorkflowVersionId(version_id))
        .await
    {
        Ok(v) => v,
        Err(err) => {
            error!(?err, %version_id, "failed to load workflow version");
            return Html(render_error_page(
                &state.templates,
                "Workflow not found",
                "The requested workflow version could not be located.",
            ));
        }
    };

    // Load instance
    let instance = match state
        .database
        .get_instance(crate::db::WorkflowInstanceId(instance_id))
        .await
    {
        Ok(i) => i,
        Err(err) => {
            error!(?err, %instance_id, "failed to load instance");
            return Html(render_error_page(
                &state.templates,
                "Instance not found",
                "The requested workflow instance could not be located.",
            ));
        }
    };

    // Load actions for this instance
    let actions = state
        .database
        .get_instance_actions(crate::db::WorkflowInstanceId(instance_id))
        .await
        .unwrap_or_default();

    Html(render_workflow_run_page(
        &state.templates,
        &version,
        &instance,
        &actions,
    ))
}

async fn list_schedules(State(state): State<WebappState>) -> impl IntoResponse {
    match state.database.list_schedules(None).await {
        Ok(schedules) => Html(render_scheduled_page(&state.templates, &schedules)),
        Err(err) => {
            error!(?err, "failed to load schedules");
            Html(render_error_page(
                &state.templates,
                "Unable to load schedules",
                "We couldn't fetch scheduled workflows. Please check the database connection.",
            ))
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ScheduleDetailQuery {
    page: Option<i64>,
}

async fn schedule_detail(
    State(state): State<WebappState>,
    Path(schedule_id): Path<Uuid>,
    axum::extract::Query(query): axum::extract::Query<ScheduleDetailQuery>,
) -> impl IntoResponse {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = 20i64;
    let offset = (page - 1) * per_page;

    // Load schedule
    let schedule = match state
        .database
        .get_schedule_by_id(ScheduleId(schedule_id))
        .await
    {
        Ok(s) => s,
        Err(err) => {
            error!(?err, %schedule_id, "failed to load schedule");
            return Html(render_error_page(
                &state.templates,
                "Schedule not found",
                "The requested schedule could not be located.",
            ));
        }
    };

    // Load invocations with pagination
    let invocations = state
        .database
        .list_schedule_invocations(&schedule.workflow_name, per_page, offset)
        .await
        .unwrap_or_default();

    // Get total count for pagination
    let total_count = state
        .database
        .count_schedule_invocations(&schedule.workflow_name)
        .await
        .unwrap_or(0);

    let total_pages = (total_count as f64 / per_page as f64).ceil() as i64;

    Html(render_schedule_detail_page(
        &state.templates,
        &schedule,
        &invocations,
        page,
        total_pages,
    ))
}

async fn pause_schedule(
    State(state): State<WebappState>,
    Path(schedule_id): Path<Uuid>,
) -> impl IntoResponse {
    match state
        .database
        .update_schedule_status_by_id(ScheduleId(schedule_id), "paused")
        .await
    {
        Ok(true) => axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id)),
        Ok(false) => axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id)),
        Err(err) => {
            error!(?err, %schedule_id, "failed to pause schedule");
            axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id))
        }
    }
}

async fn resume_schedule(
    State(state): State<WebappState>,
    Path(schedule_id): Path<Uuid>,
) -> impl IntoResponse {
    match state
        .database
        .update_schedule_status_by_id(ScheduleId(schedule_id), "active")
        .await
    {
        Ok(true) => axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id)),
        Ok(false) => axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id)),
        Err(err) => {
            error!(?err, %schedule_id, "failed to resume schedule");
            axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id))
        }
    }
}

async fn delete_schedule(
    State(state): State<WebappState>,
    Path(schedule_id): Path<Uuid>,
) -> impl IntoResponse {
    match state
        .database
        .delete_schedule_by_id(ScheduleId(schedule_id))
        .await
    {
        Ok(true) => axum::response::Redirect::to("/scheduled"),
        Ok(false) => axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id)),
        Err(err) => {
            error!(?err, %schedule_id, "failed to delete schedule");
            axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id))
        }
    }
}

// ============================================================================
// Response Types
// ============================================================================

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    #[allow(dead_code)]
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
        let body = Json(serde_json::json!({ "message": self.message }));
        (self.status, body).into_response()
    }
}

// ============================================================================
// Template Rendering
// ============================================================================

#[derive(Serialize)]
struct HomePageContext {
    title: String,
    active_tab: String,
    workflow_groups: Vec<WorkflowGroup>,
}

#[derive(Serialize)]
struct WorkflowGroup {
    name: String,
    versions: Vec<WorkflowVersionBrief>,
}

#[derive(Serialize)]
struct WorkflowVersionBrief {
    id: String,
    created_at: String,
}

fn render_home_page(templates: &Tera, workflows: &[WorkflowVersionSummary]) -> String {
    // Group workflows by name
    let mut groups: std::collections::HashMap<String, Vec<WorkflowVersionBrief>> =
        std::collections::HashMap::new();

    for w in workflows {
        groups
            .entry(w.workflow_name.clone())
            .or_default()
            .push(WorkflowVersionBrief {
                id: w.id.to_string(),
                created_at: w.created_at.to_rfc3339(),
            });
    }

    // Convert to sorted vec (sort by workflow name)
    let mut workflow_groups: Vec<WorkflowGroup> = groups
        .into_iter()
        .map(|(name, versions)| WorkflowGroup { name, versions })
        .collect();
    workflow_groups.sort_by(|a, b| a.name.cmp(&b.name));

    let context = HomePageContext {
        title: "Registered Workflow Versions".to_string(),
        active_tab: "workflows".to_string(),
        workflow_groups,
    };

    render_template(templates, "home.html", &context)
}

#[derive(Serialize)]
struct WorkflowDetailPageContext {
    title: String,
    active_tab: String,
    workflow: WorkflowDetailMetadata,
    nodes: Vec<WorkflowNodeContext>,
    has_nodes: bool,
    recent_runs: Vec<WorkflowRunSummary>,
    has_runs: bool,
    graph_data: WorkflowGraphData,
}

#[derive(Serialize)]
struct WorkflowDetailMetadata {
    id: String,
    name: String,
    hash: String,
    /// ISO 8601 timestamp (client renders as relative/local/UTC)
    created_at: String,
    concurrency_label: String,
}

#[derive(Serialize)]
struct WorkflowNodeContext {
    id: String,
    module: String,
    action: String,
    guard: String,
    depends_on_display: String,
    waits_for_display: String,
}

#[derive(Serialize)]
struct WorkflowRunSummary {
    id: String,
    /// ISO 8601 timestamp (client renders as relative/local/UTC)
    created_at: String,
    status: String,
    progress: String,
    url: String,
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

fn render_workflow_detail_page(
    templates: &Tera,
    version: &crate::db::WorkflowVersion,
    instances: &[crate::db::WorkflowInstance],
) -> String {
    // Decode the DAG from the program proto
    let dag = decode_dag_from_proto(&version.program_proto);

    let nodes: Vec<WorkflowNodeContext> = dag
        .iter()
        .map(|node| WorkflowNodeContext {
            id: node.id.clone(),
            module: if node.module.is_empty() {
                "workflow".to_string()
            } else {
                node.module.clone()
            },
            action: if node.action.is_empty() {
                "action".to_string()
            } else {
                node.action.clone()
            },
            guard: node.guard.clone().unwrap_or_else(|| "None".to_string()),
            depends_on_display: format_dependencies(&node.depends_on),
            waits_for_display: format_dependencies(&node.waits_for),
        })
        .collect();

    let graph_data = WorkflowGraphData {
        nodes: dag
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
            .collect(),
    };

    // Create a map of node sequence to action name from the DAG
    // In the DAG, nodes are ordered by their topological order which corresponds to execution sequence
    let action_names: Vec<String> = dag
        .iter()
        .map(|node| {
            if node.action.is_empty() {
                "action".to_string()
            } else {
                node.action.clone()
            }
        })
        .collect();

    let recent_runs: Vec<WorkflowRunSummary> = instances
        .iter()
        .map(|i| {
            // Determine progress based on status and sequence
            let progress = if i.status == "completed" {
                "Done".to_string()
            } else if i.status == "failed" {
                "Failed".to_string()
            } else if i.status == "pending" || i.next_action_seq == 0 {
                "Queued".to_string()
            } else {
                // Show the current action being executed (seq is 0-based for the node index)
                // next_action_seq is the NEXT action to dispatch, so current is seq - 1
                let current_idx = (i.next_action_seq as usize).saturating_sub(1);
                action_names
                    .get(current_idx)
                    .cloned()
                    .unwrap_or_else(|| format!("Step {}", i.next_action_seq))
            };

            WorkflowRunSummary {
                id: i.id.to_string(),
                created_at: i.created_at.to_rfc3339(),
                status: i.status.clone(),
                progress,
                url: format!("/workflow/{}/run/{}", version.id, i.id),
            }
        })
        .collect();

    let workflow = WorkflowDetailMetadata {
        id: version.id.to_string(),
        name: version.workflow_name.clone(),
        hash: version.dag_hash.clone(),
        created_at: version.created_at.to_rfc3339(),
        concurrency_label: if version.concurrent {
            "Concurrent".to_string()
        } else {
            "Serial".to_string()
        },
    };

    let context = WorkflowDetailPageContext {
        title: format!("{} - Workflow Detail", version.workflow_name),
        active_tab: "workflows".to_string(),
        workflow,
        has_nodes: !nodes.is_empty(),
        nodes,
        has_runs: !recent_runs.is_empty(),
        recent_runs,
        graph_data,
    };

    render_template(templates, "workflow.html", &context)
}

#[derive(Serialize)]
struct WorkflowRunPageContext {
    title: String,
    active_tab: String,
    workflow: WorkflowDetailMetadata,
    instance: InstanceContext,
    nodes: Vec<NodeExecutionContext>,
    /// Graph data for DAG visualization
    graph_data: ExecutionGraphData,
    /// JSON-encoded node data for client-side use (avoids HTML entity escaping)
    nodes_json: String,
}

/// Graph data for execution visualization (includes status)
#[derive(Serialize)]
struct ExecutionGraphData {
    nodes: Vec<ExecutionGraphNode>,
}

/// A node in the execution graph with status information
#[derive(Serialize)]
struct ExecutionGraphNode {
    id: String,
    action: String,
    module: String,
    depends_on: Vec<String>,
    /// Status: pending, dispatched, completed, failed
    status: String,
}

#[derive(Serialize)]
struct InstanceContext {
    id: String,
    /// ISO 8601 timestamp (client renders as relative/local/UTC)
    created_at: String,
    status: String,
    progress: String,
    input_payload: String,
    result_payload: String,
}

#[derive(Serialize)]
struct NodeExecutionContext {
    id: String,
    module: String,
    action: String,
    status: String,
    request_payload: String,
    response_payload: String,
    /// Current attempt number (0-based)
    attempt_number: i32,
    /// Maximum retries allowed for failures
    max_retries: i32,
    /// Maximum retries allowed for timeouts
    timeout_retry_limit: i32,
    /// Type of retry: "failure" or "timeout"
    retry_kind: String,
    /// When the action is scheduled to run (ISO 8601 format, or None)
    scheduled_at: Option<String>,
    /// Error message if the action or workflow failed
    last_error: Option<String>,
}

fn render_workflow_run_page(
    templates: &Tera,
    version: &crate::db::WorkflowVersion,
    instance: &crate::db::WorkflowInstance,
    actions: &[crate::db::QueuedAction],
) -> String {
    let workflow = WorkflowDetailMetadata {
        id: version.id.to_string(),
        name: version.workflow_name.clone(),
        hash: version.dag_hash.clone(),
        created_at: version.created_at.to_rfc3339(),
        concurrency_label: if version.concurrent {
            "Concurrent".to_string()
        } else {
            "Serial".to_string()
        },
    };

    // Decode the DAG from the workflow version
    let dag = decode_dag_from_proto(&version.program_proto);

    // Build action names list for progress display
    let action_names: Vec<String> = dag
        .iter()
        .map(|node| {
            if node.action.is_empty() {
                "action".to_string()
            } else {
                node.action.clone()
            }
        })
        .collect();

    // Determine progress based on status and sequence
    let progress = if instance.status == "completed" {
        "Done".to_string()
    } else if instance.status == "failed" {
        "Failed".to_string()
    } else if instance.status == "pending" || instance.next_action_seq == 0 {
        "Queued".to_string()
    } else {
        // Show the current action being executed
        let current_idx = (instance.next_action_seq as usize).saturating_sub(1);
        action_names
            .get(current_idx)
            .cloned()
            .unwrap_or_else(|| format!("Step {}", instance.next_action_seq))
    };

    let instance_ctx = InstanceContext {
        id: instance.id.to_string(),
        created_at: instance.created_at.to_rfc3339(),
        status: instance.status.clone(),
        progress,
        input_payload: format_payload(&instance.input_payload),
        result_payload: format_payload(&instance.result_payload),
    };

    // Build a map of node_id -> status from the executed actions
    let action_status: std::collections::HashMap<String, String> = actions
        .iter()
        .filter_map(|a| a.node_id.clone().map(|id| (id, a.status.clone())))
        .collect();

    // Build execution graph data with status info
    let graph_data = ExecutionGraphData {
        nodes: dag
            .iter()
            .map(|node| ExecutionGraphNode {
                id: node.id.clone(),
                action: if node.action.is_empty() {
                    "action".to_string()
                } else {
                    node.action.clone()
                },
                module: if node.module.is_empty() {
                    "__internal__".to_string()
                } else {
                    node.module.clone()
                },
                depends_on: node.depends_on.clone(),
                status: action_status
                    .get(&node.id)
                    .cloned()
                    .unwrap_or_else(|| "pending".to_string()),
            })
            .collect(),
    };

    let nodes: Vec<NodeExecutionContext> = actions
        .iter()
        .map(|a| NodeExecutionContext {
            id: a.node_id.clone().unwrap_or_else(|| a.id.to_string()),
            module: a.module_name.clone(),
            action: a.action_name.clone(),
            status: a.status.clone(),
            request_payload: format_binary_payload(&a.dispatch_payload),
            response_payload: a
                .result_payload
                .as_ref()
                .map(|p| format_binary_payload(p))
                .unwrap_or_else(|| "(pending)".to_string()),
            attempt_number: a.attempt_number,
            max_retries: a.max_retries,
            timeout_retry_limit: a.timeout_retry_limit,
            retry_kind: a.retry_kind.clone(),
            scheduled_at: a
                .scheduled_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
            last_error: a.last_error.clone(),
        })
        .collect();

    // Serialize nodes to JSON for client-side use (avoids HTML entity escaping issues)
    let nodes_json = serde_json::to_string(&nodes).unwrap_or_else(|_| "[]".to_string());

    let context = WorkflowRunPageContext {
        title: format!("Run {} - {}", instance.id, version.workflow_name),
        active_tab: "workflows".to_string(),
        workflow,
        instance: instance_ctx,
        nodes,
        graph_data,
        nodes_json,
    };

    render_template(templates, "workflow_run.html", &context)
}

#[derive(Serialize)]
struct ErrorPageContext {
    title: String,
    active_tab: String,
    message: String,
}

fn render_error_page(templates: &Tera, title: &str, message: &str) -> String {
    let context = ErrorPageContext {
        title: title.to_string(),
        active_tab: "".to_string(),
        message: message.to_string(),
    };
    render_template(templates, "error.html", &context)
}

// ============================================================================
// Schedule Template Context
// ============================================================================

#[derive(Serialize)]
struct ScheduledPageContext {
    title: String,
    active_tab: String,
    schedule_groups: Vec<ScheduleGroup>,
}

#[derive(Serialize)]
struct ScheduleGroup {
    schedule_type: String,
    schedules: Vec<ScheduleBrief>,
}

#[derive(Serialize)]
struct ScheduleBrief {
    id: String,
    workflow_name: String,
    status: String,
    schedule_expression: String,
    next_run_at: Option<String>,
    last_run_at: Option<String>,
}

fn render_scheduled_page(templates: &Tera, schedules: &[crate::db::WorkflowSchedule]) -> String {
    // Group schedules by type
    let mut cron_schedules = Vec::new();
    let mut interval_schedules = Vec::new();

    for s in schedules {
        let schedule_expression = if s.schedule_type == "cron" {
            s.cron_expression.clone().unwrap_or_default()
        } else {
            format_interval(s.interval_seconds)
        };

        let brief = ScheduleBrief {
            id: s.id.to_string(),
            workflow_name: s.workflow_name.clone(),
            status: s.status.clone(),
            schedule_expression,
            next_run_at: s.next_run_at.map(|dt| dt.to_rfc3339()),
            last_run_at: s.last_run_at.map(|dt| dt.to_rfc3339()),
        };

        if s.schedule_type == "cron" {
            cron_schedules.push(brief);
        } else {
            interval_schedules.push(brief);
        }
    }

    let mut schedule_groups = Vec::new();
    if !cron_schedules.is_empty() {
        schedule_groups.push(ScheduleGroup {
            schedule_type: "cron".to_string(),
            schedules: cron_schedules,
        });
    }
    if !interval_schedules.is_empty() {
        schedule_groups.push(ScheduleGroup {
            schedule_type: "interval".to_string(),
            schedules: interval_schedules,
        });
    }

    let context = ScheduledPageContext {
        title: "Scheduled Workflows".to_string(),
        active_tab: "scheduled".to_string(),
        schedule_groups,
    };

    render_template(templates, "scheduled.html", &context)
}

#[derive(Serialize)]
struct ScheduleDetailPageContext {
    title: String,
    active_tab: String,
    schedule: ScheduleDetailMetadata,
    has_invocations: bool,
    invocations: Vec<InvocationSummary>,
    current_page: i64,
    total_pages: i64,
    has_pagination: bool,
}

#[derive(Serialize)]
struct ScheduleDetailMetadata {
    id: String,
    workflow_name: String,
    schedule_type: String,
    schedule_expression: String,
    status: String,
    input_payload: Option<String>,
    next_run_at: Option<String>,
    last_run_at: Option<String>,
    created_at: String,
}

#[derive(Serialize)]
struct InvocationSummary {
    id: String,
    workflow_version_id: Option<String>,
    created_at: String,
    status: String,
}

fn render_schedule_detail_page(
    templates: &Tera,
    schedule: &crate::db::WorkflowSchedule,
    instances: &[crate::db::WorkflowInstance],
    current_page: i64,
    total_pages: i64,
) -> String {
    let schedule_expression = if schedule.schedule_type == "cron" {
        schedule.cron_expression.clone().unwrap_or_default()
    } else {
        format_interval(schedule.interval_seconds)
    };

    let input_payload = schedule
        .input_payload
        .as_ref()
        .map(|p| format_binary_payload(p));

    let schedule_metadata = ScheduleDetailMetadata {
        id: schedule.id.to_string(),
        workflow_name: schedule.workflow_name.clone(),
        schedule_type: schedule.schedule_type.clone(),
        schedule_expression,
        status: schedule.status.clone(),
        input_payload,
        next_run_at: schedule.next_run_at.map(|dt| dt.to_rfc3339()),
        last_run_at: schedule.last_run_at.map(|dt| dt.to_rfc3339()),
        created_at: schedule.created_at.to_rfc3339(),
    };

    let invocations: Vec<InvocationSummary> = instances
        .iter()
        .map(|i| InvocationSummary {
            id: i.id.to_string(),
            workflow_version_id: i.workflow_version_id.map(|id| id.to_string()),
            created_at: i.created_at.to_rfc3339(),
            status: i.status.clone(),
        })
        .collect();

    let context = ScheduleDetailPageContext {
        title: format!("{} - Schedule", schedule.workflow_name),
        active_tab: "scheduled".to_string(),
        schedule: schedule_metadata,
        has_invocations: !invocations.is_empty(),
        invocations,
        current_page,
        total_pages,
        has_pagination: total_pages > 1,
    };

    render_template(templates, "schedule_detail.html", &context)
}

/// Format interval seconds as a human-readable string
fn format_interval(interval_seconds: Option<i64>) -> String {
    match interval_seconds {
        Some(secs) if secs >= 86400 && secs % 86400 == 0 => {
            let days = secs / 86400;
            if days == 1 {
                "every day".to_string()
            } else {
                format!("every {} days", days)
            }
        }
        Some(secs) if secs >= 3600 && secs % 3600 == 0 => {
            let hours = secs / 3600;
            if hours == 1 {
                "every hour".to_string()
            } else {
                format!("every {} hours", hours)
            }
        }
        Some(secs) if secs >= 60 && secs % 60 == 0 => {
            let mins = secs / 60;
            if mins == 1 {
                "every minute".to_string()
            } else {
                format!("every {} minutes", mins)
            }
        }
        Some(secs) => format!("every {} seconds", secs),
        None => "not set".to_string(),
    }
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

// ============================================================================
// Helpers
// ============================================================================

/// Simple DAG node extracted from proto for display
struct SimpleDagNode {
    id: String,
    module: String,
    action: String,
    guard: Option<String>,
    depends_on: Vec<String>,
    waits_for: Vec<String>,
}

fn decode_dag_from_proto(proto_bytes: &[u8]) -> Vec<SimpleDagNode> {
    use prost::Message;

    // Try to decode the program proto (uses parser::ast which is the proto type)
    let program = match crate::parser::ast::Program::decode(proto_bytes) {
        Ok(p) => p,
        Err(_) => return vec![],
    };

    // Convert to DAG using the existing converter
    let dag = crate::dag::convert_to_dag(&program);

    dag.nodes
        .values()
        .map(|node| {
            // Find depends_on edges (StateMachine type = control flow)
            let depends_on: Vec<String> = dag
                .edges
                .iter()
                .filter(|e| {
                    e.target == node.id && e.edge_type == crate::dag::EdgeType::StateMachine
                })
                .map(|e| e.source.clone())
                .collect();

            // Find waits_for edges (DataFlow type)
            let waits_for: Vec<String> = dag
                .edges
                .iter()
                .filter(|e| e.target == node.id && e.edge_type == crate::dag::EdgeType::DataFlow)
                .map(|e| e.source.clone())
                .collect();

            SimpleDagNode {
                id: node.id.clone(),
                module: node.module_name.clone().unwrap_or_default(),
                action: node.action_name.clone().unwrap_or_default(),
                guard: node.guard_expr.as_ref().map(crate::print_expr),
                depends_on,
                waits_for,
            }
        })
        .collect()
}

fn format_dependencies(items: &[String]) -> String {
    if items.is_empty() {
        "None".to_string()
    } else {
        items.join(", ")
    }
}

fn format_payload(payload: &Option<Vec<u8>>) -> String {
    match payload {
        Some(bytes) if !bytes.is_empty() => format_binary_payload(bytes),
        _ => "(empty)".to_string(),
    }
}

fn format_binary_payload(bytes: &[u8]) -> String {
    // Try to decode as protobuf WorkflowArguments first
    if let Some(json) = crate::messages::workflow_arguments_to_json(bytes)
        && let Ok(pretty) = serde_json::to_string_pretty(&json)
    {
        return pretty;
    }

    // Try to decode as UTF-8
    if let Ok(s) = std::str::from_utf8(bytes) {
        // Try to pretty-print as JSON
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(s)
            && let Ok(pretty) = serde_json::to_string_pretty(&json)
        {
            return pretty;
        }
        return s.to_string();
    }

    // Fall back to byte count for binary data
    format!("({} bytes)", bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_dependencies() {
        assert_eq!(format_dependencies(&[]), "None");
        assert_eq!(
            format_dependencies(&["a".to_string(), "b".to_string()]),
            "a, b"
        );
    }

    #[test]
    fn test_format_payload_empty() {
        assert_eq!(format_payload(&None), "(empty)");
        assert_eq!(format_payload(&Some(vec![])), "(empty)");
    }

    #[test]
    fn test_format_binary_payload_json() {
        let json = r#"{"key": "value"}"#;
        let result = format_binary_payload(json.as_bytes());
        assert!(result.contains("key"));
        assert!(result.contains("value"));
    }

    #[test]
    fn test_format_binary_payload_plain_text() {
        let text = "hello world";
        let result = format_binary_payload(text.as_bytes());
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_format_binary_payload_binary() {
        let bytes = vec![0xFF, 0xFE, 0x00, 0x01];
        let result = format_binary_payload(&bytes);
        assert_eq!(result, "(4 bytes)");
    }

    // ========================================================================
    // Template Rendering Tests
    // ========================================================================

    fn test_templates() -> Tera {
        init_templates().expect("failed to initialize templates")
    }

    #[test]
    fn test_render_home_page_empty() {
        let templates = test_templates();
        let html = render_home_page(&templates, &[]);

        assert!(html.contains("Registered Workflows"));
        assert!(html.contains("No workflows found"));
    }

    #[test]
    fn test_render_home_page_with_workflows() {
        let templates = test_templates();
        let workflows = vec![
            WorkflowVersionSummary {
                id: Uuid::new_v4(),
                workflow_name: "test_workflow".to_string(),
                dag_hash: "abc123def456".to_string(),
                concurrent: false,
                created_at: chrono::Utc::now(),
            },
            WorkflowVersionSummary {
                id: Uuid::new_v4(),
                workflow_name: "another_workflow".to_string(),
                dag_hash: "xyz789".to_string(),
                concurrent: true,
                created_at: chrono::Utc::now(),
            },
        ];

        let html = render_home_page(&templates, &workflows);

        // Workflows should be grouped by name
        assert!(html.contains("test_workflow"));
        assert!(html.contains("another_workflow"));
        // Should show version count
        assert!(html.contains("1 version"));
    }

    #[test]
    fn test_render_error_page() {
        let templates = test_templates();
        let html = render_error_page(&templates, "Test Error", "Something went wrong");

        assert!(html.contains("Test Error"));
        assert!(html.contains("Something went wrong"));
    }

    #[test]
    fn test_render_workflow_detail_page() {
        let templates = test_templates();
        let version = crate::db::WorkflowVersion {
            id: Uuid::new_v4(),
            workflow_name: "my_workflow".to_string(),
            dag_hash: "hash123456789".to_string(),
            program_proto: vec![], // Empty proto - will result in empty DAG
            concurrent: true,
            created_at: chrono::Utc::now(),
        };
        let instances: Vec<crate::db::WorkflowInstance> = vec![];

        let html = render_workflow_detail_page(&templates, &version, &instances);

        assert!(html.contains("my_workflow"));
        assert!(html.contains("Concurrent"));
        assert!(html.contains("hash123456789")); // full hash
    }

    #[test]
    fn test_render_workflow_detail_page_with_instances() {
        let templates = test_templates();
        let version_id = Uuid::new_v4();
        let version = crate::db::WorkflowVersion {
            id: version_id,
            workflow_name: "my_workflow".to_string(),
            dag_hash: "hash123".to_string(),
            program_proto: vec![],
            concurrent: false,
            created_at: chrono::Utc::now(),
        };
        let instances = vec![crate::db::WorkflowInstance {
            id: Uuid::new_v4(),
            partition_id: 0,
            workflow_name: "my_workflow".to_string(),
            workflow_version_id: Some(version_id),
            next_action_seq: 5,
            input_payload: None,
            result_payload: None,
            status: "completed".to_string(),
            created_at: chrono::Utc::now(),
            completed_at: Some(chrono::Utc::now()),
        }];

        let html = render_workflow_detail_page(&templates, &version, &instances);

        assert!(html.contains("my_workflow"));
        assert!(html.contains("Serial")); // not concurrent
        assert!(html.contains("completed"));
        assert!(html.contains("Done")); // progress shows "Done" for completed status
    }

    #[test]
    fn test_render_workflow_run_page() {
        let templates = test_templates();
        let version_id = Uuid::new_v4();
        let instance_id = Uuid::new_v4();

        let version = crate::db::WorkflowVersion {
            id: version_id,
            workflow_name: "test_workflow".to_string(),
            dag_hash: "hash123".to_string(),
            program_proto: vec![],
            concurrent: false,
            created_at: chrono::Utc::now(),
        };

        let instance = crate::db::WorkflowInstance {
            id: instance_id,
            partition_id: 0,
            workflow_name: "test_workflow".to_string(),
            workflow_version_id: Some(version_id),
            next_action_seq: 3,
            input_payload: Some(b"{\"arg\": 42}".to_vec()),
            result_payload: Some(b"{\"result\": 100}".to_vec()),
            status: "completed".to_string(),
            created_at: chrono::Utc::now(),
            completed_at: Some(chrono::Utc::now()),
        };

        let actions: Vec<crate::db::QueuedAction> = vec![];

        let html = render_workflow_run_page(&templates, &version, &instance, &actions);

        assert!(html.contains("test_workflow"));
        assert!(html.contains("completed"));
        assert!(html.contains("arg")); // from input payload
        assert!(html.contains("42"));
        assert!(html.contains("result")); // from result payload
        assert!(html.contains("100"));
    }

    #[test]
    fn test_render_workflow_run_page_with_actions() {
        let templates = test_templates();
        let version_id = Uuid::new_v4();
        let instance_id = Uuid::new_v4();

        let version = crate::db::WorkflowVersion {
            id: version_id,
            workflow_name: "action_workflow".to_string(),
            dag_hash: "hash456".to_string(),
            program_proto: vec![],
            concurrent: true,
            created_at: chrono::Utc::now(),
        };

        let instance = crate::db::WorkflowInstance {
            id: instance_id,
            partition_id: 0,
            workflow_name: "action_workflow".to_string(),
            workflow_version_id: Some(version_id),
            next_action_seq: 2,
            input_payload: None,
            result_payload: None,
            status: "running".to_string(),
            created_at: chrono::Utc::now(),
            completed_at: None,
        };

        let actions = vec![crate::db::QueuedAction {
            id: Uuid::new_v4(),
            instance_id,
            partition_id: 0,
            action_seq: 1,
            module_name: "my_module".to_string(),
            action_name: "do_something".to_string(),
            dispatch_payload: b"{\"x\": 1}".to_vec(),
            timeout_seconds: 30,
            max_retries: 3,
            attempt_number: 1,
            delivery_token: Uuid::new_v4(),
            timeout_retry_limit: 2,
            retry_kind: "exponential".to_string(),
            node_id: Some("action_0".to_string()),
            node_type: "action".to_string(),
            result_payload: Some(b"{\"result\": 42}".to_vec()),
            success: Some(true),
            status: "completed".to_string(),
            scheduled_at: None,
            last_error: None,
        }];

        let html = render_workflow_run_page(&templates, &version, &instance, &actions);

        assert!(html.contains("action_workflow"));
        assert!(html.contains("running"));
        assert!(html.contains("my_module"));
        assert!(html.contains("do_something"));
        assert!(html.contains("action_0"));
    }

    #[test]
    fn test_decode_dag_from_proto_empty() {
        let nodes = decode_dag_from_proto(&[]);
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_decode_dag_from_proto_invalid() {
        let nodes = decode_dag_from_proto(&[0xFF, 0xFE, 0x00]);
        assert!(nodes.is_empty());
    }

    // ========================================================================
    // Schedule Template Tests
    // ========================================================================

    #[test]
    fn test_format_interval_seconds() {
        assert_eq!(format_interval(Some(30)), "every 30 seconds");
        assert_eq!(format_interval(Some(1)), "every 1 seconds");
    }

    #[test]
    fn test_format_interval_minutes() {
        assert_eq!(format_interval(Some(60)), "every minute");
        assert_eq!(format_interval(Some(120)), "every 2 minutes");
        assert_eq!(format_interval(Some(300)), "every 5 minutes");
    }

    #[test]
    fn test_format_interval_hours() {
        assert_eq!(format_interval(Some(3600)), "every hour");
        assert_eq!(format_interval(Some(7200)), "every 2 hours");
        assert_eq!(format_interval(Some(14400)), "every 4 hours");
    }

    #[test]
    fn test_format_interval_days() {
        assert_eq!(format_interval(Some(86400)), "every day");
        assert_eq!(format_interval(Some(172800)), "every 2 days");
        assert_eq!(format_interval(Some(604800)), "every 7 days");
    }

    #[test]
    fn test_format_interval_none() {
        assert_eq!(format_interval(None), "not set");
    }

    #[test]
    fn test_render_scheduled_page_empty() {
        let templates = test_templates();
        let html = render_scheduled_page(&templates, &[]);

        assert!(html.contains("Scheduled Workflows"));
        assert!(html.contains("No scheduled workflows found"));
    }

    #[test]
    fn test_render_scheduled_page_with_cron_schedule() {
        let templates = test_templates();
        let schedules = vec![crate::db::WorkflowSchedule {
            id: Uuid::new_v4(),
            workflow_name: "cron_workflow".to_string(),
            schedule_type: "cron".to_string(),
            cron_expression: Some("0 * * * *".to_string()),
            interval_seconds: None,
            input_payload: None,
            status: "active".to_string(),
            next_run_at: Some(chrono::Utc::now()),
            last_run_at: None,
            last_instance_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }];

        let html = render_scheduled_page(&templates, &schedules);

        assert!(html.contains("Scheduled Workflows"));
        assert!(html.contains("cron_workflow"));
        assert!(html.contains("0 * * * *"));
        assert!(html.contains("Cron")); // group title
    }

    #[test]
    fn test_render_scheduled_page_with_interval_schedule() {
        let templates = test_templates();
        let schedules = vec![crate::db::WorkflowSchedule {
            id: Uuid::new_v4(),
            workflow_name: "interval_workflow".to_string(),
            schedule_type: "interval".to_string(),
            cron_expression: None,
            interval_seconds: Some(3600),
            input_payload: None,
            status: "paused".to_string(),
            next_run_at: None,
            last_run_at: Some(chrono::Utc::now()),
            last_instance_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }];

        let html = render_scheduled_page(&templates, &schedules);

        assert!(html.contains("Scheduled Workflows"));
        assert!(html.contains("interval_workflow"));
        assert!(html.contains("every hour"));
        assert!(html.contains("Interval")); // group title
    }

    #[test]
    fn test_render_schedule_detail_page() {
        let templates = test_templates();
        let schedule = crate::db::WorkflowSchedule {
            id: Uuid::new_v4(),
            workflow_name: "detail_workflow".to_string(),
            schedule_type: "cron".to_string(),
            cron_expression: Some("*/5 * * * *".to_string()),
            interval_seconds: None,
            input_payload: Some(b"{\"key\": \"value\"}".to_vec()),
            status: "active".to_string(),
            next_run_at: Some(chrono::Utc::now()),
            last_run_at: Some(chrono::Utc::now()),
            last_instance_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let invocations: Vec<crate::db::WorkflowInstance> = vec![];

        let html = render_schedule_detail_page(&templates, &schedule, &invocations, 1, 1);

        assert!(html.contains("detail_workflow"));
        // Tera HTML-escapes `/` as `&#x2F;` for security
        assert!(
            html.contains("*/5 * * * *") || html.contains("*&#x2F;5 * * * *"),
            "Expected cron expression not found in HTML"
        );
        assert!(html.contains("active"));
        assert!(html.contains("Input Payload"));
        assert!(html.contains("key"));
        assert!(html.contains("Pause Schedule")); // active schedule shows pause button
    }

    #[test]
    fn test_render_schedule_detail_page_paused() {
        let templates = test_templates();
        let schedule = crate::db::WorkflowSchedule {
            id: Uuid::new_v4(),
            workflow_name: "paused_workflow".to_string(),
            schedule_type: "interval".to_string(),
            cron_expression: None,
            interval_seconds: Some(300),
            input_payload: None,
            status: "paused".to_string(),
            next_run_at: None,
            last_run_at: None,
            last_instance_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let invocations: Vec<crate::db::WorkflowInstance> = vec![];

        let html = render_schedule_detail_page(&templates, &schedule, &invocations, 1, 1);

        assert!(html.contains("paused_workflow"));
        assert!(html.contains("every 5 minutes"));
        assert!(html.contains("Resume Schedule")); // paused schedule shows resume button
    }

    #[test]
    fn test_render_schedule_detail_page_with_invocations() {
        let templates = test_templates();
        let schedule_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();

        let schedule = crate::db::WorkflowSchedule {
            id: schedule_id,
            workflow_name: "invoked_workflow".to_string(),
            schedule_type: "cron".to_string(),
            cron_expression: Some("0 0 * * *".to_string()),
            interval_seconds: None,
            input_payload: None,
            status: "active".to_string(),
            next_run_at: Some(chrono::Utc::now()),
            last_run_at: Some(chrono::Utc::now()),
            last_instance_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        let invocations = vec![
            crate::db::WorkflowInstance {
                id: Uuid::new_v4(),
                partition_id: 0,
                workflow_name: "invoked_workflow".to_string(),
                workflow_version_id: Some(version_id),
                next_action_seq: 5,
                input_payload: None,
                result_payload: None,
                status: "completed".to_string(),
                created_at: chrono::Utc::now(),
                completed_at: Some(chrono::Utc::now()),
            },
            crate::db::WorkflowInstance {
                id: Uuid::new_v4(),
                partition_id: 0,
                workflow_name: "invoked_workflow".to_string(),
                workflow_version_id: Some(version_id),
                next_action_seq: 2,
                input_payload: None,
                result_payload: None,
                status: "running".to_string(),
                created_at: chrono::Utc::now(),
                completed_at: None,
            },
        ];

        let html = render_schedule_detail_page(&templates, &schedule, &invocations, 1, 3);

        assert!(html.contains("invoked_workflow"));
        assert!(html.contains("Recent Invocations"));
        assert!(html.contains("completed"));
        assert!(html.contains("running"));
        assert!(html.contains("Page 1 of 3")); // pagination
        assert!(html.contains("View")); // link to view run
    }

    // ========================================================================
    // HTTP Route Tests (require database)
    // These tests require RAPPEL_DATABASE_URL to be set and run with serial_test
    // to avoid conflicts with other database tests.
    // ========================================================================

    use axum::body::Body;
    use axum::http::{Request, StatusCode as HttpStatusCode};
    use serial_test::serial;
    use tower::ServiceExt;

    async fn test_db() -> Option<Database> {
        dotenvy::dotenv().ok();
        let url = std::env::var("RAPPEL_DATABASE_URL").ok()?;
        Some(
            Database::connect(&url)
                .await
                .expect("failed to connect to database"),
        )
    }

    fn build_test_app(database: Arc<Database>) -> Router {
        let templates = Arc::new(test_templates());
        let state = WebappState {
            database,
            templates,
        };

        Router::new()
            .route("/", get(list_workflows))
            .route("/workflow/:workflow_version_id", get(workflow_detail))
            .route(
                "/workflow/:workflow_version_id/run/:instance_id",
                get(workflow_run_detail),
            )
            .route("/healthz", get(healthz))
            .with_state(state)
    }

    #[tokio::test]
    #[serial]
    async fn test_route_healthz() {
        let Some(db) = test_db().await else {
            return;
        };
        let db = Arc::new(db);
        let app = build_test_app(db);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), HttpStatusCode::OK);

        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "ok");
        assert_eq!(json["service"], "rappel-webapp");
    }

    #[tokio::test]
    #[serial]
    async fn test_route_list_workflows() {
        let Some(db) = test_db().await else {
            return;
        };
        let db = Arc::new(db);
        let app = build_test_app(db);

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), HttpStatusCode::OK);

        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();

        // Should render the home page
        assert!(html.contains("Registered Workflow Versions"));
        assert!(html.contains("<!DOCTYPE html>"));
    }

    #[tokio::test]
    #[serial]
    async fn test_route_workflow_detail_not_found() {
        let Some(db) = test_db().await else {
            return;
        };
        let db = Arc::new(db);
        let app = build_test_app(db);

        // Use a random UUID that won't exist
        let fake_id = Uuid::new_v4();
        let uri = format!("/workflow/{}", fake_id);

        let response = app
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), HttpStatusCode::OK); // Returns 200 with error page

        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("Workflow not found"));
    }

    #[tokio::test]
    #[serial]
    async fn test_route_workflow_run_not_found() {
        let Some(db) = test_db().await else {
            return;
        };
        let db = Arc::new(db);
        let app = build_test_app(db);

        let fake_version_id = Uuid::new_v4();
        let fake_instance_id = Uuid::new_v4();
        let uri = format!("/workflow/{}/run/{}", fake_version_id, fake_instance_id);

        let response = app
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), HttpStatusCode::OK); // Returns 200 with error page

        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();

        // Should show workflow not found (version check fails first)
        assert!(html.contains("not found"));
    }

    #[tokio::test]
    #[serial]
    async fn test_route_workflow_detail_with_data() {
        let Some(db) = test_db().await else {
            return;
        };
        let db = Arc::new(db);

        // Create a test workflow version
        let version_id = db
            .upsert_workflow_version(
                "webapp_test_workflow",
                "test_hash_webapp",
                b"test proto",
                false,
            )
            .await
            .expect("failed to create version");

        let app = build_test_app(Arc::clone(&db));
        let uri = format!("/workflow/{}", version_id.0);

        let response = app
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), HttpStatusCode::OK);

        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("webapp_test_workflow"));
        assert!(html.contains("test_hash_webapp")); // full hash
        assert!(html.contains("Serial")); // not concurrent
    }

    #[tokio::test]
    #[serial]
    async fn test_route_workflow_run_with_data() {
        let Some(db) = test_db().await else {
            return;
        };
        let db = Arc::new(db);

        // Create a test workflow version
        let version_id = db
            .upsert_workflow_version(
                "webapp_run_test_workflow",
                "run_test_hash",
                b"test proto",
                true,
            )
            .await
            .expect("failed to create version");

        // Create an instance
        let instance_id = db
            .create_instance("webapp_run_test_workflow", version_id, Some(b"{\"x\": 1}"))
            .await
            .expect("failed to create instance");

        let app = build_test_app(Arc::clone(&db));
        let uri = format!("/workflow/{}/run/{}", version_id.0, instance_id.0);

        let response = app
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), HttpStatusCode::OK);

        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("webapp_run_test_workflow"));
        // Page should render successfully with instance data
        assert!(html.contains("Run Created"));
        assert!(html.contains("Status"));
    }
}
