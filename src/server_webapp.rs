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
use chrono::{Duration as ChronoDuration, Utc};
use serde::Serialize;
use tera::{Context as TeraContext, Tera};
use tokio::net::TcpListener;
use tracing::{error, info};
use uuid::Uuid;

use crate::config::WebappConfig;
use crate::db::{Database, ScheduleId, WorkerStatus, WorkflowVersionId, WorkflowVersionSummary};

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
const TEMPLATE_INVOCATIONS: &str = include_str!("../templates/invocations.html");
const TEMPLATE_WORKERS: &str = include_str!("../templates/workers.html");

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
    tera.add_raw_template("invocations.html", TEMPLATE_INVOCATIONS)
        .context("failed to add invocations.html template")?;
    tera.add_raw_template("workers.html", TEMPLATE_WORKERS)
        .context("failed to add workers.html template")?;

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
        .route("/", get(list_invocations))
        .route("/invocations", get(list_invocations))
        .route("/workers", get(list_workers))
        .route("/workflows", get(list_workflows))
        .route("/workflows/:workflow_version_id", get(workflow_detail))
        .route(
            "/workflows/:workflow_version_id/run/:instance_id",
            get(workflow_run_detail),
        )
        .route("/scheduled", get(list_schedules))
        .route("/scheduled/:schedule_id", get(schedule_detail))
        .route("/scheduled/:schedule_id/pause", post(pause_schedule))
        .route("/scheduled/:schedule_id/resume", post(resume_schedule))
        .route("/scheduled/:schedule_id/delete", post(delete_schedule))
        .route("/healthz", get(healthz))
        // API endpoints for column filter dropdowns
        .route(
            "/api/invocations/filter-values/:column",
            get(get_invocation_filter_values),
        )
        .route(
            "/api/scheduled/filter-values/:column",
            get(get_schedule_filter_values),
        )
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

    // Load action logs for this instance
    let action_logs = state
        .database
        .get_instance_action_logs(crate::db::WorkflowInstanceId(instance_id))
        .await
        .unwrap_or_default();

    Html(render_workflow_run_page(
        &state.templates,
        &version,
        &instance,
        &actions,
        &action_logs,
    ))
}

#[derive(Debug, serde::Deserialize)]
struct InvocationListQuery {
    page: Option<i64>,
    q: Option<String>,
}

async fn list_invocations(
    State(state): State<WebappState>,
    axum::extract::Query(query): axum::extract::Query<InvocationListQuery>,
) -> impl IntoResponse {
    let per_page = 50i64;
    let search = query.q.as_deref().filter(|value| !value.trim().is_empty());
    let total_count = match state.database.count_invocations(search).await {
        Ok(count) => count,
        Err(err) => {
            error!(?err, "failed to count invocations");
            return Html(render_error_page(
                &state.templates,
                "Unable to load invocations",
                "We couldn't fetch workflow invocations. Please check the database connection.",
            ));
        }
    };

    let total_pages = (total_count as f64 / per_page as f64).ceil() as i64;
    let current_page = query.page.unwrap_or(1).max(1).min(total_pages.max(1));
    let offset = (current_page - 1) * per_page;

    match state
        .database
        .list_invocations_page(search, per_page, offset)
        .await
    {
        Ok(invocations) => Html(render_invocations_page(
            &state.templates,
            &invocations,
            current_page,
            total_pages,
            search.map(|value| value.to_string()),
            total_count,
        )),
        Err(err) => {
            error!(?err, "failed to load invocations");
            Html(render_error_page(
                &state.templates,
                "Unable to load invocations",
                "We couldn't fetch workflow invocations. Please check the database connection.",
            ))
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct WorkerStatusQuery {
    minutes: Option<i64>,
}

async fn list_workers(
    State(state): State<WebappState>,
    axum::extract::Query(query): axum::extract::Query<WorkerStatusQuery>,
) -> impl IntoResponse {
    let minutes = query.minutes.unwrap_or(5).max(1);
    let since = Utc::now() - ChronoDuration::minutes(minutes);

    match state.database.list_worker_statuses_recent(since).await {
        Ok(workers) => Html(render_workers_page(&state.templates, &workers, minutes)),
        Err(err) => {
            error!(?err, "failed to load worker status");
            Html(render_error_page(
                &state.templates,
                "Unable to load worker status",
                "We couldn't fetch worker throughput stats. Please check the database connection.",
            ))
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ScheduleListQuery {
    page: Option<i64>,
    q: Option<String>,
}

async fn list_schedules(
    State(state): State<WebappState>,
    axum::extract::Query(query): axum::extract::Query<ScheduleListQuery>,
) -> impl IntoResponse {
    let per_page = 20i64;
    let search = query.q.as_deref().filter(|value| !value.trim().is_empty());
    let total_count = match state.database.count_schedules(search).await {
        Ok(count) => count,
        Err(err) => {
            error!(?err, "failed to count schedules");
            return Html(render_error_page(
                &state.templates,
                "Unable to load schedules",
                "We couldn't fetch scheduled workflows. Please check the database connection.",
            ));
        }
    };

    let total_pages = (total_count as f64 / per_page as f64).ceil() as i64;
    let current_page = query.page.unwrap_or(1).max(1).min(total_pages.max(1));
    let offset = (current_page - 1) * per_page;

    match state
        .database
        .list_schedules_page(search, per_page, offset)
        .await
    {
        Ok(schedules) => Html(render_scheduled_page(
            &state.templates,
            &schedules,
            current_page,
            total_pages,
            search.map(|value| value.to_string()),
            total_count,
        )),
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
        .list_schedule_invocations(ScheduleId(schedule.id), per_page, offset)
        .await
        .unwrap_or_default();

    // Get total count for pagination
    let total_count = state
        .database
        .count_schedule_invocations(ScheduleId(schedule.id))
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
// API Handlers: Filter Values
// ============================================================================

async fn get_invocation_filter_values(
    State(state): State<WebappState>,
    Path(column): Path<String>,
) -> impl IntoResponse {
    let result = match column.as_str() {
        "workflow" => state.database.get_distinct_invocation_workflows().await,
        "status" => state.database.get_distinct_invocation_statuses().await,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(FilterValuesResponse { values: vec![] }),
            );
        }
    };

    match result {
        Ok(values) => (StatusCode::OK, Json(FilterValuesResponse { values })),
        Err(err) => {
            error!(?err, %column, "failed to fetch invocation filter values");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FilterValuesResponse { values: vec![] }),
            )
        }
    }
}

async fn get_schedule_filter_values(
    State(state): State<WebappState>,
    Path(column): Path<String>,
) -> impl IntoResponse {
    let result = match column.as_str() {
        "workflow" => state.database.get_distinct_schedule_workflows().await,
        "status" => state.database.get_distinct_schedule_statuses().await,
        "schedule_type" => state.database.get_distinct_schedule_types().await,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(FilterValuesResponse { values: vec![] }),
            );
        }
    };

    match result {
        Ok(values) => (StatusCode::OK, Json(FilterValuesResponse { values })),
        Err(err) => {
            error!(?err, %column, "failed to fetch schedule filter values");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FilterValuesResponse { values: vec![] }),
            )
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

#[derive(Debug, Serialize)]
struct FilterValuesResponse {
    values: Vec<String>,
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
    ir_text: String,
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

    // Build graph data, filtering out internal nodes
    let graph_data = build_filtered_workflow_graph(&dag);

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
                url: format!("/workflows/{}/run/{}", version.id, i.id),
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

    let ir_text = format_ir_from_proto(&version.program_proto);

    let context = WorkflowDetailPageContext {
        title: format!("{} - Workflow Detail", version.workflow_name),
        active_tab: "workflows".to_string(),
        workflow,
        ir_text,
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
    /// JSON-encoded action logs for client-side use (keyed by action_id)
    action_logs_json: String,
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
    /// The action queue ID (for looking up execution logs)
    action_id: String,
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

/// Context for action execution log entries (shows retry history)
#[derive(Serialize)]
struct ActionLogContext {
    /// Unique log ID for stable sorting
    id: String,
    /// The action ID this log belongs to
    action_id: String,
    /// Attempt number (0-indexed)
    attempt_number: i32,
    /// When this attempt was dispatched (with milliseconds for precise sorting)
    dispatched_at: String,
    /// When this attempt completed (if completed)
    completed_at: Option<String>,
    /// Whether this attempt succeeded
    success: Option<bool>,
    /// Duration in milliseconds
    duration_ms: Option<i64>,
    /// Error message if failed
    error_message: Option<String>,
    /// Result payload (formatted as JSON string)
    result_payload: Option<String>,
}

fn render_workflow_run_page(
    templates: &Tera,
    version: &crate::db::WorkflowVersion,
    instance: &crate::db::WorkflowInstance,
    actions: &[crate::db::QueuedAction],
    action_logs: &[crate::db::ActionLog],
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

    // Build execution graph data with status info, filtering out internal nodes
    let graph_data = build_filtered_execution_graph(&dag, &action_status);

    let nodes: Vec<NodeExecutionContext> = actions
        .iter()
        .map(|a| NodeExecutionContext {
            id: a.node_id.clone().unwrap_or_else(|| a.id.to_string()),
            action_id: a.id.to_string(),
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

    // Build action logs map: action_id -> Vec<ActionLogContext>
    let mut logs_by_action: std::collections::HashMap<String, Vec<ActionLogContext>> =
        std::collections::HashMap::new();
    for log in action_logs {
        let log_ctx = ActionLogContext {
            id: log.id.to_string(),
            action_id: log.action_id.to_string(),
            attempt_number: log.attempt_number,
            dispatched_at: log
                .dispatched_at
                .format("%Y-%m-%d %H:%M:%S%.3f UTC")
                .to_string(),
            completed_at: log
                .completed_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string()),
            success: log.success,
            duration_ms: log.duration_ms,
            error_message: log.error_message.clone(),
            result_payload: log
                .result_payload
                .as_ref()
                .map(|p| format_binary_payload(p)),
        };
        logs_by_action
            .entry(log.action_id.to_string())
            .or_default()
            .push(log_ctx);
    }
    let action_logs_json =
        serde_json::to_string(&logs_by_action).unwrap_or_else(|_| "{}".to_string());

    let context = WorkflowRunPageContext {
        title: format!("Run {} - {}", instance.id, version.workflow_name),
        active_tab: "workflows".to_string(),
        workflow,
        instance: instance_ctx,
        nodes,
        graph_data,
        nodes_json,
        action_logs_json,
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
// Invocation Template Context
// ============================================================================

#[derive(Serialize)]
struct InvocationsPageContext {
    title: String,
    active_tab: String,
    invocations: Vec<InvocationListItem>,
    has_invocations: bool,
    current_page: i64,
    total_pages: i64,
    has_pagination: bool,
    search_query: Option<String>,
    total_count: i64,
}

#[derive(Serialize)]
struct InvocationListItem {
    id: String,
    workflow_name: String,
    workflow_version_id: Option<String>,
    created_at: String,
    status: String,
    input_preview: String,
}

fn render_invocations_page(
    templates: &Tera,
    instances: &[crate::db::WorkflowInstance],
    current_page: i64,
    total_pages: i64,
    search_query: Option<String>,
    total_count: i64,
) -> String {
    let invocations: Vec<InvocationListItem> = instances
        .iter()
        .map(|i| InvocationListItem {
            id: i.id.to_string(),
            workflow_name: i.workflow_name.clone(),
            workflow_version_id: i.workflow_version_id.map(|id| id.to_string()),
            created_at: i.created_at.to_rfc3339(),
            status: i.status.clone(),
            input_preview: truncate_payload(&i.input_payload, 240),
        })
        .collect();

    let context = InvocationsPageContext {
        title: "Workflow Invocations".to_string(),
        active_tab: "invocations".to_string(),
        invocations,
        has_invocations: !instances.is_empty(),
        current_page,
        total_pages,
        has_pagination: total_pages > 1,
        search_query,
        total_count,
    };

    render_template(templates, "invocations.html", &context)
}

// ========================================================================
// Worker Status Template Context
// ========================================================================

#[derive(Serialize)]
struct WorkersPageContext {
    title: String,
    active_tab: String,
    window_minutes: i64,
    workers: Vec<WorkerStatusRow>,
    has_workers: bool,
}

#[derive(Serialize)]
struct WorkerStatusRow {
    pool_id: String,
    worker_id: i64,
    throughput_per_min: String,
    total_completed: i64,
    last_action_at: Option<String>,
    updated_at: String,
}

fn render_workers_page(templates: &Tera, statuses: &[WorkerStatus], window_minutes: i64) -> String {
    let workers = statuses
        .iter()
        .map(|status| WorkerStatusRow {
            pool_id: status.pool_id.to_string(),
            worker_id: status.worker_id,
            throughput_per_min: format!("{:.2}", status.throughput_per_min),
            total_completed: status.total_completed,
            last_action_at: status.last_action_at.map(|dt| dt.to_rfc3339()),
            updated_at: status.updated_at.to_rfc3339(),
        })
        .collect();

    let context = WorkersPageContext {
        title: "Worker Throughput".to_string(),
        active_tab: "workers".to_string(),
        window_minutes,
        workers,
        has_workers: !statuses.is_empty(),
    };

    render_template(templates, "workers.html", &context)
}

// ============================================================================
// Schedule Template Context
// ============================================================================

#[derive(Serialize)]
struct ScheduledPageContext {
    title: String,
    active_tab: String,
    schedules: Vec<ScheduleBrief>,
    current_page: i64,
    total_pages: i64,
    has_pagination: bool,
    search_query: Option<String>,
    total_count: i64,
}

#[derive(Serialize)]
struct ScheduleBrief {
    id: String,
    schedule_name: String,
    workflow_name: String,
    schedule_type: String,
    status: String,
    schedule_expression: String,
    next_run_at: Option<String>,
    last_run_at: Option<String>,
}

fn render_scheduled_page(
    templates: &Tera,
    schedules: &[crate::db::WorkflowSchedule],
    current_page: i64,
    total_pages: i64,
    search_query: Option<String>,
    total_count: i64,
) -> String {
    let schedule_briefs: Vec<ScheduleBrief> = schedules
        .iter()
        .map(|s| {
            let schedule_expression = if s.schedule_type == "cron" {
                s.cron_expression.clone().unwrap_or_default()
            } else {
                format_interval(s.interval_seconds)
            };

            ScheduleBrief {
                id: s.id.to_string(),
                schedule_name: s.schedule_name.clone(),
                workflow_name: s.workflow_name.clone(),
                schedule_type: s.schedule_type.clone(),
                status: s.status.clone(),
                schedule_expression,
                next_run_at: s.next_run_at.map(|dt| dt.to_rfc3339()),
                last_run_at: s.last_run_at.map(|dt| dt.to_rfc3339()),
            }
        })
        .collect();

    let context = ScheduledPageContext {
        title: "Scheduled Workflows".to_string(),
        active_tab: "scheduled".to_string(),
        schedules: schedule_briefs,
        current_page,
        total_pages,
        has_pagination: total_pages > 1,
        search_query,
        total_count,
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
    schedule_name: String,
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
        schedule_name: schedule.schedule_name.clone(),
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
        title: format!("{} - Schedule", schedule.schedule_name),
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
    let dag = match crate::dag::convert_to_dag(&program) {
        Ok(d) => d,
        Err(_) => return vec![],
    };

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

/// Build the workflow graph data, filtering out internal nodes and collapsing edges.
///
/// Internal nodes (those with empty module) are hidden from the visualization,
/// but their edges are preserved by connecting their predecessors directly to
/// their successors. Handles cycles safely.
fn build_filtered_workflow_graph(dag: &[SimpleDagNode]) -> WorkflowGraphData {
    let (internal_nodes, depends_on_map) = build_node_maps(dag);
    let collapsed = collapse_internal_nodes(&internal_nodes, &depends_on_map);

    WorkflowGraphData {
        nodes: dag
            .iter()
            .filter(|node| !internal_nodes.contains(&node.id))
            .map(|node| WorkflowGraphNode {
                id: node.id.clone(),
                action: if node.action.is_empty() {
                    "action".to_string()
                } else {
                    node.action.clone()
                },
                module: node.module.clone(),
                depends_on: collapsed.get(&node.id).cloned().unwrap_or_default(),
            })
            .collect(),
    }
}

/// Build the execution graph data, filtering out internal nodes and collapsing edges.
///
/// Internal nodes (those with empty module) are hidden from the visualization,
/// but their edges are preserved by connecting their predecessors directly to
/// their successors. Handles cycles safely.
fn build_filtered_execution_graph(
    dag: &[SimpleDagNode],
    action_status: &std::collections::HashMap<String, String>,
) -> ExecutionGraphData {
    let (internal_nodes, depends_on_map) = build_node_maps(dag);
    let collapsed = collapse_internal_nodes(&internal_nodes, &depends_on_map);

    ExecutionGraphData {
        nodes: dag
            .iter()
            .filter(|node| !internal_nodes.contains(&node.id))
            .map(|node| ExecutionGraphNode {
                id: node.id.clone(),
                action: if node.action.is_empty() {
                    "action".to_string()
                } else {
                    node.action.clone()
                },
                module: node.module.clone(),
                depends_on: collapsed.get(&node.id).cloned().unwrap_or_default(),
                status: action_status
                    .get(&node.id)
                    .cloned()
                    .unwrap_or_else(|| "pending".to_string()),
            })
            .collect(),
    }
}

/// Build lookup maps for node filtering.
fn build_node_maps(
    dag: &[SimpleDagNode],
) -> (
    std::collections::HashSet<String>,
    std::collections::HashMap<String, Vec<String>>,
) {
    let internal_nodes: std::collections::HashSet<String> = dag
        .iter()
        .filter(|node| node.module.is_empty())
        .map(|node| node.id.clone())
        .collect();

    let depends_on_map: std::collections::HashMap<String, Vec<String>> = dag
        .iter()
        .map(|node| (node.id.clone(), node.depends_on.clone()))
        .collect();

    (internal_nodes, depends_on_map)
}

/// Collapse internal nodes by computing effective dependencies for each non-internal node.
/// Uses DFS with cycle detection to handle graphs with cycles safely.
fn collapse_internal_nodes(
    internal_nodes: &std::collections::HashSet<String>,
    depends_on_map: &std::collections::HashMap<String, Vec<String>>,
) -> std::collections::HashMap<String, Vec<String>> {
    let mut result: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    let mut cache: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();

    // For each non-internal node, compute its effective dependencies
    for node_id in depends_on_map.keys() {
        if !internal_nodes.contains(node_id) {
            let effective_deps = compute_effective_deps(
                node_id,
                internal_nodes,
                depends_on_map,
                &mut cache,
                &mut std::collections::HashSet::new(),
            );
            result.insert(node_id.clone(), effective_deps.into_iter().collect());
        }
    }

    result
}

/// Recursively compute effective dependencies for a node, skipping internal nodes.
/// Uses a visiting set to detect and break cycles.
fn compute_effective_deps(
    node_id: &str,
    internal_nodes: &std::collections::HashSet<String>,
    depends_on_map: &std::collections::HashMap<String, Vec<String>>,
    cache: &mut std::collections::HashMap<String, std::collections::HashSet<String>>,
    visiting: &mut std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
    // Check cache first
    if let Some(cached) = cache.get(node_id) {
        return cached.clone();
    }

    // Cycle detection: if we're already visiting this node, return empty to break cycle
    if visiting.contains(node_id) {
        return std::collections::HashSet::new();
    }

    visiting.insert(node_id.to_string());

    let mut effective: std::collections::HashSet<String> = std::collections::HashSet::new();

    if let Some(deps) = depends_on_map.get(node_id) {
        for dep in deps {
            if internal_nodes.contains(dep) {
                // Internal node: recursively get its effective deps
                let transitive =
                    compute_effective_deps(dep, internal_nodes, depends_on_map, cache, visiting);
                effective.extend(transitive);
            } else {
                // Non-internal node: add directly
                effective.insert(dep.clone());
            }
        }
    }

    visiting.remove(node_id);
    cache.insert(node_id.to_string(), effective.clone());
    effective
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

fn truncate_payload(payload: &Option<Vec<u8>>, max_len: usize) -> String {
    let formatted = format_payload(payload);
    if formatted.len() > max_len {
        let mut truncated = formatted.chars().take(max_len).collect::<String>();
        truncated.push_str("...");
        truncated
    } else {
        formatted
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

fn format_ir_from_proto(proto_bytes: &[u8]) -> String {
    use prost::Message;

    if proto_bytes.is_empty() {
        return "(empty)".to_string();
    }

    let program = match crate::messages::ast::Program::decode(proto_bytes) {
        Ok(program) => program,
        Err(_) => return "(unable to decode IR)".to_string(),
    };

    crate::ir_printer::print_program(&program)
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
            schedule_id: None,
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
            schedule_id: None,
            next_action_seq: 3,
            input_payload: Some(b"{\"arg\": 42}".to_vec()),
            result_payload: Some(b"{\"result\": 100}".to_vec()),
            status: "completed".to_string(),
            created_at: chrono::Utc::now(),
            completed_at: Some(chrono::Utc::now()),
        };

        let actions: Vec<crate::db::QueuedAction> = vec![];
        let action_logs: Vec<crate::db::ActionLog> = vec![];

        let html =
            render_workflow_run_page(&templates, &version, &instance, &actions, &action_logs);

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
            schedule_id: None,
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
        let action_logs: Vec<crate::db::ActionLog> = vec![];

        let html =
            render_workflow_run_page(&templates, &version, &instance, &actions, &action_logs);

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

    #[test]
    fn test_render_invocations_page_empty() {
        let templates = test_templates();
        let html = render_invocations_page(&templates, &[], 1, 1, None, 0);

        assert!(html.contains("Invocations"));
        assert!(html.contains("No invocations recorded yet"));
    }

    #[test]
    fn test_render_invocations_page_with_entries() {
        let templates = test_templates();
        let instances = vec![crate::db::WorkflowInstance {
            id: Uuid::new_v4(),
            partition_id: 0,
            workflow_name: "example_workflow".to_string(),
            workflow_version_id: Some(Uuid::new_v4()),
            schedule_id: None,
            next_action_seq: 1,
            input_payload: Some(b"{\"search\": \"needle\"}".to_vec()),
            result_payload: None,
            status: "completed".to_string(),
            created_at: chrono::Utc::now(),
            completed_at: Some(chrono::Utc::now()),
        }];

        let html = render_invocations_page(&templates, &instances, 1, 1, None, 1);

        assert!(html.contains("Invocations"));
        assert!(html.contains("example_workflow"));
        assert!(html.contains("completed"));
        assert!(html.contains("search"));
    }

    #[test]
    fn test_render_workers_page_empty() {
        let templates = test_templates();
        let html = render_workers_page(&templates, &[], 5);

        assert!(html.contains("Workers"));
        assert!(html.contains("No active workers"));
    }

    #[test]
    fn test_render_workers_page_with_entries() {
        let templates = test_templates();
        let statuses = vec![crate::db::WorkerStatus {
            pool_id: Uuid::new_v4(),
            worker_id: 1,
            throughput_per_min: 2.5,
            total_completed: 42,
            last_action_at: Some(chrono::Utc::now()),
            updated_at: chrono::Utc::now(),
        }];

        let html = render_workers_page(&templates, &statuses, 5);

        assert!(html.contains("Workers"));
        assert!(html.contains("42"));
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
        let html = render_scheduled_page(&templates, &[], 1, 1, None, 0);

        assert!(html.contains("Scheduled Workflows"));
        assert!(html.contains("No scheduled workflows found"));
    }

    #[test]
    fn test_render_scheduled_page_with_cron_schedule() {
        let templates = test_templates();
        let schedules = vec![crate::db::WorkflowSchedule {
            id: Uuid::new_v4(),
            workflow_name: "cron_workflow".to_string(),
            schedule_name: "cron_schedule".to_string(),
            schedule_type: "cron".to_string(),
            cron_expression: Some("0 * * * *".to_string()),
            interval_seconds: None,
            jitter_seconds: 0,
            input_payload: None,
            status: "active".to_string(),
            next_run_at: Some(chrono::Utc::now()),
            last_run_at: None,
            last_instance_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }];

        let html = render_scheduled_page(&templates, &schedules, 1, 1, None, 1);

        assert!(html.contains("Scheduled Workflows"));
        assert!(html.contains("cron_schedule"));
        assert!(html.contains("0 * * * *"));
        assert!(html.contains("cron")); // schedule type badge
    }

    #[test]
    fn test_render_scheduled_page_with_interval_schedule() {
        let templates = test_templates();
        let schedules = vec![crate::db::WorkflowSchedule {
            id: Uuid::new_v4(),
            workflow_name: "interval_workflow".to_string(),
            schedule_name: "interval_schedule".to_string(),
            schedule_type: "interval".to_string(),
            cron_expression: None,
            interval_seconds: Some(3600),
            jitter_seconds: 0,
            input_payload: None,
            status: "paused".to_string(),
            next_run_at: None,
            last_run_at: Some(chrono::Utc::now()),
            last_instance_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }];

        let html = render_scheduled_page(&templates, &schedules, 1, 1, None, 1);

        assert!(html.contains("Scheduled Workflows"));
        assert!(html.contains("interval_schedule"));
        assert!(html.contains("every hour"));
        assert!(html.contains("interval")); // schedule type badge
    }

    #[test]
    fn test_render_schedule_detail_page() {
        let templates = test_templates();
        let schedule = crate::db::WorkflowSchedule {
            id: Uuid::new_v4(),
            workflow_name: "detail_workflow".to_string(),
            schedule_name: "detail_schedule".to_string(),
            schedule_type: "cron".to_string(),
            cron_expression: Some("*/5 * * * *".to_string()),
            interval_seconds: None,
            jitter_seconds: 0,
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

        assert!(html.contains("detail_schedule"));
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
            schedule_name: "paused_schedule".to_string(),
            schedule_type: "interval".to_string(),
            cron_expression: None,
            interval_seconds: Some(300),
            jitter_seconds: 0,
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

        assert!(html.contains("paused_schedule"));
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
            schedule_name: "invoked_schedule".to_string(),
            schedule_type: "cron".to_string(),
            cron_expression: Some("0 0 * * *".to_string()),
            interval_seconds: None,
            jitter_seconds: 0,
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
                schedule_id: None,
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
                schedule_id: None,
                next_action_seq: 2,
                input_payload: None,
                result_payload: None,
                status: "running".to_string(),
                created_at: chrono::Utc::now(),
                completed_at: None,
            },
        ];

        let html = render_schedule_detail_page(&templates, &schedule, &invocations, 1, 3);

        assert!(html.contains("invoked_schedule"));
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
            .route("/", get(list_invocations))
            .route("/invocations", get(list_invocations))
            .route("/workflows", get(list_workflows))
            .route("/workflows/:workflow_version_id", get(workflow_detail))
            .route(
                "/workflows/:workflow_version_id/run/:instance_id",
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
    async fn test_route_list_invocations() {
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

        // Should render the invocations page
        assert!(html.contains("Invocations"));
        assert!(html.contains("<!DOCTYPE html>"));
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
            .oneshot(
                Request::builder()
                    .uri("/workflows")
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
        let html = String::from_utf8(body.to_vec()).unwrap();

        // Should render the workflows page
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
        let uri = format!("/workflows/{}", fake_id);

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
        let uri = format!("/workflows/{}/run/{}", fake_version_id, fake_instance_id);

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
        let uri = format!("/workflows/{}", version_id.0);

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
            .create_instance(
                "webapp_run_test_workflow",
                version_id,
                Some(b"{\"x\": 1}"),
                None,
            )
            .await
            .expect("failed to create instance");

        let app = build_test_app(Arc::clone(&db));
        let uri = format!("/workflows/{}/run/{}", version_id.0, instance_id.0);

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
