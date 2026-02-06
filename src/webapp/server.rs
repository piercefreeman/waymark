//! Web application server for the Rappel workflow dashboard.

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use chrono::Utc;
use serde::Serialize;
use tera::{Context as TeraContext, Tera};
use tokio::net::TcpListener;
use tracing::{error, info};
use uuid::Uuid;

use super::types::{
    ActionLogsResponse, FilterValuesResponse, HealthResponse, InstanceExportInfo, TimelineEntry,
    WebappConfig, WorkflowInstanceExport, WorkflowRunDataResponse,
};
use crate::backends::WebappBackend;

// Embed templates at compile time
const TEMPLATE_BASE: &str = include_str!("../../templates/base.html");
const TEMPLATE_MACROS: &str = include_str!("../../templates/macros.html");
const TEMPLATE_HOME: &str = include_str!("../../templates/home.html");
const TEMPLATE_ERROR: &str = include_str!("../../templates/error.html");
const TEMPLATE_WORKFLOW: &str = include_str!("../../templates/workflow.html");
const TEMPLATE_WORKFLOW_RUN: &str = include_str!("../../templates/workflow_run.html");
const TEMPLATE_INVOCATIONS: &str = include_str!("../../templates/invocations.html");
const TEMPLATE_SCHEDULED: &str = include_str!("../../templates/scheduled.html");
const TEMPLATE_SCHEDULE_DETAIL: &str = include_str!("../../templates/schedule_detail.html");
const TEMPLATE_WORKERS: &str = include_str!("../../templates/workers.html");

/// Webapp server handle.
pub struct WebappServer {
    addr: SocketAddr,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl WebappServer {
    /// Start the webapp server.
    ///
    /// Returns None if the webapp is disabled via configuration.
    pub async fn start(
        config: WebappConfig,
        database: Arc<dyn WebappBackend>,
    ) -> Result<Option<Self>> {
        if !config.enabled {
            info!("webapp disabled (set RAPPEL_WEBAPP_ENABLED=true to enable)");
            return Ok(None);
        }

        let bind_addr = config.bind_addr();
        let listener = TcpListener::bind(&bind_addr)
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

    /// Get the address the server is bound to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Shutdown the server.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Initialize Tera templates from embedded strings.
fn init_templates() -> Result<Tera> {
    let mut tera = Tera::default();

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
    tera.add_raw_template("invocations.html", TEMPLATE_INVOCATIONS)
        .context("failed to add invocations.html template")?;
    tera.add_raw_template("scheduled.html", TEMPLATE_SCHEDULED)
        .context("failed to add scheduled.html template")?;
    tera.add_raw_template("schedule_detail.html", TEMPLATE_SCHEDULE_DETAIL)
        .context("failed to add schedule_detail.html template")?;
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
    database: Arc<dyn WebappBackend>,
    templates: Arc<Tera>,
}

async fn run_server(
    listener: TcpListener,
    state: WebappState,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let app = build_router(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await
        .ok();
}

fn build_router(state: WebappState) -> Router {
    use axum::routing::post;

    Router::new()
        .route("/", get(list_invocations))
        .route("/invocations", get(list_invocations))
        .route("/instance/{instance_id}", get(instance_detail))
        .route("/api/instance/{instance_id}/run-data", get(get_run_data))
        .route(
            "/api/instance/{instance_id}/action-logs/{action_id}",
            get(get_action_logs),
        )
        .route("/api/instance/{instance_id}/export", get(export_instance))
        .route(
            "/api/invocations/filter-values/{column}",
            get(get_filter_values),
        )
        // Worker routes
        .route("/workers", get(list_workers))
        // Schedule routes
        .route("/scheduled", get(list_schedules))
        .route("/scheduled/{schedule_id}", get(schedule_detail))
        .route("/scheduled/{schedule_id}/pause", post(pause_schedule))
        .route("/scheduled/{schedule_id}/resume", post(resume_schedule))
        .route("/scheduled/{schedule_id}/delete", post(delete_schedule))
        .route(
            "/api/scheduled/filter-values/{column}",
            get(get_schedule_filter_values),
        )
        .route("/healthz", get(healthz))
        .with_state(state)
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
    let search = query.q.as_deref().filter(|v| !v.trim().is_empty());

    let total_count = match state.database.count_instances(search).await {
        Ok(count) => count,
        Err(err) => {
            error!(?err, "failed to count instances");
            return Html(render_error_page(
                &state.templates,
                "Unable to load invocations",
                "We couldn't fetch workflow instances. Please check the database connection.",
            ));
        }
    };

    let total_pages = (total_count as f64 / per_page as f64).ceil() as i64;
    let current_page = query.page.unwrap_or(1).max(1).min(total_pages.max(1));
    let offset = (current_page - 1) * per_page;

    match state
        .database
        .list_instances(search, per_page, offset)
        .await
    {
        Ok(instances) => Html(render_invocations_page(
            &state.templates,
            &instances,
            current_page,
            total_pages,
            search.map(|s| s.to_string()),
            total_count,
        )),
        Err(err) => {
            error!(?err, "failed to load instances");
            Html(render_error_page(
                &state.templates,
                "Unable to load invocations",
                "We couldn't fetch workflow instances. Please check the database connection.",
            ))
        }
    }
}

async fn instance_detail(
    State(state): State<WebappState>,
    Path(instance_id): Path<Uuid>,
) -> impl IntoResponse {
    let instance = match state.database.get_instance(instance_id).await {
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

    // Get execution graph
    let graph = state
        .database
        .get_execution_graph(instance_id)
        .await
        .unwrap_or(None);

    Html(render_instance_detail_page(
        &state.templates,
        &instance,
        graph,
    ))
}

#[derive(Debug, serde::Deserialize)]
struct RunDataQuery {
    page: Option<i64>,
    per_page: Option<i64>,
    include_nodes: Option<bool>,
}

async fn get_run_data(
    State(state): State<WebappState>,
    Path(instance_id): Path<Uuid>,
    axum::extract::Query(query): axum::extract::Query<RunDataQuery>,
) -> Result<Json<WorkflowRunDataResponse>, HttpError> {
    state
        .database
        .get_instance(instance_id)
        .await
        .map_err(|err| {
            error!(?err, %instance_id, "failed to load instance");
            HttpError {
                status: StatusCode::NOT_FOUND,
                message: "workflow instance not found".to_string(),
            }
        })?;

    let per_page = query.per_page.unwrap_or(200).clamp(1, 1000);
    let page = query.page.unwrap_or(1).max(1);
    let include_nodes = query.include_nodes.unwrap_or(true);

    let mut nodes = Vec::new();
    if include_nodes
        && let Some(graph) = state
            .database
            .get_execution_graph(instance_id)
            .await
            .ok()
            .flatten()
    {
        nodes = graph.nodes;
    }

    let timeline = state
        .database
        .get_action_results(instance_id)
        .await
        .unwrap_or_default();

    let total = timeline.len() as i64;
    let start = ((page - 1) * per_page) as usize;
    let end = (start + per_page as usize).min(timeline.len());
    let paginated: Vec<TimelineEntry> = if start < timeline.len() {
        timeline[start..end].to_vec()
    } else {
        Vec::new()
    };

    let has_more = (page * per_page) < total;

    Ok(Json(WorkflowRunDataResponse {
        nodes,
        timeline: paginated,
        page,
        per_page,
        total,
        has_more,
    }))
}

async fn get_action_logs(
    State(state): State<WebappState>,
    Path((instance_id, action_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<ActionLogsResponse>, HttpError> {
    state
        .database
        .get_instance(instance_id)
        .await
        .map_err(|err| {
            error!(?err, %instance_id, "failed to load instance");
            HttpError {
                status: StatusCode::NOT_FOUND,
                message: "workflow instance not found".to_string(),
            }
        })?;

    let timeline = state
        .database
        .get_action_results(instance_id)
        .await
        .unwrap_or_default();

    let action_id_str = action_id.to_string();
    let logs: Vec<_> = timeline
        .into_iter()
        .filter(|e| e.action_id == action_id_str)
        .map(|e| super::types::ActionLogEntry {
            action_id: e.action_id,
            action_name: e.action_name,
            module_name: e.module_name,
            status: e.status,
            attempt_number: e.attempt_number,
            dispatched_at: e.dispatched_at,
            completed_at: e.completed_at,
            duration_ms: e.duration_ms,
            request: e.request_preview,
            response: e.response_preview,
            error: e.error,
        })
        .collect();

    Ok(Json(ActionLogsResponse { logs }))
}

async fn export_instance(
    State(state): State<WebappState>,
    Path(instance_id): Path<Uuid>,
) -> Result<Json<WorkflowInstanceExport>, HttpError> {
    let instance = state
        .database
        .get_instance(instance_id)
        .await
        .map_err(|err| {
            error!(?err, %instance_id, "failed to load instance");
            HttpError {
                status: StatusCode::NOT_FOUND,
                message: "workflow instance not found".to_string(),
            }
        })?;

    let nodes = state
        .database
        .get_execution_graph(instance_id)
        .await
        .ok()
        .flatten()
        .map(|g| g.nodes)
        .unwrap_or_default();

    let timeline = state
        .database
        .get_action_results(instance_id)
        .await
        .unwrap_or_default();

    let export = WorkflowInstanceExport {
        export_version: "1.0",
        exported_at: Utc::now().to_rfc3339(),
        instance: InstanceExportInfo {
            id: instance.id.to_string(),
            status: instance.status.to_string(),
            created_at: instance.created_at.to_rfc3339(),
            input_payload: instance.input_payload,
            result_payload: instance.result_payload,
        },
        nodes,
        timeline,
    };

    Ok(Json(export))
}

async fn get_filter_values(
    State(state): State<WebappState>,
    Path(column): Path<String>,
) -> impl IntoResponse {
    let result = match column.as_str() {
        "workflow" => state.database.get_distinct_workflows().await,
        "status" => state.database.get_distinct_statuses().await,
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
            error!(?err, %column, "failed to fetch filter values");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FilterValuesResponse { values: vec![] }),
            )
        }
    }
}

// ============================================================================
// Worker Handlers
// ============================================================================

#[derive(Debug, serde::Deserialize)]
struct WorkersQuery {
    minutes: Option<i64>,
}

async fn list_workers(
    State(state): State<WebappState>,
    axum::extract::Query(query): axum::extract::Query<WorkersQuery>,
) -> impl IntoResponse {
    let window_minutes = query.minutes.unwrap_or(5).clamp(1, 1440);

    // Check if worker_status table exists
    if !state.database.worker_status_table_exists().await {
        return Html(render_workers_page(&state.templates, &[], window_minutes));
    }

    let statuses = state
        .database
        .get_worker_statuses(window_minutes)
        .await
        .unwrap_or_default();

    Html(render_workers_page(
        &state.templates,
        &statuses,
        window_minutes,
    ))
}

// ============================================================================
// Schedule Handlers
// ============================================================================

#[derive(Debug, serde::Deserialize)]
struct ScheduleListQuery {
    page: Option<i64>,
}

async fn list_schedules(
    State(state): State<WebappState>,
    axum::extract::Query(query): axum::extract::Query<ScheduleListQuery>,
) -> impl IntoResponse {
    // Check if schedules table exists
    if !state.database.schedules_table_exists().await {
        return Html(render_schedules_page(&state.templates, &[], 1, 1, 0));
    }

    let per_page = 20i64;

    let total_count = match state.database.count_schedules().await {
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

    match state.database.list_schedules(per_page, offset).await {
        Ok(schedules) => Html(render_schedules_page(
            &state.templates,
            &schedules,
            current_page,
            total_pages,
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

async fn schedule_detail(
    State(state): State<WebappState>,
    Path(schedule_id): Path<Uuid>,
) -> impl IntoResponse {
    let schedule = match state.database.get_schedule(schedule_id).await {
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

    Html(render_schedule_detail_page(&state.templates, &schedule))
}

async fn pause_schedule(
    State(state): State<WebappState>,
    Path(schedule_id): Path<Uuid>,
) -> impl IntoResponse {
    match state
        .database
        .update_schedule_status(schedule_id, "paused")
        .await
    {
        Ok(_) => axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id)),
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
        .update_schedule_status(schedule_id, "active")
        .await
    {
        Ok(_) => axum::response::Redirect::to(&format!("/scheduled/{}", schedule_id)),
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
        .update_schedule_status(schedule_id, "deleted")
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

async fn get_schedule_filter_values(
    State(state): State<WebappState>,
    Path(column): Path<String>,
) -> impl IntoResponse {
    let result = match column.as_str() {
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
// Error Handling
// ============================================================================

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    message: String,
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
struct InvocationsPageContext {
    title: String,
    active_tab: String,
    invocations: Vec<InvocationRow>,
    current_page: i64,
    total_pages: i64,
    has_pagination: bool,
    search_query: Option<String>,
    total_count: i64,
}

#[derive(Serialize)]
struct InvocationRow {
    id: String,
    workflow_name: String,
    created_at: String,
    status: String,
    input_preview: String,
}

fn render_invocations_page(
    templates: &Tera,
    instances: &[super::types::InstanceSummary],
    current_page: i64,
    total_pages: i64,
    search_query: Option<String>,
    total_count: i64,
) -> String {
    let invocations: Vec<InvocationRow> = instances
        .iter()
        .map(|i| InvocationRow {
            id: i.id.to_string(),
            workflow_name: i
                .workflow_name
                .clone()
                .unwrap_or_else(|| "workflow".to_string()),
            created_at: i.created_at.to_rfc3339(),
            status: i.status.to_string(),
            input_preview: i.input_preview.clone(),
        })
        .collect();

    let context = InvocationsPageContext {
        title: "Invocations".to_string(),
        active_tab: "invocations".to_string(),
        invocations,
        current_page,
        total_pages,
        has_pagination: total_pages > 1,
        search_query,
        total_count,
    };

    render_template(templates, "invocations.html", &context)
}

#[derive(Serialize)]
struct InstanceDetailPageContext {
    title: String,
    active_tab: String,
    workflow: WorkflowInfo,
    instance: InstanceInfo,
    graph_data: GraphData,
}

#[derive(Serialize)]
struct WorkflowInfo {
    id: String,
    name: String,
}

#[derive(Serialize)]
struct InstanceInfo {
    id: String,
    created_at: String,
    status: String,
    input_payload: String,
    result_payload: String,
}

#[derive(Serialize)]
struct GraphData {
    nodes: Vec<GraphNode>,
}

#[derive(Serialize)]
struct GraphNode {
    id: String,
    action: String,
    module: String,
    depends_on: Vec<String>,
}

fn render_instance_detail_page(
    templates: &Tera,
    instance: &super::types::InstanceDetail,
    graph: Option<super::types::ExecutionGraphView>,
) -> String {
    let graph_data = if let Some(g) = graph {
        // Build depends_on from edges
        let edges_by_target: std::collections::HashMap<String, Vec<String>> =
            g.edges
                .iter()
                .fold(std::collections::HashMap::new(), |mut acc, e| {
                    acc.entry(e.target.clone())
                        .or_default()
                        .push(e.source.clone());
                    acc
                });

        let nodes: Vec<GraphNode> = g
            .nodes
            .iter()
            .filter(|n| n.action_name.is_some()) // Only show action nodes
            .map(|n| GraphNode {
                id: n.id.clone(),
                action: n.action_name.clone().unwrap_or_default(),
                module: n
                    .module_name
                    .clone()
                    .unwrap_or_else(|| "workflow".to_string()),
                depends_on: edges_by_target.get(&n.id).cloned().unwrap_or_default(),
            })
            .collect();

        GraphData { nodes }
    } else {
        GraphData { nodes: Vec::new() }
    };

    let context = InstanceDetailPageContext {
        title: format!("Instance {}", instance.id),
        active_tab: "invocations".to_string(),
        workflow: WorkflowInfo {
            id: instance.id.to_string(),
            name: instance
                .workflow_name
                .clone()
                .unwrap_or_else(|| "workflow".to_string()),
        },
        instance: InstanceInfo {
            id: instance.id.to_string(),
            created_at: instance.created_at.to_rfc3339(),
            status: instance.status.to_string(),
            input_payload: instance.input_payload.clone(),
            result_payload: instance.result_payload.clone(),
        },
        graph_data,
    };

    render_template(templates, "workflow_run.html", &context)
}

fn render_error_page(templates: &Tera, title: &str, message: &str) -> String {
    let mut ctx = TeraContext::new();
    ctx.insert("title", title);
    ctx.insert("active_tab", "");
    ctx.insert("error_title", title);
    ctx.insert("error_message", message);
    templates.render("error.html", &ctx).unwrap_or_else(|e| {
        error!(?e, "failed to render error template");
        format!("<h1>{}</h1><p>{}</p>", title, message)
    })
}

fn render_template<T: Serialize>(templates: &Tera, name: &str, context: &T) -> String {
    let ctx = TeraContext::from_serialize(context).unwrap_or_default();
    templates.render(name, &ctx).unwrap_or_else(|e| {
        error!(?e, template = name, "failed to render template");
        format!("Template error: {}", e)
    })
}

// ============================================================================
// Schedule Template Rendering
// ============================================================================

#[derive(Serialize)]
struct SchedulesPageContext {
    title: String,
    active_tab: String,
    schedules: Vec<ScheduleRow>,
    current_page: i64,
    total_pages: i64,
    has_pagination: bool,
    total_count: i64,
}

#[derive(Serialize)]
struct ScheduleRow {
    id: String,
    workflow_name: String,
    schedule_name: String,
    schedule_type: String,
    expression: String,
    status: String,
    next_run_at: Option<String>,
    last_run_at: Option<String>,
}

fn render_schedules_page(
    templates: &Tera,
    schedules: &[super::types::ScheduleSummary],
    current_page: i64,
    total_pages: i64,
    total_count: i64,
) -> String {
    let schedule_rows: Vec<ScheduleRow> = schedules
        .iter()
        .map(|s| {
            let expression = if s.schedule_type == "cron" {
                s.cron_expression.clone().unwrap_or_default()
            } else {
                format!("every {} seconds", s.interval_seconds.unwrap_or(0))
            };
            ScheduleRow {
                id: s.id.clone(),
                workflow_name: s.workflow_name.clone(),
                schedule_name: s.schedule_name.clone(),
                schedule_type: s.schedule_type.clone(),
                expression,
                status: s.status.clone(),
                next_run_at: s.next_run_at.clone(),
                last_run_at: s.last_run_at.clone(),
            }
        })
        .collect();

    let context = SchedulesPageContext {
        title: "Scheduled Workflows".to_string(),
        active_tab: "scheduled".to_string(),
        schedules: schedule_rows,
        current_page,
        total_pages,
        has_pagination: total_pages > 1,
        total_count,
    };

    render_template(templates, "scheduled.html", &context)
}

#[derive(Serialize)]
struct ScheduleDetailPageContext {
    title: String,
    active_tab: String,
    schedule: ScheduleDetailView,
}

#[derive(Serialize)]
struct ScheduleDetailView {
    id: String,
    workflow_name: String,
    schedule_name: String,
    schedule_type: String,
    cron_expression: Option<String>,
    interval_seconds: Option<i64>,
    schedule_expression: String,
    jitter_seconds: i64,
    status: String,
    next_run_at: Option<String>,
    last_run_at: Option<String>,
    last_instance_id: Option<String>,
    created_at: String,
    updated_at: String,
    priority: i32,
    allow_duplicate: bool,
    input_payload: Option<String>,
}

fn render_schedule_detail_page(
    templates: &Tera,
    schedule: &super::types::ScheduleDetail,
) -> String {
    let schedule_expression = if schedule.schedule_type == "cron" {
        schedule.cron_expression.clone().unwrap_or_default()
    } else {
        format!("every {} seconds", schedule.interval_seconds.unwrap_or(0))
    };

    let context = ScheduleDetailPageContext {
        title: format!("Schedule: {}", schedule.schedule_name),
        active_tab: "scheduled".to_string(),
        schedule: ScheduleDetailView {
            id: schedule.id.clone(),
            workflow_name: schedule.workflow_name.clone(),
            schedule_name: schedule.schedule_name.clone(),
            schedule_type: schedule.schedule_type.clone(),
            cron_expression: schedule.cron_expression.clone(),
            interval_seconds: schedule.interval_seconds,
            schedule_expression,
            jitter_seconds: schedule.jitter_seconds,
            status: schedule.status.clone(),
            next_run_at: schedule.next_run_at.clone(),
            last_run_at: schedule.last_run_at.clone(),
            last_instance_id: schedule.last_instance_id.clone(),
            created_at: schedule.created_at.clone(),
            updated_at: schedule.updated_at.clone(),
            priority: schedule.priority,
            allow_duplicate: schedule.allow_duplicate,
            input_payload: schedule.input_payload.clone(),
        },
    };

    render_template(templates, "schedule_detail.html", &context)
}

// ============================================================================
// Workers Template Rendering
// ============================================================================

#[derive(Serialize)]
struct WorkersPageContext {
    title: String,
    active_tab: String,
    window_minutes: i64,
    active_worker_count: i64,
    actions_per_sec: String,
    median_instance_duration: String,
    active_instance_count: i64,
    total_in_flight: i64,
    total_queue_depth: i64,
    has_time_series: bool,
    time_series_json: String,
    action_rows: Vec<WorkerActionRowView>,
    instance_rows: Vec<WorkerInstanceRowView>,
}

#[derive(Serialize)]
struct WorkerActionRowView {
    pool_id: String,
    active_workers: i64,
    actions_per_sec: String,
    throughput_per_min: i64,
    total_completed: i64,
    median_dequeue_ms: Option<i64>,
    median_handling_ms: Option<i64>,
    last_action_at: Option<String>,
    updated_at: String,
}

#[derive(Serialize)]
struct WorkerInstanceRowView {
    pool_id: String,
    active_instances: i64,
    instances_per_sec: String,
    instances_per_min: i64,
    total_completed: i64,
    median_duration: String,
    median_dequeue_ms: Option<i64>,
    updated_at: String,
}

fn render_workers_page(
    templates: &Tera,
    statuses: &[super::WorkerStatus],
    window_minutes: i64,
) -> String {
    use crate::pool_status::PoolTimeSeries;

    // Build action rows
    let action_rows: Vec<WorkerActionRowView> = statuses
        .iter()
        .map(|s| WorkerActionRowView {
            pool_id: s.pool_id.to_string(),
            active_workers: s.active_workers as i64,
            actions_per_sec: format!("{:.2}", s.actions_per_sec),
            throughput_per_min: s.throughput_per_min as i64,
            total_completed: s.total_completed,
            median_dequeue_ms: s.median_dequeue_ms,
            median_handling_ms: s.median_handling_ms,
            last_action_at: s.last_action_at.map(|dt| dt.to_rfc3339()),
            updated_at: s.updated_at.to_rfc3339(),
        })
        .collect();

    // Build instance rows
    let instance_rows: Vec<WorkerInstanceRowView> = statuses
        .iter()
        .map(|s| {
            let median_duration = match s.median_instance_duration_secs {
                Some(secs) => format_duration_secs(secs),
                None => "\u{2014}".to_string(),
            };
            WorkerInstanceRowView {
                pool_id: s.pool_id.to_string(),
                active_instances: s.active_instance_count as i64,
                instances_per_sec: format!("{:.2}", s.instances_per_sec),
                instances_per_min: s.instances_per_min as i64,
                total_completed: s.total_instances_completed,
                median_duration,
                median_dequeue_ms: s.median_dequeue_ms,
                updated_at: s.updated_at.to_rfc3339(),
            }
        })
        .collect();

    // Aggregate across pools
    let active_worker_count: i64 = statuses.iter().map(|s| s.active_workers as i64).sum();
    let total_actions_per_sec: f64 = statuses.iter().map(|s| s.actions_per_sec).sum();
    let actions_per_sec = format!("{:.2}", total_actions_per_sec);
    let active_instance_count: i64 = statuses
        .iter()
        .map(|s| s.active_instance_count as i64)
        .sum();
    let total_queue_depth: i64 = statuses.iter().filter_map(|s| s.dispatch_queue_size).sum();
    let total_in_flight: i64 = statuses.iter().filter_map(|s| s.total_in_flight).sum();

    // Average of median instance duration across pools
    let median_instance_duration = {
        let durations: Vec<f64> = statuses
            .iter()
            .filter_map(|s| s.median_instance_duration_secs)
            .collect();
        if durations.is_empty() {
            "\u{2014}".to_string()
        } else {
            let avg = durations.iter().sum::<f64>() / durations.len() as f64;
            format_duration_secs(avg)
        }
    };

    // Decode time-series from all pools and merge into a single JSON array.
    // For a single-pool setup this is just the one blob; for multi-pool we
    // pick the pool with the most data points (typically there's only one).
    let mut best_ts: Option<PoolTimeSeries> = None;
    for status in statuses {
        if let Some(ref bytes) = status.time_series
            && let Some(ts) = PoolTimeSeries::decode(bytes)
        {
            let is_better = best_ts.as_ref().is_none_or(|b| ts.len() > b.len());
            if is_better {
                best_ts = Some(ts);
            }
        }
    }

    let (time_series_json, has_time_series) = match best_ts {
        Some(ts) if !ts.is_empty() => {
            let json = serde_json::to_string(&ts.to_json_entries()).unwrap_or_default();
            (json, true)
        }
        _ => ("[]".to_string(), false),
    };

    let context = WorkersPageContext {
        title: "Workers".to_string(),
        active_tab: "workers".to_string(),
        window_minutes,
        active_worker_count,
        actions_per_sec,
        median_instance_duration,
        active_instance_count,
        total_in_flight,
        total_queue_depth,
        has_time_series,
        time_series_json,
        action_rows,
        instance_rows,
    };

    render_template(templates, "workers.html", &context)
}

/// Format duration in seconds as a human-readable string.
fn format_duration_secs(secs: f64) -> String {
    if secs < 0.001 {
        format!("{:.0}µs", secs * 1_000_000.0)
    } else if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.1}s", secs)
    } else if secs < 3600.0 {
        format!("{:.1}m", secs / 60.0)
    } else {
        format!("{:.1}h", secs / 3600.0)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use sqlx::postgres::PgPoolOptions;
    use tower::util::ServiceExt;
    use uuid::Uuid;

    use super::{WebappState, build_router, init_templates};
    use crate::backends::{
        MemoryBackend, PostgresBackend, WebappBackend, WorkerStatusBackend, WorkerStatusUpdate,
    };

    async fn call_route(backend: Arc<dyn WebappBackend>, uri: &str) -> (StatusCode, String) {
        let templates = Arc::new(init_templates().expect("templates initialize"));
        let app = build_router(WebappState {
            database: backend,
            templates,
        });

        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("route request"),
            )
            .await
            .expect("route response");

        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("route body")
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("route body utf8");
        (status, body)
    }

    #[tokio::test]
    async fn high_level_pages_resolve_with_memory_backend() {
        let backend = MemoryBackend::new();
        backend
            .upsert_worker_status(&WorkerStatusUpdate {
                pool_id: Uuid::new_v4(),
                throughput_per_min: 120.0,
                total_completed: 42,
                last_action_at: None,
                median_dequeue_ms: Some(5),
                median_handling_ms: Some(18),
                dispatch_queue_size: 3,
                total_in_flight: 1,
                active_workers: 2,
                actions_per_sec: 2.0,
                median_instance_duration_secs: Some(0.25),
                active_instance_count: 1,
                total_instances_completed: 7,
                instances_per_sec: 0.2,
                instances_per_min: 12.0,
                time_series: None,
            })
            .await
            .expect("worker status upsert");

        let backend: Arc<dyn WebappBackend> = Arc::new(backend);
        let routes: Vec<(String, &str)> = vec![
            ("/".to_string(), "Invocations"),
            ("/invocations".to_string(), "Invocations"),
            ("/workers".to_string(), "Workers"),
            ("/scheduled".to_string(), "Scheduled Workflows"),
            (
                format!("/instance/{}", Uuid::new_v4()),
                "Instance not found",
            ),
            (
                format!("/scheduled/{}", Uuid::new_v4()),
                "Schedule not found",
            ),
        ];

        for (route, expected) in routes {
            let (status, body) = call_route(backend.clone(), &route).await;
            assert_eq!(status, StatusCode::OK, "route={route}");
            assert!(!body.trim().is_empty(), "route={route}");
            assert!(
                body.contains(expected),
                "route={route}, expected={expected}"
            );
        }

        let (status, body) = call_route(backend, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"service\":\"rappel-webapp\""));
    }

    #[tokio::test]
    async fn high_level_pages_resolve_with_postgres_backend_when_db_is_unavailable() {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgres://rappel:rappel@127.0.0.1:1/rappel")
            .expect("lazy postgres pool");
        let backend: Arc<dyn WebappBackend> = Arc::new(PostgresBackend::new(pool));
        let routes: Vec<(String, &str)> = vec![
            ("/".to_string(), "Unable to load invocations"),
            ("/invocations".to_string(), "Unable to load invocations"),
            ("/workers".to_string(), "Workers"),
            ("/scheduled".to_string(), "Scheduled Workflows"),
            (
                format!("/instance/{}", Uuid::new_v4()),
                "Instance not found",
            ),
            (
                format!("/scheduled/{}", Uuid::new_v4()),
                "Schedule not found",
            ),
        ];

        for (route, expected) in routes {
            let (status, body) = call_route(backend.clone(), &route).await;
            assert_eq!(status, StatusCode::OK, "route={route}");
            assert!(!body.trim().is_empty(), "route={route}");
            assert!(
                body.contains(expected),
                "route={route}, expected={expected}"
            );
        }

        let (status, body) = call_route(backend, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"status\":\"ok\""));
        assert!(body.contains("\"service\":\"rappel-webapp\""));
    }
}
