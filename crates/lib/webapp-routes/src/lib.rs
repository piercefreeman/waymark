//! Web application server for the Waymark workers dashboard.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
};
use serde::Serialize;
use tera::{Context as TeraContext, Tera};
use tracing::error;
use waymark_webapp_core::{HealthResponse, WorkerStatus};

// Embed templates at compile time
const TEMPLATE_BASE: &str = include_str!("../templates/base.html");
const TEMPLATE_MACROS: &str = include_str!("../templates/macros.html");
const TEMPLATE_WORKERS: &str = include_str!("../templates/workers.html");

/// Initialize Tera templates from embedded strings.
pub fn init_templates() -> Result<Tera, tera::Error> {
    let mut tera = Tera::default();

    tera.add_raw_template("base.html", TEMPLATE_BASE)?;
    tera.add_raw_template("macros.html", TEMPLATE_MACROS)?;
    tera.add_raw_template("workers.html", TEMPLATE_WORKERS)?;

    tera.autoescape_on(vec![".html", ".tera"]);
    Ok(tera)
}

// ============================================================================
// Internal Server State
// ============================================================================

pub struct WebappState<WebappBackend: ?Sized> {
    pub database: Arc<WebappBackend>,
    pub templates: Arc<Tera>,
}

impl<WebappBackend: ?Sized> Clone for WebappState<WebappBackend> {
    fn clone(&self) -> Self {
        Self {
            database: Arc::clone(&self.database),
            templates: Arc::clone(&self.templates),
        }
    }
}

pub fn build_router<WebappBackend>(state: WebappState<WebappBackend>) -> Router
where
    WebappBackend: ?Sized,
    WebappBackend: waymark_webapp_backend::WebappBackend,
    WebappBackend: Send + Sync + 'static,
{
    Router::new()
        .route("/", get(list_workers))
        .route("/workers", get(list_workers))
        .route("/healthz", get(healthz))
        .with_state(state)
}

// ============================================================================
// Handlers
// ============================================================================

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "waymark-webapp",
    })
}

#[derive(Debug, serde::Deserialize)]
struct WorkersQuery {
    minutes: Option<i64>,
}

async fn list_workers<WebappBackend>(
    State(state): State<WebappState<WebappBackend>>,
    axum::extract::Query(query): axum::extract::Query<WorkersQuery>,
) -> impl IntoResponse
where
    WebappBackend: ?Sized,
    WebappBackend: waymark_webapp_backend::WebappBackend,
{
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
// Template Rendering
// ============================================================================

fn render_template<T: Serialize>(templates: &Tera, name: &str, context: &T) -> String {
    let ctx = TeraContext::from_serialize(context).unwrap_or_default();
    templates.render(name, &ctx).unwrap_or_else(|e| {
        error!(?e, template = name, "failed to render template");
        format!("Template error: {}", e)
    })
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

fn render_workers_page(templates: &Tera, statuses: &[WorkerStatus], window_minutes: i64) -> String {
    use waymark_pool_status::PoolTimeSeries;

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
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;
    use tower::util::ServiceExt;
    use uuid::Uuid;
    use waymark_backend_memory::MemoryBackend;
    use waymark_backend_postgres::PostgresBackend;
    use waymark_worker_status_backend::{WorkerStatusBackend as _, WorkerStatusUpdate};

    use super::{WebappState, build_router, init_templates};

    use waymark_support_test::postgres_setup;

    async fn call_route<WebappBackend>(
        backend: Arc<WebappBackend>,
        uri: &str,
    ) -> (StatusCode, String)
    where
        WebappBackend: ?Sized,
        WebappBackend: waymark_webapp_backend::WebappBackend,
        WebappBackend: Send + Sync + 'static,
    {
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

        let backend = Arc::new(backend);
        let routes: Vec<(String, &str)> = vec![
            ("/".to_string(), "Workers"),
            ("/workers".to_string(), "Workers"),
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
        assert!(body.contains("\"service\":\"waymark-webapp\""));
    }

    #[tokio::test]
    async fn high_level_pages_resolve_with_postgres_backend_when_db_is_unavailable() {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgres://waymark:waymark@127.0.0.1:1/waymark")
            .expect("lazy postgres pool");
        let backend = Arc::new(PostgresBackend::new(pool));
        let routes: Vec<(String, &str)> = vec![
            ("/".to_string(), "Workers"),
            ("/workers".to_string(), "Workers"),
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
        assert!(body.contains("\"service\":\"waymark-webapp\""));
    }

    #[tokio::test]
    #[serial(postgres)]
    async fn high_level_pages_resolve_with_postgres_backend_live_db() {
        let pool = postgres_setup().await;

        let backend = Arc::new(PostgresBackend::new(pool.clone()));
        let pool_id = Uuid::new_v4();
        backend
            .upsert_worker_status(&WorkerStatusUpdate {
                pool_id,
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
            .expect("insert worker status");

        let (status, workers_body) = call_route(backend.clone(), "/workers").await;
        assert_eq!(status, StatusCode::OK);
        assert!(!workers_body.trim().is_empty());
        assert!(workers_body.contains("Workers"));
        assert!(workers_body.contains(&pool_id.to_string()));

        let (status, health_body) = call_route(backend, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        assert!(health_body.contains("\"status\":\"ok\""));
        assert!(health_body.contains("\"service\":\"waymark-webapp\""));

        sqlx::query("DELETE FROM worker_status WHERE pool_id = $1")
            .bind(pool_id)
            .execute(&pool)
            .await
            .expect("cleanup worker status row");
    }
}
