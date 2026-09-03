use chrono::{DateTime, Utc};
use color_eyre::eyre::{WrapErr as _, bail, eyre};
use serde::Serialize;
use sqlx::PgPool;
use sqlx::prelude::*;
use uuid::Uuid;

/// Snapshot of the runnable-workloads backlog and cumulative completions.
///
/// `ready` counts rows eligible for pinning (unpinned, or pinning expired);
/// `pinned_expired` rows are therefore also counted as `ready`.
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct WorkloadSnapshot {
    pub total: i64,
    pub ready: i64,
    pub pinned_live: i64,
    pub pinned_expired: i64,
    pub workflows_completed: i64,
    pub oldest_ready_updated_at: Option<DateTime<Utc>>,
}

pub async fn fetch_workload_snapshot(
    pool: &PgPool,
) -> Result<WorkloadSnapshot, color_eyre::eyre::Report> {
    let row = sqlx::query_as::<_, WorkloadSnapshot>(
        r#"
        SELECT
            COUNT(*)::bigint AS total,
            COUNT(*) FILTER (WHERE node_id IS NULL OR expires_at <= NOW())::bigint AS ready,
            COUNT(*) FILTER (WHERE node_id IS NOT NULL AND expires_at > NOW())::bigint AS pinned_live,
            COUNT(*) FILTER (WHERE node_id IS NOT NULL AND expires_at <= NOW())::bigint AS pinned_expired,
            (SELECT COUNT(*)::bigint FROM vm_execution_results) AS workflows_completed,
            MIN(updated_at) FILTER (WHERE node_id IS NULL OR expires_at <= NOW()) AS oldest_ready_updated_at
        FROM runnable_workloads
        "#,
    )
    .fetch_one(pool)
    .await
    .wrap_err("fetch workload snapshot")?;

    Ok(row)
}

/// The latest essential-metrics node sample, mirrored into a
/// serializable shape for health samples and diagnostics.
#[derive(Debug, Clone, Serialize)]
pub struct NodeSampleReport {
    pub node_id: waymark_ids::NodeId,
    pub sampled_at: DateTime<Utc>,
    pub worker_pool_size: u64,
    pub max_in_flight_actions: u64,
    pub in_flight_actions: u64,
    pub queued_action_dispatches: u64,
    pub driven_vm_runtimes: u64,
    pub actions_completed_total: u64,
    pub last_action_completed_at: Option<DateTime<Utc>>,
    pub action_dequeue_seconds_p50: Option<f64>,
    pub action_handling_seconds_p50: Option<f64>,
    pub action_dequeue_seconds_sum: f64,
    pub action_handling_seconds_sum: f64,
    pub essential_metrics_dropped_total: u64,
}

fn node_sample_report(
    sample: waymark_essential_metrics_core::NodeSample<waymark_ids::NodeId>,
) -> NodeSampleReport {
    NodeSampleReport {
        node_id: sample.node_id,
        sampled_at: sample.sampled_at,
        worker_pool_size: sample.worker_pool_size,
        max_in_flight_actions: sample.max_in_flight_actions,
        in_flight_actions: sample.in_flight_actions,
        queued_action_dispatches: sample.queued_action_dispatches,
        driven_vm_runtimes: sample.driven_vm_runtimes,
        actions_completed_total: sample.actions_completed_total,
        last_action_completed_at: sample.last_action_completed_at,
        action_dequeue_seconds_p50: sample.action_dequeue_seconds.quantile(
            &waymark_essential_metrics_core::ACTION_DEQUEUE_SECONDS_BOUNDS,
            0.5,
        ),
        action_handling_seconds_p50: sample.action_handling_seconds.quantile(
            &waymark_essential_metrics_core::ACTION_HANDLING_SECONDS_BOUNDS,
            0.5,
        ),
        action_dequeue_seconds_sum: sample.action_dequeue_seconds.sum,
        action_handling_seconds_sum: sample.action_handling_seconds.sum,
        essential_metrics_dropped_total: sample.essential_metrics_dropped_total,
    }
}

/// The newest sample across all nodes; `None` while no node has ever
/// sampled — including before the worker's first boot has provisioned
/// the observability schema at all.
pub async fn fetch_latest_node_sample(
    store: &waymark_observability_store_postgres::Store,
) -> Result<Option<NodeSampleReport>, color_eyre::eyre::Report> {
    use waymark_essential_metrics_query_backend::Latest as _;

    let samples = match store.latest().await {
        Ok(samples) => samples,
        // The worker's own bringup provisions the observability schema;
        // before its first boot the table simply does not exist yet.
        Err(error) if is_undefined_table(&error) => return Ok(None),
        Err(error) => return Err(error).wrap_err("fetch latest node samples"),
    };

    let latest = samples
        .into_iter()
        .max_by_key(|sample| sample.sampled_at)
        .map(node_sample_report);
    Ok(latest)
}

fn is_undefined_table(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db_error) = error else {
        return false;
    };
    db_error.code().as_deref() == Some("42P01")
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct PinningOwnerRow {
    pub node_id: Option<Uuid>,
    pub rows: i64,
    pub oldest_updated_at: Option<DateTime<Utc>>,
    pub newest_updated_at: Option<DateTime<Utc>>,
}

pub async fn fetch_pinning_owners(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<PinningOwnerRow>, color_eyre::eyre::Report> {
    let rows = sqlx::query_as::<_, PinningOwnerRow>(
        r#"
        SELECT
            node_id,
            COUNT(*)::bigint AS rows,
            MIN(updated_at) AS oldest_updated_at,
            MAX(updated_at) AS newest_updated_at
        FROM runnable_workloads
        WHERE node_id IS NOT NULL
        GROUP BY node_id
        ORDER BY rows DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("fetch pinning owners")?;

    Ok(rows)
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct ExpiredPinningRow {
    pub workload_id: Uuid,
    pub node_id: Option<Uuid>,
    pub expires_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

pub async fn fetch_expired_pinnings(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ExpiredPinningRow>, color_eyre::eyre::Report> {
    let rows = sqlx::query_as::<_, ExpiredPinningRow>(
        r#"
        SELECT
            workload_id,
            node_id,
            expires_at,
            updated_at
        FROM runnable_workloads
        WHERE node_id IS NOT NULL
        ORDER BY expires_at ASC NULLS FIRST
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("fetch expired pinnings")?;

    Ok(rows)
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct ActionCallRequestLockOwnerRow {
    pub locked_by: Option<Uuid>,
    pub rows: i64,
    pub oldest_created_at: Option<DateTime<Utc>>,
    pub newest_created_at: Option<DateTime<Utc>>,
}

pub async fn fetch_action_call_request_lock_owners(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ActionCallRequestLockOwnerRow>, color_eyre::eyre::Report> {
    let rows = sqlx::query_as::<_, ActionCallRequestLockOwnerRow>(
        r#"
        SELECT
            locked_by,
            COUNT(*)::bigint AS rows,
            MIN(created_at) AS oldest_created_at,
            MAX(created_at) AS newest_created_at
        FROM action_call_requests
        GROUP BY locked_by
        ORDER BY rows DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("fetch action-call request lock owners")?;

    Ok(rows)
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct StaleActionCallRequestRow {
    pub vm_id: Uuid,
    pub promise_state_id: i64,
    pub locked_by: Option<Uuid>,
    pub lock_expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

pub async fn fetch_stale_action_call_request_locks(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<StaleActionCallRequestRow>, color_eyre::eyre::Report> {
    let rows = sqlx::query_as::<_, StaleActionCallRequestRow>(
        r#"
        SELECT
            vm_id,
            promise_state_id,
            locked_by,
            lock_expires_at,
            created_at
        FROM action_call_requests
        ORDER BY lock_expires_at ASC NULLS FIRST
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("fetch stale action-call request locks")?;

    Ok(rows)
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct ActivityRow {
    pub pid: i32,
    pub state: String,
    pub wait_event_type: String,
    pub wait_event: String,
    pub age: String,
    pub query: String,
}

pub async fn fetch_pg_stat_activity(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ActivityRow>, color_eyre::eyre::Report> {
    let rows = sqlx::query_as::<_, ActivityRow>(
        r#"
        SELECT
            pid,
            COALESCE(state, '') AS state,
            COALESCE(wait_event_type, '') AS wait_event_type,
            COALESCE(wait_event, '') AS wait_event,
            (NOW() - query_start)::text AS age,
            LEFT(COALESCE(query, ''), 500) AS query
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND state <> 'idle'
          AND pid <> pg_backend_pid()
        ORDER BY query_start ASC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("fetch pg_stat_activity")?;

    Ok(rows)
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct PgStatStatementRow {
    pub calls: i64,
    pub total_ms: f64,
    pub mean_ms: f64,
    pub max_ms: f64,
    pub rows: i64,
    pub query: String,
}

pub async fn fetch_pg_stat_statements(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<PgStatStatementRow>, color_eyre::eyre::Report> {
    let extension_enabled: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')",
    )
    .fetch_one(pool)
    .await
    .wrap_err("check pg_stat_statements extension")?;

    if !extension_enabled {
        bail!("pg_stat_statements extension is not enabled");
    }

    let new_columns_query = r#"
        SELECT
            calls::bigint AS calls,
            total_exec_time AS total_ms,
            mean_exec_time AS mean_ms,
            max_exec_time AS max_ms,
            rows::bigint AS rows,
            LEFT(query, 500) AS query
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT $1
    "#;

    match sqlx::query_as::<_, PgStatStatementRow>(new_columns_query)
        .bind(limit)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => Ok(rows),
        Err(primary_err) => {
            let old_columns_query = r#"
                SELECT
                    calls::bigint AS calls,
                    total_time AS total_ms,
                    mean_time AS mean_ms,
                    max_time AS max_ms,
                    rows::bigint AS rows,
                    LEFT(query, 500) AS query
                FROM pg_stat_statements
                ORDER BY total_time DESC
                LIMIT $1
            "#;

            sqlx::query_as::<_, PgStatStatementRow>(old_columns_query)
                .bind(limit)
                .fetch_all(pool)
                .await
                .map_err(|secondary_err| {
                    eyre!(
                        "failed querying pg_stat_statements (new columns: {}; old columns: {})",
                        primary_err,
                        secondary_err
                    )
                })
        }
    }
}
