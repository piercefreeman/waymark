use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
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

pub async fn fetch_workload_snapshot(pool: &PgPool) -> Result<WorkloadSnapshot> {
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
    .context("fetch workload snapshot")?;

    Ok(row)
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct WorkerStatusSnapshot {
    pub pool_id: Uuid,
    pub actions_per_sec: f64,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub active_workers: i32,
    pub total_in_flight: i64,
    pub dispatch_queue_size: i64,
    pub median_dequeue_ms: Option<i64>,
    pub median_handling_ms: Option<i64>,
    pub last_action_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub active_instance_count: i32,
}

pub async fn fetch_latest_worker_status(pool: &PgPool) -> Result<Option<WorkerStatusSnapshot>> {
    let row = sqlx::query_as::<_, WorkerStatusSnapshot>(
        r#"
        SELECT
            pool_id,
            actions_per_sec,
            throughput_per_min,
            total_completed,
            active_workers,
            total_in_flight,
            dispatch_queue_size,
            median_dequeue_ms,
            median_handling_ms,
            last_action_at,
            updated_at,
            active_instance_count
        FROM worker_status
        ORDER BY updated_at DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .context("fetch latest worker status")?;

    Ok(row)
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct PinningOwnerRow {
    pub node_id: Option<Uuid>,
    pub rows: i64,
    pub oldest_updated_at: Option<DateTime<Utc>>,
    pub newest_updated_at: Option<DateTime<Utc>>,
}

pub async fn fetch_pinning_owners(pool: &PgPool, limit: i64) -> Result<Vec<PinningOwnerRow>> {
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
    .context("fetch pinning owners")?;

    Ok(rows)
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct ExpiredPinningRow {
    pub workload_id: Uuid,
    pub node_id: Option<Uuid>,
    pub expires_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

pub async fn fetch_expired_pinnings(pool: &PgPool, limit: i64) -> Result<Vec<ExpiredPinningRow>> {
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
    .context("fetch expired pinnings")?;

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
) -> Result<Vec<ActionCallRequestLockOwnerRow>> {
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
    .context("fetch action-call request lock owners")?;

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
) -> Result<Vec<StaleActionCallRequestRow>> {
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
    .context("fetch stale action-call request locks")?;

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

pub async fn fetch_pg_stat_activity(pool: &PgPool, limit: i64) -> Result<Vec<ActivityRow>> {
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
    .context("fetch pg_stat_activity")?;

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
) -> Result<Vec<PgStatStatementRow>> {
    let extension_enabled: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')",
    )
    .fetch_one(pool)
    .await
    .context("check pg_stat_statements extension")?;

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
                    anyhow!(
                        "failed querying pg_stat_statements (new columns: {}; old columns: {})",
                        primary_err,
                        secondary_err
                    )
                })
        }
    }
}
