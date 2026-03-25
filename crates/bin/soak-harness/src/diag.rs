use std::collections::VecDeque;
use std::fs::{self};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::data;
use crate::flow::HealthSample;

#[derive(Debug, Clone, Serialize)]
struct DiagnosticBundle {
    reason: crate::flow::TerminationReason,
    generated_at: DateTime<Utc>,
    workflow_name: String,
    workflow_version_id: Uuid,
    queue_snapshot: data::QueueSnapshot,
    worker_status: Option<data::WorkerStatusSnapshot>,
    lock_owners: QueryCapture<data::LockOwnerRow>,
    stale_locks: QueryCapture<data::StaleLockRow>,
    pg_stat_activity: QueryCapture<data::ActivityRow>,
    pg_stat_statements: QueryCapture<data::PgStatStatementRow>,
    worker_log_tail: QueryCapture<String>,
    recent_samples: Vec<HealthSample>,
    config: crate::cli::SoakArgs,
}

#[derive(Debug, Clone, Serialize)]
struct QueryCapture<T: Serialize> {
    pub rows: Vec<T>,
    pub error: Option<String>,
}

fn capture_query<T: Serialize>(result: Result<Vec<T>>) -> QueryCapture<T> {
    match result {
        Ok(rows) => QueryCapture { rows, error: None },
        Err(err) => QueryCapture {
            rows: Vec::new(),
            error: Some(err.to_string()),
        },
    }
}

pub async fn capture_diagnostics(
    args: &crate::cli::SoakArgs,
    pool: &PgPool,
    workflow: &crate::setup_workflows::RegisteredWorkflow,
    reason: &crate::flow::TerminationReason,
    samples: &VecDeque<HealthSample>,
    worker_log_path: Option<&Path>,
    run_dir: &Path,
) -> Result<PathBuf> {
    let queue_snapshot = data::fetch_queue_snapshot(pool).await?;
    let worker_status = data::fetch_latest_worker_status(pool).await?;

    let lock_owners = capture_query(data::fetch_lock_owners(pool, args.pg_stat_limit).await);
    let stale_locks = capture_query(data::fetch_stale_locks(pool, args.pg_stat_limit).await);
    let pg_stat_activity =
        capture_query(data::fetch_pg_stat_activity(pool, args.pg_stat_limit).await);
    let pg_stat_statements =
        capture_query(data::fetch_pg_stat_statements(pool, args.pg_stat_limit).await);

    let worker_log_tail = match worker_log_path {
        Some(path) => capture_query(crate::common::read_tail_lines(
            path,
            args.max_diagnostic_tail_lines,
        )),
        None => QueryCapture {
            rows: Vec::new(),
            error: Some("worker log unavailable (worker launch skipped)".to_string()),
        },
    };

    let max_sample_dump = 2_000usize;
    let sample_count = samples.len();
    let start_idx = sample_count.saturating_sub(max_sample_dump);
    let recent_samples: Vec<HealthSample> = samples.iter().skip(start_idx).cloned().collect();

    let bundle = DiagnosticBundle {
        reason: reason.clone(),
        generated_at: Utc::now(),
        workflow_name: workflow.workflow_name.clone(),
        workflow_version_id: workflow.workflow_version_id,
        queue_snapshot,
        worker_status,
        lock_owners,
        stale_locks,
        pg_stat_activity,
        pg_stat_statements,
        worker_log_tail,
        recent_samples,
        config: args.clone(),
    };

    let path = run_dir.join("diagnostics.json");
    let json = serde_json::to_vec_pretty(&bundle).context("serialize diagnostics")?;
    fs::write(&path, json).with_context(|| format!("write {}", path.display()))?;

    Ok(path)
}
