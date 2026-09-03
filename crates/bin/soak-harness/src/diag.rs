use std::collections::VecDeque;
use std::fs::{self};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use color_eyre::eyre::WrapErr as _;
use serde::Serialize;
use sqlx::PgPool;
use waymark_ids::WorkflowVersionId;

use crate::data;
use crate::flow::HealthSample;

#[derive(Debug, Clone, Serialize)]
struct DiagnosticBundle {
    reason: crate::flow::TerminationReason,
    generated_at: DateTime<Utc>,
    workflow_name: String,
    workflow_version_id: WorkflowVersionId,
    workload_snapshot: data::WorkloadSnapshot,
    node_sample: Option<data::NodeSampleReport>,
    pinning_owners: QueryCapture<data::PinningOwnerRow>,
    expired_pinnings: QueryCapture<data::ExpiredPinningRow>,
    action_call_request_lock_owners: QueryCapture<data::ActionCallRequestLockOwnerRow>,
    stale_action_call_request_locks: QueryCapture<data::StaleActionCallRequestRow>,
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

fn capture_query<T: Serialize>(
    result: Result<Vec<T>, color_eyre::eyre::Report>,
) -> QueryCapture<T> {
    match result {
        Ok(rows) => QueryCapture { rows, error: None },
        Err(err) => QueryCapture {
            rows: Vec::new(),
            error: Some(err.to_string()),
        },
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the diagnostics capture aggregates every source the harness has"
)]
pub async fn capture_diagnostics(
    args: &crate::cli::SoakArgs,
    pool: &PgPool,
    store: &waymark_observability_store_postgres::Store,
    workflow: &crate::setup_workflows::RegisteredWorkflow,
    reason: &crate::flow::TerminationReason,
    samples: &VecDeque<HealthSample>,
    worker_log_path: Option<&Path>,
    run_dir: &Path,
) -> Result<PathBuf, color_eyre::eyre::Report> {
    let workload_snapshot = data::fetch_workload_snapshot(pool).await?;
    let node_sample = data::fetch_latest_node_sample(store).await?;

    let pinning_owners = capture_query(data::fetch_pinning_owners(pool, args.pg_stat_limit).await);
    let expired_pinnings =
        capture_query(data::fetch_expired_pinnings(pool, args.pg_stat_limit).await);
    let action_call_request_lock_owners =
        capture_query(data::fetch_action_call_request_lock_owners(pool, args.pg_stat_limit).await);
    let stale_action_call_request_locks =
        capture_query(data::fetch_stale_action_call_request_locks(pool, args.pg_stat_limit).await);
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
        workload_snapshot,
        node_sample,
        pinning_owners,
        expired_pinnings,
        action_call_request_lock_owners,
        stale_action_call_request_locks,
        pg_stat_activity,
        pg_stat_statements,
        worker_log_tail,
        recent_samples,
        config: args.clone(),
    };

    let path = run_dir.join("diagnostics.json");
    let json = serde_json::to_vec_pretty(&bundle).wrap_err("serialize diagnostics")?;
    fs::write(&path, json).wrap_err_with(|| format!("write {}", path.display()))?;

    Ok(path)
}
