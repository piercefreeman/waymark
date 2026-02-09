//! Long-running soak harness for local, prod-like runtime stress testing.
//!
//! The harness can:
//! - Boot local Postgres via docker compose
//! - Start the standard `start-workers` runtime as a child process
//! - Continuously queue synthetic workloads with configurable timeout/failure mix
//! - Detect sustained stall conditions (near-zero actions/sec with large ready queue)
//! - Capture diagnostics (DB snapshots + worker log tail) on exit/issue

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::Parser;
use prost::Message;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};
use tokio::process::{Child, Command};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;
use waymark::backends::{
    PostgresBackend, QueuedInstance, WorkflowRegistration, WorkflowRegistryBackend,
};
use waymark::db;
use waymark::messages::ast as ir;
use waymark::waymark_core::dag::{DAG, convert_to_dag};
use waymark::waymark_core::ir_parser::parse_program;
use waymark::waymark_core::runner::RunnerState;

const DEFAULT_DSN: &str = "postgresql://waymark:waymark@127.0.0.1:5433/waymark";
const DEFAULT_WORKFLOW_NAME: &str = "waymark_soak_timeout_mix_v1";
const DB_READY_TIMEOUT: Duration = Duration::from_secs(90);
const DB_RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_SAMPLE_HISTORY: usize = 20_000;

#[derive(Parser, Debug, Clone, Serialize)]
#[command(
    name = "soak-harness",
    about = "Run a long-lived, timeout-capable Waymark soak workload with diagnostics"
)]
struct SoakArgs {
    #[arg(long, env = "WAYMARK_DATABASE_URL", default_value = DEFAULT_DSN)]
    dsn: String,
    #[arg(long, default_value_t = false)]
    skip_postgres_boot: bool,
    #[arg(long, default_value_t = false)]
    skip_worker_launch: bool,
    #[arg(long, default_value_t = false)]
    keep_existing_data: bool,
    #[arg(long, default_value = "tests.fixtures_actions.soak_actions")]
    user_module: String,
    #[arg(long, default_value_t = 8)]
    worker_count: usize,
    #[arg(long, default_value_t = 500)]
    concurrent_per_worker: usize,
    #[arg(long, default_value_t = 5000)]
    max_concurrent_instances: usize,
    #[arg(long, default_value_t = 100)]
    poll_interval_ms: u64,
    #[arg(long, default_value_t = 500)]
    persist_interval_ms: u64,
    #[arg(long, default_value_t = 5000)]
    profile_interval_ms: u64,
    #[arg(long, default_value_t = false)]
    disable_webapp: bool,
    #[arg(long, default_value = "0.0.0.0:24119")]
    webapp_addr: String,
    #[arg(long, default_value_t = 5)]
    startup_log_interval_secs: u64,
    #[arg(long, default_value_t = 20)]
    timeout_seconds: u32,
    #[arg(long, default_value_t = 10_000)]
    queue_rate_per_minute: usize,
    #[arg(long, default_value_t = 5)]
    actions_per_workflow: usize,
    #[arg(long, default_value_t = 500)]
    queue_batch_size: usize,
    #[arg(long, default_value_t = 50_000)]
    target_ready_queue: i64,
    #[arg(long, default_value_t = 10_000)]
    max_top_up_per_tick: usize,
    #[arg(long, default_value_t = 10_000)]
    max_queue_per_tick: usize,
    #[arg(long, default_value_t = 10)]
    tick_seconds: u64,
    #[arg(long)]
    duration_hours: Option<f64>,
    #[arg(long, default_value_t = 10.0)]
    timeout_percent: f64,
    #[arg(long, default_value_t = 3.0)]
    failure_percent: f64,
    #[arg(long, default_value_t = 25.0)]
    slow_percent: f64,
    #[arg(long, default_value_t = 4096)]
    payload_bytes: i64,
    #[arg(long, default_value_t = 1000)]
    issue_min_ready_queue: i64,
    #[arg(long, default_value_t = 0.01)]
    issue_actions_per_sec_threshold: f64,
    #[arg(long, default_value_t = 12)]
    issue_consecutive_samples: usize,
    #[arg(long, default_value_t = 90)]
    issue_last_action_stale_secs: i64,
    #[arg(long, default_value_t = 30)]
    issue_status_stale_secs: i64,
    #[arg(long, default_value_t = 20)]
    pg_stat_limit: i64,
    #[arg(long, default_value_t = 400)]
    max_diagnostic_tail_lines: usize,
    #[arg(long, default_value = "target/soak-runs")]
    diagnostic_dir: PathBuf,
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(Debug)]
struct WorkerProcess {
    child: Child,
    log_path: PathBuf,
}

#[derive(Debug, Clone)]
struct RegisteredWorkflow {
    workflow_name: String,
    workflow_version_id: Uuid,
    dag: Arc<DAG>,
    entry_template_id: String,
}

#[derive(Debug, Clone)]
struct WorkItem {
    step_delays_ms: Vec<i64>,
    step_should_fail: Vec<bool>,
    step_payload_bytes: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct QueueSnapshot {
    total: i64,
    ready: i64,
    future: i64,
    locked_live: i64,
    locked_expired: i64,
    oldest_ready_at: Option<DateTime<Utc>>,
    oldest_scheduled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct WorkerStatusSnapshot {
    pool_id: Uuid,
    actions_per_sec: f64,
    throughput_per_min: f64,
    total_completed: i64,
    active_workers: i32,
    total_in_flight: i64,
    dispatch_queue_size: i64,
    median_dequeue_ms: Option<i64>,
    median_handling_ms: Option<i64>,
    last_action_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    active_instance_count: i32,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct LockOwnerRow {
    lock_uuid: Option<Uuid>,
    rows: i64,
    oldest_scheduled_at: Option<DateTime<Utc>>,
    newest_scheduled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct StaleLockRow {
    instance_id: Uuid,
    lock_uuid: Option<Uuid>,
    lock_expires_at: Option<DateTime<Utc>>,
    scheduled_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct ActivityRow {
    pid: i32,
    state: String,
    wait_event_type: String,
    wait_event: String,
    age: String,
    query: String,
}

#[derive(Debug, Clone, Serialize, FromRow)]
struct PgStatStatementRow {
    calls: i64,
    total_ms: f64,
    mean_ms: f64,
    max_ms: f64,
    rows: i64,
    query: String,
}

#[derive(Debug, Clone, Serialize)]
struct HealthSample {
    at: DateTime<Utc>,
    queued_total: i64,
    queued_ready: i64,
    queued_future: i64,
    queued_locked_live: i64,
    queued_locked_expired: i64,
    queued_this_tick: usize,
    actions_per_sec: Option<f64>,
    throughput_per_min: Option<f64>,
    total_completed: Option<i64>,
    active_workers: Option<i32>,
    total_in_flight: Option<i64>,
    active_instance_count: Option<i32>,
    worker_status_age_secs: Option<i64>,
    last_action_age_secs: Option<i64>,
    zero_streak: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
enum TerminationReason {
    DurationReached,
    Interrupted,
    IssueDetected(String),
    WorkerExited(String),
}

#[derive(Debug, Clone, Serialize)]
struct QueryCapture<T: Serialize> {
    rows: Vec<T>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DiagnosticBundle {
    reason: TerminationReason,
    generated_at: DateTime<Utc>,
    workflow_name: String,
    workflow_version_id: Uuid,
    queue_snapshot: QueueSnapshot,
    worker_status: Option<WorkerStatusSnapshot>,
    lock_owners: QueryCapture<LockOwnerRow>,
    stale_locks: QueryCapture<StaleLockRow>,
    pg_stat_activity: QueryCapture<ActivityRow>,
    pg_stat_statements: QueryCapture<PgStatStatementRow>,
    worker_log_tail: QueryCapture<String>,
    recent_samples: Vec<HealthSample>,
    config: SoakArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "waymark=info,soak_harness=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = SoakArgs::parse();
    validate_args(&args)?;

    let run_id = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let run_dir = args.diagnostic_dir.join(&run_id);
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("create run directory {}", run_dir.display()))?;

    info!(run_dir = %run_dir.display(), "starting soak harness");
    info!(?args, "soak harness config");

    if !args.skip_postgres_boot {
        boot_postgres().await?;
    }

    let pool = wait_for_database(&args.dsn, DB_READY_TIMEOUT).await?;
    db::run_migrations(&pool)
        .await
        .context("run migrations before soak")?;

    let backend = PostgresBackend::new(pool.clone());
    if !args.keep_existing_data {
        info!("clearing runner data before soak run");
        backend.clear_all().await.context("clear runner tables")?;
        sqlx::query("DELETE FROM worker_status")
            .execute(&pool)
            .await
            .context("clear worker_status")?;
    }

    let mut worker = if args.skip_worker_launch {
        None
    } else {
        Some(start_workers(&args, &run_dir).await?)
    };

    if let Some(worker_process) = worker.as_mut()
        && let Err(err) = wait_for_worker_status(
            &pool,
            Duration::from_secs(60),
            Duration::from_secs(args.startup_log_interval_secs.max(1)),
            worker_process,
        )
        .await
    {
        shutdown_worker_if_running(&mut worker).await;
        return Err(err);
    }

    let workflow = match register_workflow(
        &backend,
        args.timeout_seconds,
        args.actions_per_workflow,
        &args.user_module,
    )
    .await
    {
        Ok(workflow) => workflow,
        Err(err) => {
            shutdown_worker_if_running(&mut worker).await;
            return Err(err);
        }
    };
    info!(
        workflow_name = %workflow.workflow_name,
        workflow_version_id = %workflow.workflow_version_id,
        "registered soak workflow"
    );
    let expected_actions_per_minute = args
        .queue_rate_per_minute
        .saturating_mul(args.actions_per_workflow);
    info!(
        queue_rate_per_minute = args.queue_rate_per_minute,
        actions_per_workflow = args.actions_per_workflow,
        expected_actions_per_minute,
        "soak throughput target"
    );

    let run_result = run_soak_loop(&args, &backend, &pool, &workflow, &mut worker).await;
    let (reason, samples) = match run_result {
        Ok(result) => result,
        Err(err) => {
            error!(error = %err, "soak loop failed");
            (
                TerminationReason::IssueDetected(format!("soak loop error: {err}")),
                VecDeque::new(),
            )
        }
    };

    let diagnostics_path = capture_diagnostics(
        &args,
        &pool,
        &workflow,
        &reason,
        &samples,
        worker.as_ref().map(|process| process.log_path.as_path()),
        &run_dir,
    )
    .await?;

    if let Some(worker_process) = worker.as_mut() {
        shutdown_worker(worker_process).await?;
    }

    info!(
        reason = ?reason,
        diagnostics = %diagnostics_path.display(),
        "soak harness finished"
    );

    if is_error_exit(&reason) {
        bail!(
            "soak harness detected an issue; see {}",
            diagnostics_path.display()
        );
    }

    Ok(())
}

fn validate_args(args: &SoakArgs) -> Result<()> {
    if args.queue_batch_size == 0 {
        bail!("--queue-batch-size must be at least 1");
    }
    if args.tick_seconds == 0 {
        bail!("--tick-seconds must be at least 1");
    }
    if args.max_queue_per_tick == 0 {
        bail!("--max-queue-per-tick must be at least 1");
    }
    if args.issue_consecutive_samples == 0 {
        bail!("--issue-consecutive-samples must be at least 1");
    }
    if args.actions_per_workflow == 0 {
        bail!("--actions-per-workflow must be at least 1");
    }
    if args.timeout_percent < 0.0 || args.failure_percent < 0.0 || args.slow_percent < 0.0 {
        bail!("workload percentages cannot be negative");
    }
    let used = args.timeout_percent + args.failure_percent + args.slow_percent;
    if used > 100.0 {
        bail!(
            "timeout/failure/slow percentages sum to {:.2}, must be <= 100",
            used
        );
    }
    Ok(())
}

fn is_error_exit(reason: &TerminationReason) -> bool {
    matches!(
        reason,
        TerminationReason::IssueDetected(_) | TerminationReason::WorkerExited(_)
    )
}

async fn boot_postgres() -> Result<()> {
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let status = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg("docker-compose.yml")
        .arg("up")
        .arg("-d")
        .arg("postgres")
        .current_dir(&project_root)
        .status()
        .await
        .with_context(|| format!("run docker compose from {}", project_root.display()))?;

    if !status.success() {
        bail!("docker compose up -d postgres failed with status {status}");
    }

    Ok(())
}

async fn wait_for_database(dsn: &str, timeout: Duration) -> Result<PgPool> {
    let deadline = Instant::now() + timeout;
    let mut last_error: Option<String> = None;

    while Instant::now() < deadline {
        match PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(5))
            .connect(dsn)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(DB_RETRY_DELAY).await;
            }
        }
    }

    Err(anyhow!(
        "timed out waiting for Postgres at {dsn}; last error: {}",
        last_error.unwrap_or_else(|| "unknown".to_string())
    ))
}

async fn start_workers(args: &SoakArgs, run_dir: &Path) -> Result<WorkerProcess> {
    let webapp_enabled = !args.disable_webapp;
    let log_path = run_dir.join("start-workers.log");
    let log_file = File::create(&log_path)
        .with_context(|| format!("create worker log file {}", log_path.display()))?;
    let log_file_err = log_file
        .try_clone()
        .with_context(|| format!("clone worker log handle {}", log_path.display()))?;

    let mut cmd = start_workers_command();
    cmd.current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    cmd.env("WAYMARK_DATABASE_URL", &args.dsn);
    cmd.env("WAYMARK_USER_MODULE", &args.user_module);
    cmd.env("WAYMARK_WORKER_COUNT", args.worker_count.to_string());
    cmd.env(
        "WAYMARK_CONCURRENT_PER_WORKER",
        args.concurrent_per_worker.to_string(),
    );
    cmd.env(
        "WAYMARK_MAX_CONCURRENT_INSTANCES",
        args.max_concurrent_instances.to_string(),
    );
    cmd.env(
        "WAYMARK_POLL_INTERVAL_MS",
        args.poll_interval_ms.to_string(),
    );
    cmd.env(
        "WAYMARK_PERSIST_INTERVAL_MS",
        args.persist_interval_ms.to_string(),
    );
    cmd.env(
        "WAYMARK_RUNNER_PROFILE_INTERVAL_MS",
        args.profile_interval_ms.to_string(),
    );
    cmd.env("WAYMARK_WEBAPP_ENABLED", webapp_enabled.to_string());
    cmd.env("WAYMARK_WEBAPP_ADDR", &args.webapp_addr);
    if std::env::var_os("RUST_LOG").is_none() {
        cmd.env("RUST_LOG", "waymark=info,start_workers=info");
    }
    cmd.stdout(Stdio::from(log_file));
    cmd.stderr(Stdio::from(log_file_err));

    let child = cmd.spawn().context("spawn start-workers process")?;
    info!(
        log_path = %log_path.display(),
        webapp_enabled,
        webapp_addr = %args.webapp_addr,
        "started worker process"
    );

    Ok(WorkerProcess { child, log_path })
}

fn start_workers_command() -> Command {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let local_debug_bin = repo_root
        .join("target")
        .join("debug")
        .join(if cfg!(windows) {
            "start-workers.exe"
        } else {
            "start-workers"
        });
    if local_debug_bin.is_file() {
        return Command::new(local_debug_bin);
    }

    if let Some(start_workers_bin) = find_executable("start-workers") {
        return Command::new(start_workers_bin);
    }

    let mut command = Command::new("cargo");
    command
        .arg("run")
        .arg("--bin")
        .arg("start-workers")
        .arg("--");
    command
}

fn find_executable(bin: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path_var) {
        let candidate = directory.join(bin);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let exe_candidate = directory.join(format!("{bin}.exe"));
            if exe_candidate.is_file() {
                return Some(exe_candidate);
            }
        }
    }
    None
}

async fn shutdown_worker(worker: &mut WorkerProcess) -> Result<()> {
    if worker
        .child
        .try_wait()
        .context("check worker process status")?
        .is_some()
    {
        return Ok(());
    }

    warn!("stopping worker process");
    worker
        .child
        .start_kill()
        .context("send kill signal to worker process")?;

    let status = tokio::time::timeout(Duration::from_secs(10), worker.child.wait())
        .await
        .context("timed out waiting for worker process shutdown")?
        .context("wait for worker process")?;

    info!(status = %status, "worker process stopped");
    Ok(())
}

async fn shutdown_worker_if_running(worker: &mut Option<WorkerProcess>) {
    if let Some(worker_process) = worker.as_mut()
        && let Err(err) = shutdown_worker(worker_process).await
    {
        warn!(error = %err, "failed to stop worker process during error cleanup");
    }
}

async fn wait_for_worker_status(
    pool: &PgPool,
    timeout: Duration,
    startup_log_interval: Duration,
    worker: &mut WorkerProcess,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let started = Instant::now();
    let mut last_log_at = Instant::now();

    while Instant::now() < deadline {
        if let Some(status) = worker
            .child
            .try_wait()
            .context("check worker status during startup wait")?
        {
            let tail = read_tail_lines(&worker.log_path, 80).unwrap_or_default();
            let tail_text = if tail.is_empty() {
                "worker log unavailable".to_string()
            } else {
                tail.join("\n")
            };
            bail!("worker process exited before first heartbeat: {status}\nlog tail:\n{tail_text}");
        }

        if fetch_latest_worker_status(pool).await?.is_some() {
            info!(
                startup_wait_secs = started.elapsed().as_secs_f64(),
                "worker status heartbeat detected"
            );
            return Ok(());
        }

        if last_log_at.elapsed() >= startup_log_interval {
            info!(
                startup_wait_secs = started.elapsed().as_secs_f64(),
                "waiting for first worker_status heartbeat"
            );
            last_log_at = Instant::now();
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    bail!("timed out waiting for worker_status rows; worker may not have started successfully")
}

async fn register_workflow(
    backend: &PostgresBackend,
    timeout_seconds: u32,
    actions_per_workflow: usize,
    user_module: &str,
) -> Result<RegisteredWorkflow> {
    let source = workflow_source(user_module, timeout_seconds, actions_per_workflow);

    let program = parse_program(source.trim()).map_err(|err| anyhow!(err.to_string()))?;
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).map_err(|err| anyhow!(err.to_string()))?);
    let entry_template_id = dag
        .entry_node
        .clone()
        .ok_or_else(|| anyhow!("compiled workflow has no entry node"))?;

    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: DEFAULT_WORKFLOW_NAME.to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .context("upsert soak workflow version")?;

    Ok(RegisteredWorkflow {
        workflow_name: DEFAULT_WORKFLOW_NAME.to_string(),
        workflow_version_id,
        dag,
        entry_template_id,
    })
}

fn workflow_source(user_module: &str, timeout_seconds: u32, actions_per_workflow: usize) -> String {
    let mut input_names = Vec::with_capacity(actions_per_workflow * 3);
    let mut lines = Vec::with_capacity(actions_per_workflow + 3);
    lines.push("fn main(input: [".to_string());

    for step in 0..actions_per_workflow {
        let idx = step + 1;
        input_names.push(format!("delay_ms_{idx}"));
        input_names.push(format!("should_fail_{idx}"));
        input_names.push(format!("payload_bytes_{idx}"));
    }

    lines[0].push_str(&input_names.join(", "));
    lines[0].push_str("], output: [result]):");

    for step in 0..actions_per_workflow {
        let idx = step + 1;
        lines.push(format!(
            "    step_{idx} = @{user_module}.simulated_action(delay_ms=delay_ms_{idx}, should_fail=should_fail_{idx}, payload_bytes=payload_bytes_{idx})[ActionTimeout -> retry: 1, backoff: 1 s][timeout: {timeout_seconds} s]"
        ));
    }

    lines.push(format!("    result = step_{actions_per_workflow}"));
    lines.push("    return result".to_string());
    lines.join("\n")
}

async fn run_soak_loop(
    args: &SoakArgs,
    backend: &PostgresBackend,
    pool: &PgPool,
    workflow: &RegisteredWorkflow,
    worker: &mut Option<WorkerProcess>,
) -> Result<(TerminationReason, VecDeque<HealthSample>)> {
    let seed = args.seed.unwrap_or_else(rand::random);
    info!(seed, "soak workload random seed");
    let mut rng = StdRng::seed_from_u64(seed);

    let mut samples: VecDeque<HealthSample> = VecDeque::new();
    let mut zero_streak = 0usize;
    let mut queue_tokens = 0.0f64;
    let start = Instant::now();
    let mut previous_total_completed: Option<i64> = None;
    let tick_duration = Duration::from_secs(args.tick_seconds);
    let mut last_tick = Instant::now();

    let mut ticker = tokio::time::interval(tick_duration);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = ticker.tick().await;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                return Ok((TerminationReason::Interrupted, samples));
            }
            _ = ticker.tick() => {}
        }

        if let Some(worker_process) = worker.as_mut()
            && let Some(status) = worker_process
                .child
                .try_wait()
                .context("poll worker process")?
        {
            return Ok((
                TerminationReason::WorkerExited(format!("worker process exited: {status}")),
                samples,
            ));
        }

        let now_tick = Instant::now();
        let elapsed = now_tick.duration_since(last_tick);
        last_tick = now_tick;

        queue_tokens += elapsed.as_secs_f64() * (args.queue_rate_per_minute as f64 / 60.0);

        let queue_snapshot = fetch_queue_snapshot(pool).await?;
        let worker_status = fetch_latest_worker_status(pool).await?;

        let mut requested = queue_tokens.floor() as usize;
        queue_tokens -= requested as f64;

        if queue_snapshot.ready < args.target_ready_queue {
            let deficit = (args.target_ready_queue - queue_snapshot.ready) as usize;
            requested = requested.saturating_add(deficit.min(args.max_top_up_per_tick));
        }

        requested = requested.min(args.max_queue_per_tick);

        let queued_this_tick = if requested > 0 {
            queue_instances(backend, workflow, args, requested, &mut rng).await?
        } else {
            0
        };

        let now = Utc::now();
        let status_age_secs = worker_status
            .as_ref()
            .map(|status| (now - status.updated_at).num_seconds());
        let last_action_age_secs = worker_status.as_ref().and_then(|status| {
            status
                .last_action_at
                .map(|last_action| (now - last_action).num_seconds())
        });

        let stalled = should_count_stall(
            args,
            &queue_snapshot,
            worker_status.as_ref(),
            status_age_secs,
            last_action_age_secs,
        );

        if stalled {
            zero_streak = zero_streak.saturating_add(1);
        } else {
            zero_streak = 0;
        }

        let sample = HealthSample {
            at: now,
            queued_total: queue_snapshot.total,
            queued_ready: queue_snapshot.ready,
            queued_future: queue_snapshot.future,
            queued_locked_live: queue_snapshot.locked_live,
            queued_locked_expired: queue_snapshot.locked_expired,
            queued_this_tick,
            actions_per_sec: worker_status.as_ref().map(|value| value.actions_per_sec),
            throughput_per_min: worker_status.as_ref().map(|value| value.throughput_per_min),
            total_completed: worker_status.as_ref().map(|value| value.total_completed),
            active_workers: worker_status.as_ref().map(|value| value.active_workers),
            total_in_flight: worker_status.as_ref().map(|value| value.total_in_flight),
            active_instance_count: worker_status
                .as_ref()
                .map(|value| value.active_instance_count),
            worker_status_age_secs: status_age_secs,
            last_action_age_secs,
            zero_streak,
        };
        samples.push_back(sample);
        while samples.len() > MAX_SAMPLE_HISTORY {
            let _ = samples.pop_front();
        }

        match worker_status.as_ref() {
            Some(status) => {
                let completed_delta = previous_total_completed
                    .map(|previous| status.total_completed.saturating_sub(previous))
                    .unwrap_or(0);
                previous_total_completed = Some(status.total_completed);
                info!(
                    elapsed_secs = start.elapsed().as_secs_f64(),
                    queued_total = queue_snapshot.total,
                    ready_queue = queue_snapshot.ready,
                    queued_this_tick,
                    actions_per_sec = status.actions_per_sec,
                    total_completed = status.total_completed,
                    completed_delta,
                    in_flight = status.total_in_flight,
                    active_instances = status.active_instance_count,
                    status_age_secs = status_age_secs.unwrap_or(-1),
                    last_action_age_secs = last_action_age_secs.unwrap_or(-1),
                    zero_streak,
                    "soak tick"
                );
            }
            None => {
                warn!(
                    ready_queue = queue_snapshot.ready,
                    queued_this_tick, zero_streak, "soak tick without worker_status row"
                );
            }
        }

        if zero_streak >= args.issue_consecutive_samples {
            let detail = format!(
                "actions/sec <= {:.4} for {} consecutive samples while ready queue={} (threshold={})",
                args.issue_actions_per_sec_threshold,
                zero_streak,
                queue_snapshot.ready,
                args.issue_min_ready_queue
            );
            return Ok((TerminationReason::IssueDetected(detail), samples));
        }

        if let Some(hours) = args.duration_hours
            && start.elapsed() >= Duration::from_secs_f64(hours * 60.0 * 60.0)
        {
            return Ok((TerminationReason::DurationReached, samples));
        }
    }
}

fn should_count_stall(
    args: &SoakArgs,
    queue_snapshot: &QueueSnapshot,
    worker_status: Option<&WorkerStatusSnapshot>,
    status_age_secs: Option<i64>,
    last_action_age_secs: Option<i64>,
) -> bool {
    if queue_snapshot.ready < args.issue_min_ready_queue {
        return false;
    }

    let Some(status) = worker_status else {
        return true;
    };

    let status_age = status_age_secs.unwrap_or(i64::MAX);
    if status_age > args.issue_status_stale_secs {
        return true;
    }

    if status.actions_per_sec > args.issue_actions_per_sec_threshold {
        return false;
    }

    let last_action_age = last_action_age_secs.unwrap_or(i64::MAX);
    status.total_in_flight == 0 || last_action_age > args.issue_last_action_stale_secs
}

async fn queue_instances(
    backend: &PostgresBackend,
    workflow: &RegisteredWorkflow,
    args: &SoakArgs,
    count: usize,
    rng: &mut StdRng,
) -> Result<usize> {
    let mut queued_total = 0usize;
    let batch_size = args.queue_batch_size.max(1);
    let mut remaining = count;

    while remaining > 0 {
        let take = remaining.min(batch_size);
        let mut instances = Vec::with_capacity(take);

        for _ in 0..take {
            let item = sample_work_item(args, rng);
            let instance = build_instance(workflow, item)?;
            instances.push(instance);
        }

        backend
            .queue_instances(&instances)
            .await
            .context("queue soak instances")?;

        queued_total += take;
        remaining -= take;
    }

    Ok(queued_total)
}

fn sample_work_item(args: &SoakArgs, rng: &mut StdRng) -> WorkItem {
    let mut step_delays_ms = Vec::with_capacity(args.actions_per_workflow);
    let mut step_should_fail = Vec::with_capacity(args.actions_per_workflow);
    let mut step_payload_bytes = Vec::with_capacity(args.actions_per_workflow);

    for _ in 0..args.actions_per_workflow {
        let (delay_ms, should_fail) = sample_step_behavior(args, rng);
        step_delays_ms.push(delay_ms);
        step_should_fail.push(should_fail);
        step_payload_bytes.push(jitter_payload(args.payload_bytes, rng));
    }

    WorkItem {
        step_delays_ms,
        step_should_fail,
        step_payload_bytes,
    }
}

fn sample_step_behavior(args: &SoakArgs, rng: &mut StdRng) -> (i64, bool) {
    let timeout_threshold = args.timeout_percent;
    let failure_threshold = timeout_threshold + args.failure_percent;
    let slow_threshold = failure_threshold + args.slow_percent;

    let class = rng.gen_range(0.0..100.0);
    let timeout_base_ms = i64::from(args.timeout_seconds) * 1000;

    if class < timeout_threshold {
        let delay_ms = rng.gen_range(
            (timeout_base_ms + 1500)..=(timeout_base_ms * 3).max(timeout_base_ms + 1500),
        );
        return (delay_ms, false);
    }

    if class < failure_threshold {
        return (rng.gen_range(50..=400), true);
    }

    if class < slow_threshold {
        return (rng.gen_range(1_000..=8_000), false);
    }

    (rng.gen_range(25..=400), false)
}

fn jitter_payload(base_payload: i64, rng: &mut StdRng) -> i64 {
    if base_payload <= 0 {
        return 0;
    }

    let lower = (base_payload / 2).max(1);
    let upper = (base_payload * 3 / 2).max(lower);
    rng.gen_range(lower..=upper)
}

fn build_instance(workflow: &RegisteredWorkflow, item: WorkItem) -> Result<QueuedInstance> {
    let mut state = RunnerState::new(Some(Arc::clone(&workflow.dag)), None, None, false);
    if item.step_delays_ms.len() != item.step_should_fail.len()
        || item.step_delays_ms.len() != item.step_payload_bytes.len()
    {
        bail!("step input vectors are not aligned");
    }

    for (step, ((delay_ms, should_fail), payload_bytes)) in item
        .step_delays_ms
        .iter()
        .zip(item.step_should_fail.iter())
        .zip(item.step_payload_bytes.iter())
        .enumerate()
    {
        let idx = step + 1;
        state
            .record_assignment(
                vec![format!("delay_ms_{idx}")],
                &literal_int(*delay_ms),
                None,
                Some(format!("input delay_ms_{idx} = {delay_ms}")),
            )
            .map_err(|err| anyhow!(err.0))?;
        state
            .record_assignment(
                vec![format!("should_fail_{idx}")],
                &literal_bool(*should_fail),
                None,
                Some(format!("input should_fail_{idx} = {should_fail}")),
            )
            .map_err(|err| anyhow!(err.0))?;
        state
            .record_assignment(
                vec![format!("payload_bytes_{idx}")],
                &literal_int(*payload_bytes),
                None,
                Some(format!("input payload_bytes_{idx} = {payload_bytes}")),
            )
            .map_err(|err| anyhow!(err.0))?;
    }

    let entry_node = state
        .queue_template_node(&workflow.entry_template_id, None)
        .map_err(|err| anyhow!(err.0))?;

    Ok(QueuedInstance {
        workflow_version_id: workflow.workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_node.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id: Uuid::new_v4(),
        scheduled_at: None,
    })
}

fn literal_int(value: i64) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Literal(ir::Literal {
            value: Some(ir::literal::Value::IntValue(value)),
        })),
        span: None,
    }
}

fn literal_bool(value: bool) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Literal(ir::Literal {
            value: Some(ir::literal::Value::BoolValue(value)),
        })),
        span: None,
    }
}

async fn fetch_queue_snapshot(pool: &PgPool) -> Result<QueueSnapshot> {
    let row = sqlx::query_as::<_, QueueSnapshot>(
        r#"
        SELECT
            COUNT(*)::bigint AS total,
            COUNT(*) FILTER (WHERE scheduled_at <= NOW())::bigint AS ready,
            COUNT(*) FILTER (WHERE scheduled_at > NOW())::bigint AS future,
            COUNT(*) FILTER (WHERE lock_uuid IS NOT NULL AND lock_expires_at > NOW())::bigint AS locked_live,
            COUNT(*) FILTER (WHERE lock_uuid IS NOT NULL AND (lock_expires_at IS NULL OR lock_expires_at <= NOW()))::bigint AS locked_expired,
            MIN(CASE WHEN scheduled_at <= NOW() THEN scheduled_at END) AS oldest_ready_at,
            MIN(scheduled_at) AS oldest_scheduled_at
        FROM queued_instances
        "#,
    )
    .fetch_one(pool)
    .await
    .context("fetch queue snapshot")?;

    Ok(row)
}

async fn fetch_latest_worker_status(pool: &PgPool) -> Result<Option<WorkerStatusSnapshot>> {
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

async fn fetch_lock_owners(pool: &PgPool, limit: i64) -> Result<Vec<LockOwnerRow>> {
    let rows = sqlx::query_as::<_, LockOwnerRow>(
        r#"
        SELECT
            lock_uuid,
            COUNT(*)::bigint AS rows,
            MIN(scheduled_at) AS oldest_scheduled_at,
            MAX(scheduled_at) AS newest_scheduled_at
        FROM queued_instances
        WHERE lock_uuid IS NOT NULL
        GROUP BY lock_uuid
        ORDER BY rows DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("fetch lock owners")?;

    Ok(rows)
}

async fn fetch_stale_locks(pool: &PgPool, limit: i64) -> Result<Vec<StaleLockRow>> {
    let rows = sqlx::query_as::<_, StaleLockRow>(
        r#"
        SELECT
            instance_id,
            lock_uuid,
            lock_expires_at,
            scheduled_at,
            created_at
        FROM queued_instances
        WHERE lock_uuid IS NOT NULL
        ORDER BY lock_expires_at ASC NULLS FIRST
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("fetch stale locks")?;

    Ok(rows)
}

async fn fetch_pg_stat_activity(pool: &PgPool, limit: i64) -> Result<Vec<ActivityRow>> {
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

async fn fetch_pg_stat_statements(pool: &PgPool, limit: i64) -> Result<Vec<PgStatStatementRow>> {
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

fn capture_query<T: Serialize>(result: Result<Vec<T>>) -> QueryCapture<T> {
    match result {
        Ok(rows) => QueryCapture { rows, error: None },
        Err(err) => QueryCapture {
            rows: Vec::new(),
            error: Some(err.to_string()),
        },
    }
}

fn read_tail_lines(path: &Path, max_lines: usize) -> Result<Vec<String>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = VecDeque::with_capacity(max_lines.max(1));

    for line in reader.lines() {
        let line = line.with_context(|| format!("read line from {}", path.display()))?;
        if lines.len() == max_lines {
            let _ = lines.pop_front();
        }
        lines.push_back(line);
    }

    Ok(lines.into_iter().collect())
}

async fn capture_diagnostics(
    args: &SoakArgs,
    pool: &PgPool,
    workflow: &RegisteredWorkflow,
    reason: &TerminationReason,
    samples: &VecDeque<HealthSample>,
    worker_log_path: Option<&Path>,
    run_dir: &Path,
) -> Result<PathBuf> {
    let queue_snapshot = fetch_queue_snapshot(pool).await?;
    let worker_status = fetch_latest_worker_status(pool).await?;

    let lock_owners = capture_query(fetch_lock_owners(pool, args.pg_stat_limit).await);
    let stale_locks = capture_query(fetch_stale_locks(pool, args.pg_stat_limit).await);
    let pg_stat_activity = capture_query(fetch_pg_stat_activity(pool, args.pg_stat_limit).await);
    let pg_stat_statements =
        capture_query(fetch_pg_stat_statements(pool, args.pg_stat_limit).await);

    let worker_log_tail = match worker_log_path {
        Some(path) => capture_query(read_tail_lines(path, args.max_diagnostic_tail_lines)),
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
