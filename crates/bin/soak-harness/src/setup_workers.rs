use std::fs::File;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use sqlx::PgPool;
use tokio::process::{Child, Command};
use tracing::{info, warn};

use crate::data;

#[derive(Debug)]
pub struct WorkerProcess {
    pub child: Child,
    pub log_path: PathBuf,
}

pub async fn start_workers(args: &crate::cli::SoakArgs, run_dir: &Path) -> Result<WorkerProcess> {
    let webapp_enabled = !args.disable_webapp;
    let log_path = run_dir.join("start-workers.log");
    // `CARGO_MANIFEST_DIR` here is `crates/bin/soak-harness`, while the soak action module lives at
    // `<workspace>/python/tests/fixtures_actions/soak_actions.py` and the child binary is resolved from
    // `<workspace>/target/debug/waymark-start-workers`. Run the child from the workspace root and seed
    // `PYTHONPATH` with the workspace Python directories so `tests.fixtures_actions.soak_actions`
    // remains importable even if worker-remote falls back to the caller's current directory.
    let repo_root = repo_root();
    let log_file = File::create(&log_path)
        .with_context(|| format!("create worker log file {}", log_path.display()))?;
    let log_file_err = log_file
        .try_clone()
        .with_context(|| format!("clone worker log handle {}", log_path.display()))?;

    let mut cmd = start_workers_command();
    cmd.current_dir(&repo_root);
    cmd.env("WAYMARK_DATABASE_URL", args.dsn.expose_secret());
    cmd.env("WAYMARK_USER_MODULE", &args.user_module);
    cmd.env("PYTHONPATH", soak_python_path(&repo_root)?);
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

    cmd.stdout(Stdio::from(log_file));
    cmd.stderr(Stdio::from(log_file_err));

    let child = cmd.spawn().context("spawn waymark-start-workers process")?;
    info!(
        log_path = %log_path.display(),
        webapp_enabled,
        webapp_addr = %args.webapp_addr,
        "started worker process"
    );

    Ok(WorkerProcess { child, log_path })
}

fn start_workers_command() -> Command {
    let repo_root = repo_root();
    let local_debug_bin = repo_root
        .join("target")
        .join("debug")
        .join(if cfg!(windows) {
            "waymark-start-workers.exe"
        } else {
            "waymark-start-workers"
        });
    if local_debug_bin.is_file() {
        return Command::new(local_debug_bin);
    }

    if let Some(start_workers_bin) = find_executable("waymark-start-workers") {
        return Command::new(start_workers_bin);
    }

    let mut command = Command::new("cargo");
    command
        .arg("run")
        .arg("--bin")
        .arg("waymark-start-workers")
        .arg("--");
    command
}

fn repo_root() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for _ in 0..3 {
        debug_assert!(
            path.components().next_back() != Some(Component::RootDir),
            "expected soak harness to live under workspace root"
        );
        path.pop();
    }
    path
}

fn soak_python_path(repo_root: &Path) -> Result<std::ffi::OsString> {
    let mut python_paths = vec![
        repo_root.join("python"),
        repo_root.join("python").join("src"),
    ];
    if let Some(existing) = std::env::var_os("PYTHONPATH") {
        python_paths.extend(std::env::split_paths(&existing));
    }

    std::env::join_paths(&python_paths).context("build soak PYTHONPATH")
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

pub async fn shutdown_worker(worker: &mut WorkerProcess) -> Result<()> {
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

pub async fn shutdown_worker_if_running(worker: &mut Option<WorkerProcess>) {
    if let Some(worker_process) = worker.as_mut()
        && let Err(err) = shutdown_worker(worker_process).await
    {
        warn!(error = %err, "failed to stop worker process during error cleanup");
    }
}

pub async fn wait_for_worker_status(
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
            let tail = crate::common::read_tail_lines(&worker.log_path, 80).unwrap_or_default();
            let tail_text = if tail.is_empty() {
                "worker log unavailable".to_string()
            } else {
                tail.join("\n")
            };
            bail!("worker process exited before first heartbeat: {status}\nlog tail:\n{tail_text}");
        }

        if data::fetch_latest_worker_status(pool).await?.is_some() {
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
