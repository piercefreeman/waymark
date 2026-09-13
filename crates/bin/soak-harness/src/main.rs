//! Long-running soak harness for local, prod-like runtime stress testing.
//!
//! The harness can:
//! - Boot local Postgres via docker compose
//! - Start the standard `waymark-start-workers` runtime as a child process
//! - Continuously queue synthetic workloads with configurable timeout/failure mix
//! - Detect sustained stall conditions (near-zero actions/sec with large ready queue)
//! - Capture diagnostics (DB snapshots + worker log tail) on exit/issue

mod cli;
mod common;
mod data;
mod diag;
mod flow;
mod setup_db;
mod setup_workers;
mod setup_workflows;
mod shutdown;

use std::collections::VecDeque;
use std::fs::{self};
use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use color_eyre::eyre::{WrapErr as _, bail};
use tracing::{error, info, warn};
use waymark_backend_postgres::PostgresBackend;

const DB_READY_TIMEOUT: Duration = Duration::from_secs(90);

/// The schema the worker's observability bringup provisions its store
/// in. Hardcoded for now — mirrors `waymark-observability-bringup`.
const OBSERVABILITY_SCHEMA: &str = "observability";

#[tokio::main]
async fn main() -> Result<(), color_eyre::eyre::Report> {
    waymark_fn_main_common::init()?;

    let args = cli::SoakArgs::parse();
    cli::validate_args(&args)?;

    let run_id = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let run_dir = args.diagnostic_dir.join(&run_id);
    fs::create_dir_all(&run_dir)
        .wrap_err_with(|| format!("create run directory {}", run_dir.display()))?;

    // Create a convenience symlink pointing to this run directory at
    // the `diagnostic_dir`; the goal is to allow repeating
    // the same `cat`/`tail` command for reading the latest-run logs.
    symlink_last(&args.diagnostic_dir, &run_id)?;

    info!(run_dir = %run_dir.display(), "starting soak harness");
    info!(?args, "soak harness config");

    // The OS's shutdown requests are tracked from here on, for the whole
    // run; what they do is `shutdown`'s.
    shutdown::managed(|stop_token, abort_token| run(args, run_dir, stop_token, abort_token)).await?
}

/// The soak run proper, from booting the database to the diagnostics.
async fn run(
    args: cli::SoakArgs,
    run_dir: std::path::PathBuf,
    stop_token: tokio_util::sync::CancellationToken,
    abort_token: tokio_util::sync::CancellationToken,
) -> Result<(), color_eyre::eyre::Report> {
    if !args.skip_postgres_boot {
        common::run_unless_cancelled(&stop_token, "booting postgres", setup_db::boot_postgres())
            .await?;
    }

    let pool = common::run_unless_cancelled(
        &stop_token,
        "waiting for the database",
        setup_db::wait_for_database(&args.dsn, DB_READY_TIMEOUT),
    )
    .await?;
    common::run_unless_cancelled(&stop_token, "running migrations", async {
        waymark_backend_postgres_migrations::run(&pool)
            .await
            .wrap_err("run migrations before soak")
    })
    .await?;

    let observability_pool = common::run_unless_cancelled(
        &stop_token,
        "connecting the observability schema pool",
        async {
            waymark_sqlx_postgres_schema_pool::connect(
                args.dsn.expose_secret(),
                OBSERVABILITY_SCHEMA,
            )
            .await
            .wrap_err("connect the observability schema pool")
        },
    )
    .await?;
    let observability_store = waymark_observability_store_postgres::Store {
        pool: observability_pool,
    };

    let backend = PostgresBackend::new(pool.clone());
    if !args.keep_existing_data {
        info!("clearing durable-VM and observability data before soak run");
        common::run_unless_cancelled(&stop_token, "clearing durable tables", async {
            waymark_backend_postgres::reset::truncate_all(&pool)
                .await
                .wrap_err("clear durable tables")
        })
        .await?;
        common::run_unless_cancelled(&stop_token, "clearing observability tables", async {
            waymark_observability_store_postgres::reset::truncate_all(&observability_store.pool)
                .await
                .wrap_err("clear observability tables")
        })
        .await?;
    }
    let services = setup_workflows::soak_services(&backend);

    let mut worker = if args.skip_worker_launch {
        None
    } else {
        Some(
            common::run_unless_cancelled(
                &stop_token,
                "starting the worker",
                setup_workers::start_workers(&args, &run_dir),
            )
            .await?,
        )
    };

    if let Some(worker_process) = worker.as_mut()
        && let Err(err) = common::run_unless_cancelled(
            &stop_token,
            "waiting for the first node sample",
            setup_workers::wait_for_node_sample(
                &observability_store,
                Duration::from_secs(60),
                Duration::from_secs(args.startup_log_interval_secs.max(1)),
                worker_process,
            ),
        )
        .await
    {
        setup_workers::shutdown_worker_if_running(&mut worker, &abort_token).await;
        return Err(err);
    }

    let workflow = match common::run_unless_cancelled(
        &stop_token,
        "registering the soak workflow",
        setup_workflows::register_workflow(
            &services,
            args.timeout_seconds,
            args.actions_per_workflow,
            &args.user_module,
        ),
    )
    .await
    {
        Ok(workflow) => workflow,
        Err(err) => {
            setup_workers::shutdown_worker_if_running(&mut worker, &abort_token).await;
            return Err(err);
        }
    };
    info!(
        workflow_name = %workflow.workflow_name,
        workflow_version_id = %workflow.workflow_version_id,
        "registered soak workflow"
    );
    let expected_actions_per_minute = args.queue_rate_per_minute.saturating_mul(
        args.actions_per_workflow
            .try_into()
            .unwrap_or(std::num::NonZeroU128::MAX),
    );
    info!(
        queue_rate_per_minute = args.queue_rate_per_minute,
        actions_per_workflow = args.actions_per_workflow,
        expected_actions_per_minute,
        "soak throughput target"
    );

    let run_result = flow::run_soak_loop(
        &args,
        &services,
        &pool,
        &observability_store,
        &workflow,
        &mut worker,
        stop_token.clone(),
    )
    .await;
    let (reason, samples) = match run_result {
        Ok(result) => result,
        Err(err) => {
            error!(error = %err, "soak loop failed");
            (
                flow::TerminationReason::IssueDetected(format!("soak loop error: {err}")),
                VecDeque::new(),
            )
        }
    };

    // The worker is stopped whatever the capture did: a failed or
    // aborted capture must not leave the child behind.
    let diagnostics_result = common::run_unless_cancelled(
        &abort_token,
        "capturing diagnostics",
        diag::capture_diagnostics(
            &args,
            &pool,
            &observability_store,
            &workflow,
            &reason,
            &samples,
            worker.as_ref().map(|process| process.log_path.as_path()),
            &run_dir,
        ),
    )
    .await;
    let shutdown_result = match worker.as_mut() {
        Some(worker_process) => setup_workers::shutdown_worker(worker_process, &abort_token).await,
        None => Ok(()),
    };
    let diagnostics_path = match diagnostics_result {
        Ok(diagnostics_path) => diagnostics_path,
        Err(err) => {
            if let Err(shutdown_err) = shutdown_result {
                warn!(error = %shutdown_err, "failed to stop worker process during error cleanup");
            }
            return Err(err);
        }
    };
    shutdown_result?;

    info!(
        reason = ?reason,
        diagnostics = %diagnostics_path.display(),
        "soak harness finished"
    );

    if reason.is_error_exit() {
        bail!(
            "soak harness detected an issue; see {}",
            diagnostics_path.display()
        );
    }

    Ok(())
}

/// Creates a `last` symlink that points to the current run directory adjacent
/// to it.
fn symlink_last(
    diagnostic_dir: &std::path::Path,
    run_id: &str,
) -> Result<(), color_eyre::eyre::Report> {
    symlink_if_possible(std::path::Path::new(run_id), diagnostic_dir.join("last"))?;
    Ok(())
}

/// Creates a new or atomically replaces an existing symlink if the OS supports
/// it; if it doesn't - do nothing.
fn symlink_if_possible(
    target: impl AsRef<std::path::Path>,
    link: impl AsRef<std::path::Path>,
) -> Result<(), color_eyre::eyre::Report> {
    #[cfg(unix)]
    {
        let new_link = link.as_ref().with_added_extension("new");
        std::os::unix::fs::symlink(target, &new_link)?;
        fs::rename(new_link, link)?;
    }

    Ok(())
}
