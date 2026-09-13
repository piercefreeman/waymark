//! Benchmark CLI for running mixed IR workloads against Postgres through
//! the durable VM execution subsystem.

mod actions;
mod cases;
mod cli;
mod execution;
mod observability;
mod registration;
mod report;

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser as _;
use color_eyre::eyre::{WrapErr as _, bail};
use waymark_backend_postgres::PostgresBackend;
use waymark_observability::obs;
use waymark_secret_string::SecretStr;
use waymark_support_integration::{LOCAL_POSTGRES_DSN, ensure_local_postgres};

use crate::report::BenchmarkStats;

/// The supervisor of the run's tasks: their errors differ per task, so
/// they are supervised erased.
type Supervisor = waymark_task_supervisor::Supervisor<waymark_task_supervisor::BoxedError>;

/// How often to poll the recorded execution results while draining.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long the drain may go without recording a single new execution
/// result before the benchmark declares a stall and aborts.
const DRAIN_STALL_TIMEOUT: Duration = Duration::from_secs(120);

#[obs]
async fn run_benchmark(
    count_per_case: NonZeroUsize,
    base: i64,
    dsn: &SecretStr,
    max_pinned: NonZeroUsize,
    pool_size: NonZeroU32,
    registration_batch_max: NonZeroUsize,
    with_observability: bool,
) -> Result<BenchmarkStats, color_eyre::eyre::Report> {
    let cases = cases::build_cases(base)?;
    if dsn.expose_secret() == LOCAL_POSTGRES_DSN.expose_secret() {
        ensure_local_postgres()
            .await
            .wrap_err("bootstrap local postgres")?;
    }
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(pool_size.get())
        .connect(dsn.expose_secret())
        .await
        .wrap_err("connect postgres")?;
    waymark_backend_postgres::reset::rebuild_schema(&pool)
        .await
        .wrap_err("rebuild database schema")?;
    let backend = PostgresBackend::new(pool);
    let codec = waymark_vm_codec_rmp::RmpCodec;
    let executables =
        waymark_workflow_service_vm_executables::ExecutablesService::new(backend.clone(), codec);
    let registration =
        waymark_workflow_service_vm_runtimes::RegistrationService::new(backend.clone(), codec);

    let queue_start = Instant::now();
    let total = registration::register_benchmark_vms(
        &executables,
        &registration,
        &cases,
        count_per_case,
        registration_batch_max,
    )
    .await?;
    println!(
        "Queued {total} instances across {} IR jobs in {:.2?}",
        cases.len(),
        queue_start.elapsed(),
    );

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let force_shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor: Supervisor = waymark_task_supervisor::start(shutdown_token.clone());

    let observability = if with_observability {
        let node_id = waymark_ids::NodeId::new_uuid_v4();
        let observability =
            observability::start(&mut supervisor, dsn, node_id, shutdown_token.child_token())
                .await?;
        Some(observability)
    } else {
        None
    };
    let observability_events = observability.as_ref().map(|observability| {
        waymark_execution_bringup::ObservabilityEvents {
            emitter: Arc::clone(&observability.emitter),
            vm_driver_hooks_policy: observability.vm_driver_hooks_policy,
        }
    });

    let (worker_pool, pool_loop) = waymark_worker_inline::run(actions::action_registry());
    supervisor.spawn("inline worker pool loop", pool_loop);

    let start = Instant::now();
    let execution_handles = waymark_execution_bringup::start(
        execution::durable_execution_config(max_pinned)?,
        Arc::new(backend.clone()),
        Arc::new(worker_pool),
        observability_events,
        shutdown_token.child_token(),
        force_shutdown_token.child_token(),
    )
    .await;
    execution::track(&mut supervisor, execution_handles);

    // The supervisor cancels the shutdown token when any task ends before
    // the shutdown was requested (e.g. on a lock fence breach) — watched
    // below so the drain loop fails loudly instead of waiting forever on a
    // dead subsystem. Whichever way the loop ends, the tasks are drained.
    let mut last_progress = (0i64, Instant::now());
    let draining_result: Result<(), color_eyre::eyre::Report> = async {
        loop {
            let done: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM vm_execution_results")
                .fetch_one(backend.pool())
                .await
                .wrap_err("count recorded execution results")?;
            if done as usize >= total {
                break;
            }
            if shutdown_token.is_cancelled() {
                bail!(
                    "execution subsystem shut down after draining only {done} of {total} instances"
                );
            }
            if done > last_progress.0 {
                last_progress = (done, Instant::now());
            } else if last_progress.1.elapsed() > DRAIN_STALL_TIMEOUT {
                bail!(
                    "drain stalled: no new execution results for {}s at {done} of {total} instances",
                    DRAIN_STALL_TIMEOUT.as_secs(),
                );
            }
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        }

        Ok(())
    }
    .await;
    let elapsed = start.elapsed();

    // Nothing is draining anymore — force the pinning manager out of its
    // drain loop along with the graceful stop, then wait for every task.
    shutdown_token.cancel();
    force_shutdown_token.cancel();
    let report = supervisor.drain().await;

    draining_result?;
    if report.any_before_shutdown() {
        bail!("a task ended before the shutdown was requested:\n{report}");
    }

    if let Some(observability) = observability {
        let events = observability::recorded_events(observability).await?;
        println!("Observability events recorded: {events}");
    }

    Ok(BenchmarkStats {
        total,
        elapsed,
        query_counts: backend.query_counts(),
        batch_counts: backend.batch_size_counts(),
    })
}

fn main() -> Result<(), waymark_fn_main_common::Error> {
    let args = cli::BenchmarkArgs::parse();
    // The guard is dropped on every exit path, so the chrome trace tail
    // survives error returns too.
    let (observability_layer, _flush_on_drop) = waymark_observability_setup::tracing_layer(
        &waymark_observability_setup::ObservabilityOptions {
            tokio_console: args.observe,
            chrome_trace_path: args.trace.clone(),
        },
    );
    waymark_fn_main_common::init_with(waymark_fn_main_common::Params {
        tracing: waymark_fn_main_common::tracing::Params {
            filter_bypassing_layer: observability_layer,
            filter_wrapped_layer: waymark_fn_main_common::tracing::NO_EXTRA_LAYER,
        },
        skip_color_eyre: false,
    })?;
    let runtime = tokio::runtime::Runtime::new().wrap_err("create tokio runtime")?;
    let _span = tracing::info_span!("benchmark_main").entered();
    let max_pinned: NonZeroUsize = envfury::or_parse("WAYMARK_MAX_CONCURRENT_INSTANCES", "500")?;
    let pool_size: NonZeroU32 = envfury::or_parse("WAYMARK_DB_POOL_SIZE", "10")?;
    let registration_batch_max: NonZeroUsize =
        envfury::or_parse("WAYMARK_REGISTRATION_BATCH_MAX", "256")?;
    println!("max_pinned = {max_pinned}");
    println!("db_pool_size = {pool_size}");
    println!("registration_batch_max = {registration_batch_max}");
    println!("observability = {}", args.observability);
    let stats = runtime.block_on(run_benchmark(
        args.count,
        args.base,
        &args.dsn,
        max_pinned,
        pool_size,
        registration_batch_max,
        args.observability,
    ))?;
    println!("Benchmark completed in {:.2?}", stats.elapsed);
    println!("{}", report::format_query_counts(&stats.query_counts));
    println!("{}", report::format_batch_size_counts(&stats.batch_counts));
    if let Some(json) = &args.json {
        let report = report::format_json(&stats, args.count, args.base);
        if json == "-" {
            println!("{report}");
        } else {
            std::fs::write(json, format!("{report}\n"))
                .wrap_err_with(|| format!("write the JSON report to {json}"))?;
        }
    }
    Ok(())
}
