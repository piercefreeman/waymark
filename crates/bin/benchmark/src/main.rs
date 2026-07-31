//! Benchmark CLI for running mixed IR workloads against Postgres through
//! the durable VM execution subsystem.

mod actions;
mod cases;
mod cli;
mod execution;
mod registration;
mod report;
mod setup_db;

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser as _;
use waymark_backend_postgres::PostgresBackend;
use waymark_observability::obs;
use waymark_secret_string::SecretStr;
use waymark_support_integration::{LOCAL_POSTGRES_DSN, ensure_local_postgres};
use waymark_worker_inline::InlineWorkerPool;

use crate::report::BenchmarkStats;

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
) -> BenchmarkStats {
    let cases = cases::build_cases(base);
    if dsn.expose_secret() == LOCAL_POSTGRES_DSN.expose_secret() {
        ensure_local_postgres()
            .await
            .expect("bootstrap local postgres");
    }
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(pool_size.get())
        .connect(dsn.expose_secret())
        .await
        .expect("connect postgres");
    setup_db::drop_benchmark_tables(&pool).await;
    waymark_backend_postgres_migrations::run(&pool)
        .await
        .expect("run migrations");
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
    .await;
    println!(
        "Queued {total} instances across {} IR jobs in {:.2?}",
        cases.len(),
        queue_start.elapsed(),
    );

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let force_shutdown_token = tokio_util::sync::CancellationToken::new();
    // The subsystem's internal loops cancel this token when any of them
    // fails (e.g. a lock fence breach) — watched below so the drain loop
    // fails loudly instead of waiting forever on a dead subsystem.
    let subsystem_token = shutdown_token.child_token();
    let start = Instant::now();
    let execution_handles = waymark_execution_bringup::start(
        execution::durable_execution_config(max_pinned),
        Arc::new(backend.clone()),
        InlineWorkerPool::new(actions::action_registry()),
        subsystem_token.clone(),
        force_shutdown_token.child_token(),
    )
    .await
    .expect("start execution subsystem");

    let mut last_progress = (0i64, Instant::now());
    loop {
        let done: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM vm_execution_results")
            .fetch_one(backend.pool())
            .await
            .expect("count recorded execution results");
        if done as usize >= total {
            break;
        }
        if subsystem_token.is_cancelled() {
            panic!("execution subsystem shut down after draining only {done} of {total} instances");
        }
        if done > last_progress.0 {
            last_progress = (done, Instant::now());
        } else if last_progress.1.elapsed() > DRAIN_STALL_TIMEOUT {
            panic!(
                "drain stalled: no new execution results for {}s at {done} of {total} instances",
                DRAIN_STALL_TIMEOUT.as_secs(),
            );
        }
        tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
    }
    let elapsed = start.elapsed();

    // Nothing is draining anymore — force the pinning manager out of its
    // drain loop along with the graceful stop.
    shutdown_token.cancel();
    force_shutdown_token.cancel();
    execution::shutdown_execution(execution_handles).await;

    BenchmarkStats {
        elapsed,
        query_counts: backend.query_counts(),
        batch_counts: backend.batch_size_counts(),
    }
}

fn main() {
    let args = cli::BenchmarkArgs::parse();
    if args.observe || args.trace.is_some() {
        waymark_observability_setup::init(waymark_observability_setup::ObservabilityOptions {
            console: args.observe,
            trace_path: args.trace.clone(),
        });
    } else {
        waymark_fn_main_common::init().expect("tracing setup");
    }
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let _span = tracing::info_span!("benchmark_main").entered();
    let max_pinned: NonZeroUsize = envfury::or_parse("WAYMARK_MAX_CONCURRENT_INSTANCES", "500")
        .expect("WAYMARK_MAX_CONCURRENT_INSTANCES");
    let pool_size: NonZeroU32 =
        envfury::or_parse("WAYMARK_DB_POOL_SIZE", "10").expect("WAYMARK_DB_POOL_SIZE");
    let registration_batch_max: NonZeroUsize =
        envfury::or_parse("WAYMARK_REGISTRATION_BATCH_MAX", "256")
            .expect("WAYMARK_REGISTRATION_BATCH_MAX");
    println!("max_pinned = {max_pinned}");
    println!("db_pool_size = {pool_size}");
    let stats = runtime.block_on(run_benchmark(
        args.count,
        args.base,
        &args.dsn,
        max_pinned,
        pool_size,
        registration_batch_max,
    ));
    println!("Benchmark completed in {:.2?}", stats.elapsed);
    println!("{}", report::format_query_counts(stats.query_counts));
    println!("{}", report::format_batch_size_counts(stats.batch_counts));
    if args.trace.is_some() {
        waymark_observability_setup::flush();
    }
}
