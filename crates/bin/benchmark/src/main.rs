//! Benchmark CLI for running mixed IR workloads against Postgres through
//! the durable VM execution subsystem.

use std::collections::HashMap;
use std::env;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use prost::Message;
use rand::seq::SliceRandom;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use waymark_backend_postgres::PostgresBackend;
use waymark_convert_core::Convert;
use waymark_observability::obs;
use waymark_proto::ast as ir;
use waymark_secret_string::{SecretStr, SecretString};
use waymark_smoke_sources::{
    build_control_flow_program, build_parallel_spread_program, build_program,
    build_try_except_program, build_while_loop_program,
};
use waymark_support_integration::{LOCAL_POSTGRES_DSN, ensure_local_postgres};
use waymark_worker_core::WorkerPoolError;
use waymark_worker_inline::{ActionCallable, InlineWorkerPool};

const DEFAULT_DSN: &SecretStr = LOCAL_POSTGRES_DSN;
const DEFAULT_MAX_PINNED: NonZeroUsize = NonZeroUsize::new(500).unwrap();

/// How often to poll the recorded execution results while draining.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long the drain may go without recording a single new execution
/// result before the benchmark declares a stall and aborts.
const DRAIN_STALL_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Parser, Debug)]
#[command(
    name = "waymark-benchmark",
    about = "Benchmark mixed IR workloads against Postgres."
)]
struct BenchmarkArgs {
    #[arg(long, default_value_t = 10_000.try_into().unwrap())]
    count: NonZeroUsize,
    #[arg(long, default_value_t = 5)]
    base: i64,
    #[arg(long, default_value = DEFAULT_DSN.expose_secret())]
    dsn: SecretString,
    #[arg(long, default_value_t = false)]
    observe: bool,
    #[arg(long, num_args = 0..=1, default_missing_value = "target/benchmark-trace.json")]
    trace: Option<String>,
}

async fn action_double(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let value = kwargs
        .get("value")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| WorkerPoolError::new("ActionError", "double expects integer value"))?;
    Ok(Value::Number((value * 2).into()))
}

async fn action_sum(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let values = kwargs
        .get("values")
        .and_then(|value| value.as_array())
        .ok_or_else(|| WorkerPoolError::new("ActionError", "sum expects list of integers"))?;
    let mut total = 0i64;
    for item in values {
        total += item.as_i64().unwrap_or(0);
    }
    Ok(Value::Number(total.into()))
}

fn action_registry() -> HashMap<String, ActionCallable> {
    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "double".to_string(),
        std::sync::Arc::new(|kwargs| Box::pin(action_double(kwargs))),
    );
    actions.insert(
        "sum".to_string(),
        std::sync::Arc::new(|kwargs| Box::pin(action_sum(kwargs))),
    );
    actions
}

struct BenchmarkCase {
    program: waymark_vm_ast_old::Program,
    inputs: HashMap<String, waymark_system_vm::Value>,
    ir_hash: String,
}

fn build_cases(base: i64) -> HashMap<String, BenchmarkCase> {
    let mut cases = HashMap::new();
    let entries: Vec<(&str, ir::Program, HashMap<String, Value>)> = vec![
        (
            "smoke",
            build_program(),
            HashMap::from([("base".to_string(), Value::Number(base.into()))]),
        ),
        (
            "control_flow",
            build_control_flow_program().expect("control_flow program"),
            HashMap::from([("base".to_string(), Value::Number(2.into()))]),
        ),
        (
            "parallel_spread",
            build_parallel_spread_program().expect("parallel_spread program"),
            HashMap::from([("base".to_string(), Value::Number(3.into()))]),
        ),
        (
            "try_except",
            build_try_except_program().expect("try_except program"),
            HashMap::from([(
                "values".to_string(),
                Value::Array(vec![1.into(), 2.into(), 3.into()]),
            )]),
        ),
        (
            "while_loop",
            build_while_loop_program().expect("while_loop program"),
            HashMap::from([("limit".to_string(), Value::Number(6.into()))]),
        ),
    ];

    for (name, program, inputs) in entries {
        let program_proto = program.encode_to_vec();
        let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
        let program = waymark_vm_ast_old_proto::convert(program).expect("convert IR to VM AST");
        let inputs = inputs
            .into_iter()
            .map(|(name, value)| {
                let value: waymark_system_vm::Value =
                    waymark_vm_value_convert_json::Converter::convert(value);
                (name, value)
            })
            .collect();
        cases.insert(
            name.to_string(),
            BenchmarkCase {
                program,
                inputs,
                ir_hash,
            },
        );
    }
    cases
}

struct CompiledCase {
    executable_id: waymark_ids::WorkflowVersionId,
    executable: Arc<waymark_system_vm::Executable>,
    metadata: waymark_vm_compiler_for_ast_old_core::Metadata,
}

async fn register_benchmark_vms(
    executables: &waymark_workflow_service_vm_executables::ExecutablesService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >,
    registration: &waymark_workflow_service_vm_runtimes::RegistrationService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
    >,
    cases: &HashMap<String, BenchmarkCase>,
    count_per_case: NonZeroUsize,
) -> usize {
    let mut compiled = HashMap::new();
    for (name, case) in cases {
        match executables
            .compile_and_store(name, &case.ir_hash, &case.program)
            .await
        {
            Ok((executable_id, executable, metadata)) => {
                compiled.insert(
                    name.clone(),
                    CompiledCase {
                        executable_id,
                        executable: Arc::new(executable),
                        metadata,
                    },
                );
            }
            Err(err) => {
                eprintln!("Skipping IR job '{name}': compilation failed: {err}");
            }
        }
    }

    let mut case_names = Vec::new();
    for name in compiled.keys() {
        for _ in 0..count_per_case.get() {
            case_names.push(name.clone());
        }
    }
    case_names.shuffle(&mut rand::rng());

    for name in &case_names {
        let case = cases.get(name).expect("case");
        let compiled_case = compiled.get(name).expect("compiled case");

        let call_spec = waymark_vm_runtime_builder::builder(&compiled_case.metadata)
            .first_fn()
            .expect("select entry function")
            .args(case.inputs.clone())
            .expect("match entry function arguments");
        let runtime = waymark_system_vm::Runtime::with_custom_entrypoint(
            waymark_system_vm::Interpreter::default(),
            Arc::clone(&compiled_case.executable),
            call_spec,
        )
        .expect("create VM runtime");

        registration
            .register_vm(
                waymark_ids::InstanceId::new_uuid_v4(),
                compiled_case.executable_id,
                |serializer| runtime.snapshot(serializer),
            )
            .await
            .expect("register vm");
    }

    case_names.len()
}

fn format_query_counts(counts: HashMap<String, usize>) -> String {
    let mut keys: Vec<_> = counts.keys().cloned().collect();
    keys.sort();
    let mut lines = vec!["Postgres query counts:".to_string()];
    for key in keys {
        let value = counts.get(&key).copied().unwrap_or(0);
        lines.push(format!("  {key}: {value}"));
    }
    lines.join("\n")
}

fn median_from_counts(counts: &HashMap<usize, usize>) -> usize {
    let total: usize = counts.values().sum();
    if total == 0 {
        return 0;
    }
    let threshold = total.div_ceil(2);
    let mut running = 0;
    let mut sizes: Vec<_> = counts.keys().cloned().collect();
    sizes.sort();
    for size in sizes {
        running += counts.get(&size).copied().unwrap_or(0);
        if running >= threshold {
            return size;
        }
    }
    0
}

fn format_batch_size_counts(batch_counts: HashMap<String, HashMap<usize, usize>>) -> String {
    let mut keys: Vec<_> = batch_counts.keys().cloned().collect();
    keys.sort();
    let mut lines = vec!["Postgres batch size p50:".to_string()];
    for key in keys {
        if let Some(counts) = batch_counts.get(&key) {
            if counts.is_empty() {
                continue;
            }
            let median = median_from_counts(counts);
            let total: usize = counts.values().sum();
            lines.push(format!("  {key}: p50={median} batches={total}"));
        }
    }
    lines.join("\n")
}

async fn drop_benchmark_tables(pool: &PgPool) {
    sqlx::query(
        r#"
        DROP TABLE IF EXISTS
            worker_status,
            workflow_schedules,
            queued_instances,
            runner_instances,
            runner_actions_done,
            runner_instances_done,
            runner_graph_updates,
            workflow_versions,
            action_call_completions,
            action_call_requests,
            sleep_requests,
            vm_executables,
            vm_runtime_snapshots,
            runnable_workloads,
            vm_execution_results,
            _sqlx_migrations
        CASCADE
        "#,
    )
    .execute(pool)
    .await
    .expect("drop benchmark tables");

    // The migrations also create trigger functions; they survive the table
    // drops and would fail the re-run with "function already exists".
    sqlx::query(
        r#"
        DROP FUNCTION IF EXISTS
            action_call_requests_remove_on_completion,
            vm_runtime_snapshots_cleanup_on_delete
        CASCADE
        "#,
    )
    .execute(pool)
    .await
    .expect("drop benchmark trigger functions");
}

fn durable_execution_config(
    max_pinned: NonZeroUsize,
) -> waymark_execution_bringup::Config<uuid::Uuid> {
    waymark_execution_bringup::Config {
        node_id: uuid::Uuid::new_v4(),
        // Generous lock/pinning windows: at benchmark load the renewal and
        // refresh heartbeats lag behind their default 15s TTLs, and a lapsed
        // lock gets relocked at revive — the renewal loop then breaches its
        // fence (`HeldElsewhere`) and shuts the whole subsystem down.
        action_effect_reconciler_lock_ttl: Duration::from_secs(120).try_into().unwrap(),
        action_effect_reconciler_lock_heartbeat: Duration::from_secs(15).try_into().unwrap(),
        max_pinned,
        pinning_ttl: Duration::from_secs(120).try_into().unwrap(),
        pinning_heartbeat: Duration::from_secs(15).try_into().unwrap(),
        pinning_fencing_margin: Duration::from_secs(5).try_into().unwrap(),
        sleep_poll_interval: Duration::from_millis(250).try_into().unwrap(),
        vm_retention: Duration::from_secs(60).try_into().unwrap(),
        vm_sweep_interval: Duration::from_secs(10).try_into().unwrap(),
        executable_retention: Duration::from_secs(300).try_into().unwrap(),
        executable_sweep_interval: Duration::from_secs(60).try_into().unwrap(),
    }
}

async fn shutdown_execution(handles: waymark_execution_bringup::Handles) {
    let waymark_execution_bringup::Handles {
        pinning_manager,
        execution_driver,
        executable_sweeper,
        vm_sweeper,
        durable_action_completions_writer,
        durable_action_completions_poller,
        durable_action_completions_acker,
        durable_sleeps_poller,
        durable_sleeps_acker,
        action_effect_reconciler_lock_renewal,
    } = handles;

    let _ = tokio::time::timeout(Duration::from_secs(5), pinning_manager).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), execution_driver).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), executable_sweeper).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), vm_sweeper).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_writer).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_poller).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_acker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_sleeps_poller).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_sleeps_acker).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        action_effect_reconciler_lock_renewal,
    )
    .await;
}

struct BenchmarkStats {
    elapsed: Duration,
    query_counts: HashMap<String, usize>,
    batch_counts: HashMap<String, HashMap<usize, usize>>,
}

#[obs]
async fn run_benchmark(
    count_per_case: NonZeroUsize,
    base: i64,
    dsn: &SecretStr,
    max_pinned: NonZeroUsize,
) -> BenchmarkStats {
    let cases = build_cases(base);
    if dsn.expose_secret() == LOCAL_POSTGRES_DSN.expose_secret() {
        ensure_local_postgres()
            .await
            .expect("bootstrap local postgres");
    }
    let pool = PgPool::connect(dsn.expose_secret())
        .await
        .expect("connect postgres");
    drop_benchmark_tables(&pool).await;
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
    let total = register_benchmark_vms(&executables, &registration, &cases, count_per_case).await;
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
        durable_execution_config(max_pinned),
        Arc::new(backend.clone()),
        InlineWorkerPool::new(action_registry()),
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
    shutdown_execution(execution_handles).await;

    BenchmarkStats {
        elapsed,
        query_counts: backend.query_counts(),
        batch_counts: backend.batch_size_counts(),
    }
}

fn benchmark_max_pinned() -> NonZeroUsize {
    env::var("WAYMARK_MAX_CONCURRENT_INSTANCES")
        .ok()
        .and_then(|value| value.trim().parse::<NonZeroUsize>().ok())
        .unwrap_or(DEFAULT_MAX_PINNED)
}

fn main() {
    let args = BenchmarkArgs::parse();
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
    let max_pinned = benchmark_max_pinned();
    println!("max_pinned = {max_pinned}");
    let stats = runtime.block_on(run_benchmark(args.count, args.base, &args.dsn, max_pinned));
    println!("Benchmark completed in {:.2?}", stats.elapsed);
    println!("{}", format_query_counts(stats.query_counts));
    println!("{}", format_batch_size_counts(stats.batch_counts));
    if args.trace.is_some() {
        waymark_observability_setup::flush();
    }
}
