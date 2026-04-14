//! Benchmark CLI for running mixed IR workloads against Postgres.

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
use waymark_core_backend::QueuedInstance;
use waymark_ids::{InstanceId, LockId, WorkflowVersionId};
use waymark_secret_string::{SecretStr, SecretString};
use waymark_support_integration::{LOCAL_POSTGRES_DSN, ensure_local_postgres};
use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowRegistryBackend as _};

use waymark_dag_builder::convert_to_dag;
use waymark_ir_conversions::literal_from_json_value;
use waymark_observability::obs;
use waymark_proto::ast as ir;
use waymark_runloop::{RunLoop, RunLoopConfig};
use waymark_runner_state::RunnerState;
use waymark_smoke_sources::{
    build_control_flow_program, build_parallel_spread_program, build_program,
    build_try_except_program, build_while_loop_program,
};
use waymark_worker_core::WorkerPoolError;
use waymark_worker_inline::{ActionCallable, InlineWorkerPool};

const DEFAULT_DSN: &SecretStr = LOCAL_POSTGRES_DSN;
const DEFAULT_MAX_CONCURRENT_INSTANCES: NonZeroUsize = NonZeroUsize::new(500).unwrap();

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
    #[arg(long, default_value_t = 250.try_into().unwrap())]
    batch_size: NonZeroUsize,
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

#[derive(Clone)]
struct BenchmarkCase {
    dag: Arc<waymark_dag::DAG>,
    inputs: HashMap<String, Value>,
    program_proto: Vec<u8>,
    ir_hash: String,
}

fn build_cases(base: i64) -> HashMap<String, BenchmarkCase> {
    let smoke_program = build_program();
    let mut cases = HashMap::new();
    let entries: Vec<(&str, ir::Program, HashMap<String, Value>)> = vec![
        (
            "smoke",
            smoke_program,
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
        let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));
        cases.insert(
            name.to_string(),
            BenchmarkCase {
                dag,
                inputs,
                program_proto,
                ir_hash,
            },
        );
    }
    cases
}

fn build_instance(case: &BenchmarkCase, workflow_version_id: WorkflowVersionId) -> QueuedInstance {
    let mut state = RunnerState::from_dag(Arc::clone(&case.dag));
    for (name, value) in &case.inputs {
        let expr = literal_from_json_value(value);
        let label = format!("input {name} = {value}");
        let _ = state
            .record_assignment(vec![name.clone()], &expr, None, Some(label))
            .expect("record assignment");
    }
    let entry = case
        .dag
        .entry_node
        .clone()
        .expect("DAG entry node not found");
    let entry_exec = state
        .queue_template_node(&entry, None)
        .expect("queue entry node");
    QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        entry_node: entry_exec.node_id,
        state,
        action_results: HashMap::new(),
        instance_id: InstanceId::new_uuid_v4(),
        scheduled_at: None,
    }
}

async fn queue_benchmark_instances(
    backend: &PostgresBackend,
    cases: &HashMap<String, BenchmarkCase>,
    count_per_case: NonZeroUsize,
    batch_size: NonZeroUsize,
) -> usize {
    let mut version_ids = HashMap::new();
    for (name, case) in cases {
        let registration = WorkflowRegistration {
            workflow_name: name.clone(),
            workflow_version: case.ir_hash.clone(),
            ir_hash: case.ir_hash.clone(),
            program_proto: case.program_proto.clone(),
            concurrent: false,
        };
        let version_id = backend
            .upsert_workflow_version(&registration)
            .await
            .expect("register workflow version");
        version_ids.insert(name.clone(), version_id);
    }

    let mut case_names = Vec::new();
    for name in cases.keys() {
        for _ in 0..count_per_case.get() {
            case_names.push(name.clone());
        }
    }
    case_names.shuffle(&mut rand::rng());

    let mut queued = 0;
    let mut batch = Vec::new();
    for name in case_names {
        let case = cases.get(&name).expect("case");
        let version_id = *version_ids.get(&name).expect("workflow version");
        batch.push(build_instance(case, version_id));
        if batch.len() >= batch_size.get() {
            backend
                .queue_instances(&batch)
                .await
                .expect("queue instances");
            queued += batch.len();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        backend
            .queue_instances(&batch)
            .await
            .expect("queue instances");
        queued += batch.len();
    }
    queued
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
            _sqlx_migrations
        CASCADE
        "#,
    )
    .execute(pool)
    .await
    .expect("drop benchmark tables");
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
    batch_size: NonZeroUsize,
    dsn: &SecretStr,
    max_concurrent_instances: NonZeroUsize,
    executor_shards: NonZeroUsize,
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
    backend.clear_all().await.expect("clear all");
    let total = queue_benchmark_instances(&backend, &cases, count_per_case, batch_size).await;
    println!("Queued {total} instances across {} IR jobs", cases.len());

    let worker_pool = InlineWorkerPool::new(action_registry());
    let runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopConfig {
            max_concurrent_instances,
            executor_shards,
            instance_done_batch_size: None,
            poll_interval: Some(Duration::from_secs_f64(0.05).try_into().unwrap()),
            persistence_interval: Some(Duration::from_secs_f64(0.1).try_into().unwrap()),
            lock_uuid: LockId::new_uuid_v4(),
            lock_ttl: Duration::from_secs(15).try_into().unwrap(),
            lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
            evict_sleep_threshold: Duration::from_secs(10).try_into().unwrap(),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );
    let start = Instant::now();
    runloop.run().await.expect("runloop");
    let elapsed = start.elapsed();
    BenchmarkStats {
        elapsed,
        query_counts: backend.query_counts(),
        batch_counts: backend.batch_size_counts(),
    }
}

fn benchmark_max_concurrent() -> NonZeroUsize {
    env::var("WAYMARK_MAX_CONCURRENT_INSTANCES")
        .ok()
        .and_then(|value| value.trim().parse::<NonZeroUsize>().ok())
        .unwrap_or(DEFAULT_MAX_CONCURRENT_INSTANCES)
}

fn benchmark_executor_shards() -> NonZeroUsize {
    env::var("WAYMARK_EXECUTOR_SHARDS")
        .ok()
        .and_then(|value| value.trim().parse::<NonZeroUsize>().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().unwrap_or(1.try_into().unwrap()))
}

fn main() {
    let args = BenchmarkArgs::parse();
    if args.observe || args.trace.is_some() {
        waymark_observability_setup::init(waymark_observability_setup::ObservabilityOptions {
            console: args.observe,
            trace_path: args.trace.clone(),
        });
    }
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let _span = tracing::info_span!("benchmark_main").entered();
    let max_concurrent_instances = benchmark_max_concurrent();
    let executor_shards = benchmark_executor_shards();
    println!("max_concurrent_instances = {max_concurrent_instances}");
    println!("executor_shards = {executor_shards}");
    let stats = runtime.block_on(run_benchmark(
        args.count,
        args.base,
        args.batch_size,
        &args.dsn,
        max_concurrent_instances,
        executor_shards,
    ));
    println!("Benchmark completed in {:.2?}", stats.elapsed);
    println!("{}", format_query_counts(stats.query_counts));
    println!("{}", format_batch_size_counts(stats.batch_counts));
    if args.trace.is_some() {
        waymark_observability_setup::flush();
    }
}
