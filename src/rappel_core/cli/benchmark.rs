//! Benchmark CLI for running mixed IR workloads against Postgres.

use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::seq::SliceRandom;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::backends::{PostgresBackend, QueuedInstance};
use crate::db;
use crate::messages::ast as ir;
use crate::observability::obs;
use crate::rappel_core::cli::smoke::{
    build_control_flow_program, build_parallel_spread_program, build_program,
    build_try_except_program, build_while_loop_program, literal_from_value,
};
use crate::rappel_core::dag::{DAG, convert_to_dag};
use crate::rappel_core::runloop::RunLoop;
use crate::rappel_core::runner::RunnerState;
use crate::workers::{ActionCallable, InlineWorkerPool, WorkerPoolError};

const DEFAULT_DSN: &str = "postgresql://rappel:rappel@localhost:5432/rappel_core";
const DEFAULT_MAX_CONCURRENT_INSTANCES: usize = 25;

#[derive(Parser, Debug)]
#[command(
    name = "rappel-benchmark",
    about = "Benchmark mixed IR workloads against Postgres."
)]
struct BenchmarkArgs {
    #[arg(long, default_value_t = 10_000)]
    count: usize,
    #[arg(long, default_value_t = 5)]
    base: i64,
    #[arg(long, default_value_t = 250)]
    batch_size: usize,
    #[arg(long, default_value = DEFAULT_DSN)]
    dsn: String,
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
    dag: DAG,
    inputs: HashMap<String, Value>,
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
        let dag = convert_to_dag(&program).expect("convert to dag");
        cases.insert(name.to_string(), BenchmarkCase { dag, inputs });
    }
    cases
}

fn build_instance(case: &BenchmarkCase) -> QueuedInstance {
    let mut state = RunnerState::new(Some(case.dag.clone()), None, None, false);
    for (name, value) in &case.inputs {
        let expr = literal_from_value(value);
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
        dag: case.dag.clone(),
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id: Uuid::new_v4(),
    }
}

async fn queue_benchmark_instances(
    backend: &PostgresBackend,
    cases: &HashMap<String, BenchmarkCase>,
    count_per_case: usize,
    batch_size: usize,
) -> usize {
    let mut case_names = Vec::new();
    for name in cases.keys() {
        for _ in 0..count_per_case {
            case_names.push(name.clone());
        }
    }
    case_names.shuffle(&mut rand::thread_rng());

    let mut queued = 0;
    let mut batch = Vec::new();
    for name in case_names {
        batch.push(build_instance(cases.get(&name).expect("case")));
        if batch.len() >= batch_size {
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
            runner_instances_done,
            runner_instances,
            runner_actions_done,
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
    count_per_case: usize,
    base: i64,
    batch_size: usize,
    dsn: &str,
    max_concurrent_instances: usize,
    executor_shards: usize,
) -> BenchmarkStats {
    let cases = build_cases(base);
    let pool = PgPool::connect(dsn).await.expect("connect postgres");
    drop_benchmark_tables(&pool).await;
    db::run_migrations(&pool).await.expect("run migrations");
    let backend = PostgresBackend::new(pool);
    backend.clear_all().await.expect("clear all");
    let total = queue_benchmark_instances(&backend, &cases, count_per_case, batch_size).await;
    println!("Queued {total} instances across {} IR jobs", cases.len());

    let worker_pool = InlineWorkerPool::new(action_registry());
    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        max_concurrent_instances,
        None,
        Duration::from_secs_f64(0.05),
        Duration::from_secs_f64(0.1),
        executor_shards,
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

fn benchmark_max_concurrent() -> usize {
    env::var("RAPPEL_MAX_CONCURRENT_INSTANCES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_CONCURRENT_INSTANCES)
        .max(1)
}

fn benchmark_executor_shards() -> usize {
    env::var("RAPPEL_EXECUTOR_SHARDS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1)
        })
        .max(1)
}

pub fn main() {
    let args = BenchmarkArgs::parse();
    if args.observe || args.trace.is_some() {
        crate::observability::init(crate::observability::ObservabilityOptions {
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
        crate::observability::flush();
    }
}
