//! Fixture integration parity runner.
//!
//! For each curated fixture case:
//! 1. Ask a Python helper for ground-truth inline execution and compiled IR.
//! 2. Execute that IR through Rust runtime backends.
//! 3. Assert backend output matches inline Python output.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use waymark::backends::{
    CoreBackend, MemoryBackend, PostgresBackend, QueuedInstance, WorkflowRegistration,
    WorkflowRegistryBackend,
};
use waymark::db;
use waymark::integration_support::{LOCAL_POSTGRES_DSN, connect_pool, ensure_local_postgres};
use waymark::messages::ast as ir;
use waymark::waymark_core::runloop::{RunLoop, RunLoopSupervisorConfig};
use waymark::waymark_core::runner::RunnerState;
use waymark::workers::{PythonWorkerConfig, RemoteWorkerPool};
use waymark_dag::{DAG, convert_to_dag};

#[derive(Parser, Debug)]
#[command(name = "integration_test")]
struct Args {
    /// Comma-separated backend list. Supported: in-memory,postgres.
    #[arg(long, default_value = "in-memory,postgres")]
    backends: String,

    /// Optional fixture case IDs to run.
    #[arg(long = "case")]
    cases: Vec<String>,

    /// Number of Python workers for backend execution.
    #[arg(long, default_value_t = 2)]
    worker_count: usize,

    /// Timeout per backend execution, in seconds.
    #[arg(long, default_value_t = 120)]
    timeout_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendKind {
    InMemory,
    Postgres,
}

impl BackendKind {
    fn label(self) -> &'static str {
        match self {
            Self::InMemory => "in-memory",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Clone, Debug)]
struct FixtureCase {
    id: &'static str,
    module_name: &'static str,
    workflow_class: &'static str,
    kwargs_json: &'static str,
}

const CASES: &[FixtureCase] = &[
    FixtureCase {
        id: "simple",
        module_name: "simple_workflow",
        workflow_class: "SimpleWorkflow",
        kwargs_json: r#"{"name":"world"}"#,
    },
    FixtureCase {
        id: "sequential",
        module_name: "sequential_workflow",
        workflow_class: "SequentialWorkflow",
        kwargs_json: r#"{}"#,
    },
    FixtureCase {
        id: "conditional",
        module_name: "conditional_workflow",
        workflow_class: "ConditionalWorkflow",
        kwargs_json: r#"{"tier":"high"}"#,
    },
    FixtureCase {
        id: "immediate-conditional",
        module_name: "immediate_conditional_workflow",
        workflow_class: "ImmediateConditionalWorkflow",
        kwargs_json: r#"{"value":17}"#,
    },
    FixtureCase {
        id: "chain",
        module_name: "chain_workflow",
        workflow_class: "ChainWorkflow",
        kwargs_json: r#"{"text":"hello"}"#,
    },
    FixtureCase {
        id: "for-loop",
        module_name: "for_loop_workflow",
        workflow_class: "ForLoopWorkflow",
        kwargs_json: r#"{"items":["alpha","beta","gamma"]}"#,
    },
    FixtureCase {
        id: "parallel",
        module_name: "parallel_workflow",
        workflow_class: "ParallelWorkflow",
        kwargs_json: r#"{"value":7}"#,
    },
    FixtureCase {
        id: "gather-listcomp",
        module_name: "integration_gather_listcomp",
        workflow_class: "GatherListCompWorkflow",
        kwargs_json: r#"{"items":[1,2,3]}"#,
    },
    FixtureCase {
        id: "tuple-unpack-fn-call",
        module_name: "integration_tuple_unpack_fn_call",
        workflow_class: "TupleUnpackFnCallWorkflow",
        kwargs_json: r#"{"user_id":"user_42"}"#,
    },
    FixtureCase {
        id: "nested-conditionals",
        module_name: "integration_nested_conditionals",
        workflow_class: "NestedConditionalsWorkflow",
        kwargs_json: r#"{"user_id":"user_c"}"#,
    },
    FixtureCase {
        id: "data-pipeline",
        module_name: "integration_data_pipeline",
        workflow_class: "DataPipelineWorkflow",
        kwargs_json: r#"{"source":"sales","threshold":100}"#,
    },
    FixtureCase {
        id: "string-processing",
        module_name: "integration_string_processing",
        workflow_class: "StringProcessingWorkflow",
        kwargs_json: r#"{"text":"Alpha123"}"#,
    },
    FixtureCase {
        id: "timeout",
        module_name: "integration_timeout_workflow",
        workflow_class: "TimeoutWorkflow",
        kwargs_json: r#"{}"#,
    },
];

#[derive(Clone, Debug, Deserialize)]
struct HelperRegistration {
    workflow_name: String,
    workflow_version: String,
    ir_hash: String,
    concurrent: bool,
    ir_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
struct HelperOutput {
    expected: CaseOutcome,
    registration: HelperRegistration,
}

#[derive(Clone, Debug)]
struct PreparedCase {
    case: FixtureCase,
    kwargs: HashMap<String, Value>,
    expected: CaseOutcome,
    registration: WorkflowRegistration,
    program: ir::Program,
    dag: Arc<DAG>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct CaseOutcome {
    status: String,
    value: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let backend_kinds = parse_backends(&args.backends)?;
    let selected_cases = select_cases(&args.cases)?;
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let timeout = Duration::from_secs(args.timeout_seconds);

    if selected_cases.is_empty() {
        bail!("no fixture cases selected");
    }

    let mut prepared_cases = Vec::new();
    for case in selected_cases {
        prepared_cases.push(prepare_case(&repo_root, case.clone()).with_context(|| {
            format!(
                "prepare fixture case '{}' ({}::{})",
                case.id, case.module_name, case.workflow_class
            )
        })?);
    }

    let maybe_postgres_backend = if backend_kinds.contains(&BackendKind::Postgres) {
        Some(connect_postgres_backend().await?)
    } else {
        None
    };

    let worker_pool = setup_worker_pool(&repo_root, &prepared_cases, args.worker_count)
        .await
        .context("start integration worker pool")?;

    let mut failures = Vec::new();
    let mut comparisons = 0usize;

    for prepared in &prepared_cases {
        for backend_kind in &backend_kinds {
            let actual = match backend_kind {
                BackendKind::InMemory => {
                    run_case_in_memory(prepared, worker_pool.clone(), timeout).await?
                }
                BackendKind::Postgres => {
                    let backend = maybe_postgres_backend
                        .as_ref()
                        .context("postgres backend requested but not initialized")?;
                    run_case_postgres(prepared, backend, worker_pool.clone(), timeout).await?
                }
            };
            comparisons += 1;

            let mismatch = if prepared.case.id == "timeout" {
                validate_timeout_outcome(&actual)
            } else if actual != prepared.expected {
                Some(format!(
                    "expected={}\nactual={}",
                    serde_json::to_string(&prepared.expected).expect("serialize expected"),
                    serde_json::to_string(&actual).expect("serialize actual"),
                ))
            } else {
                None
            };

            if let Some(mismatch) = mismatch {
                failures.push(format!(
                    "case={} backend={}\n{}",
                    prepared.case.id,
                    backend_kind.label(),
                    mismatch,
                ));
            }
        }
    }

    if let Err(err) = worker_pool.shutdown().await {
        eprintln!("failed to shutdown worker pool: {err}");
    }

    if !failures.is_empty() {
        eprintln!(
            "fixture integration parity failed: {} mismatches across {} comparisons",
            failures.len(),
            comparisons
        );
        for failure in failures {
            eprintln!(
                "--------------------------------------------------------------------------------"
            );
            eprintln!("{failure}");
        }
        std::process::exit(1);
    }

    println!(
        "fixture integration parity passed: {} cases across {} backend comparisons",
        prepared_cases.len(),
        comparisons
    );
    Ok(())
}

fn parse_backends(raw: &str) -> Result<Vec<BackendKind>> {
    let mut parsed = Vec::new();
    for item in raw.split(',') {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        match trimmed {
            "in-memory" => parsed.push(BackendKind::InMemory),
            "postgres" => parsed.push(BackendKind::Postgres),
            other => bail!("unsupported backend '{other}'"),
        }
    }

    if parsed.is_empty() {
        bail!("no backends requested")
    }

    Ok(parsed)
}

fn select_cases(filters: &[String]) -> Result<Vec<FixtureCase>> {
    if filters.is_empty() {
        return Ok(CASES.to_vec());
    }

    let mut selected = Vec::new();
    for filter in filters {
        let Some(case) = CASES.iter().find(|candidate| candidate.id == filter) else {
            bail!("unknown fixture case '{filter}'")
        };
        selected.push(case.clone());
    }
    Ok(selected)
}

fn helper_python(repo_root: &Path) -> Result<PathBuf> {
    let python = repo_root
        .join("python")
        .join(".venv")
        .join("bin")
        .join("python");
    if python.exists() {
        Ok(python)
    } else {
        bail!(
            "python helper interpreter not found at {}; run 'cd python && uv sync'",
            python.display()
        )
    }
}

fn prepare_case(repo_root: &Path, case: FixtureCase) -> Result<PreparedCase> {
    let kwargs_value: Value = serde_json::from_str(case.kwargs_json)
        .with_context(|| format!("parse kwargs JSON for case '{}'", case.id))?;
    let kwargs = match kwargs_value {
        Value::Object(map) => map.into_iter().collect::<HashMap<String, Value>>(),
        _ => bail!("case '{}' kwargs JSON must be an object", case.id),
    };

    let helper = run_python_helper(repo_root, &case)?;

    let program = ir::Program::decode(&helper.registration.ir_bytes[..]).with_context(|| {
        format!(
            "decode IR bytes for case '{}' ({})",
            case.id, case.workflow_class
        )
    })?;

    let dag = Arc::new(
        convert_to_dag(&program).map_err(|err| anyhow!("convert DAG for {}: {}", case.id, err))?,
    );

    let registration = WorkflowRegistration {
        workflow_name: helper.registration.workflow_name,
        workflow_version: helper.registration.workflow_version,
        ir_hash: helper.registration.ir_hash,
        program_proto: helper.registration.ir_bytes,
        concurrent: helper.registration.concurrent,
    };

    Ok(PreparedCase {
        case,
        kwargs,
        expected: canonicalize_outcome(helper.expected),
        registration,
        program,
        dag,
    })
}

fn run_python_helper(repo_root: &Path, case: &FixtureCase) -> Result<HelperOutput> {
    let helper_script = repo_root.join("scripts").join("fixture_ground_truth.py");
    let python = helper_python(repo_root)?;

    let output = Command::new(python)
        .arg(&helper_script)
        .arg("--module")
        .arg(case.module_name)
        .arg("--workflow-class")
        .arg(case.workflow_class)
        .arg("--kwargs-json")
        .arg(case.kwargs_json)
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("run python helper for case '{}'", case.id))?;

    if !output.status.success() {
        bail!(
            "python helper failed for case '{}'\nstdout:\n{}\nstderr:\n{}",
            case.id,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("decode python helper stdout for case '{}'", case.id))?;
    let payload = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .with_context(|| format!("python helper produced no payload for case '{}'", case.id))?;

    serde_json::from_str(payload)
        .with_context(|| format!("parse python helper JSON payload for case '{}'", case.id))
}

async fn setup_worker_pool(
    repo_root: &Path,
    cases: &[PreparedCase],
    worker_count: usize,
) -> Result<RemoteWorkerPool> {
    let mut modules = cases
        .iter()
        .map(|prepared| prepared.case.module_name.to_string())
        .collect::<Vec<_>>();
    modules.sort();
    modules.dedup();

    let config = PythonWorkerConfig::new()
        .with_user_modules(modules)
        .with_python_paths(vec![
            repo_root.to_path_buf(),
            repo_root.join("tests"),
            repo_root.join("tests/integration_tests"),
        ]);

    RemoteWorkerPool::new_with_config(config, worker_count.max(1), None, None, 10)
        .await
        .context("create remote worker pool")
}

async fn connect_postgres_backend() -> Result<PostgresBackend> {
    let dsn =
        std::env::var("WAYMARK_DATABASE_URL").unwrap_or_else(|_| LOCAL_POSTGRES_DSN.to_string());

    if dsn == LOCAL_POSTGRES_DSN {
        ensure_local_postgres()
            .await
            .context("auto-bootstrap local postgres for integration runner")?;
    }

    let pool = connect_pool(&dsn)
        .await
        .with_context(|| format!("connect postgres backend: {dsn}"))?;
    db::run_migrations(&pool)
        .await
        .context("run postgres migrations for integration runner")?;
    Ok(PostgresBackend::new(pool))
}

async fn run_case_in_memory(
    prepared: &PreparedCase,
    worker_pool: RemoteWorkerPool,
    timeout: Duration,
) -> Result<CaseOutcome> {
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue);

    let workflow_version_id =
        WorkflowRegistryBackend::upsert_workflow_version(&backend, &prepared.registration)
            .await
            .map_err(|err| anyhow!(err.to_string()))
            .context("register workflow version in memory backend")?;

    let instance_id = Uuid::new_v4();
    let queued = build_queued_instance(
        instance_id,
        workflow_version_id,
        Arc::clone(&prepared.dag),
        &prepared.kwargs,
    )?;

    CoreBackend::queue_instances(&backend, &[queued])
        .await
        .map_err(|err| anyhow!(err.to_string()))
        .context("queue in-memory instance")?;

    run_runloop(worker_pool, backend.clone(), timeout).await?;

    let done = backend
        .instances_done()
        .into_iter()
        .find(|candidate| candidate.executor_id == instance_id)
        .with_context(|| {
            format!(
                "missing completed in-memory instance for case '{}'",
                prepared.case.id
            )
        })?;

    Ok(canonicalize_outcome(outcome_from_payload(
        &prepared.program,
        done.result,
        done.error,
    )))
}

async fn run_case_postgres(
    prepared: &PreparedCase,
    backend: &PostgresBackend,
    worker_pool: RemoteWorkerPool,
    timeout: Duration,
) -> Result<CaseOutcome> {
    backend
        .clear_all()
        .await
        .map_err(|err| anyhow!(err.to_string()))
        .context("clear postgres runner tables")?;

    let workflow_version_id =
        WorkflowRegistryBackend::upsert_workflow_version(backend, &prepared.registration)
            .await
            .map_err(|err| anyhow!(err.to_string()))
            .context("register workflow version in postgres backend")?;

    let instance_id = Uuid::new_v4();
    let queued = build_queued_instance(
        instance_id,
        workflow_version_id,
        Arc::clone(&prepared.dag),
        &prepared.kwargs,
    )?;

    CoreBackend::queue_instances(backend, &[queued])
        .await
        .map_err(|err| anyhow!(err.to_string()))
        .context("queue postgres instance")?;

    run_runloop(worker_pool, backend.clone(), timeout).await?;

    let row = sqlx::query("SELECT result, error FROM runner_instances WHERE instance_id = $1")
        .bind(instance_id)
        .fetch_optional(backend.pool())
        .await
        .context("fetch postgres instance outcome row")?
        .with_context(|| {
            format!(
                "missing runner_instances row for case '{}' instance {}",
                prepared.case.id, instance_id
            )
        })?;

    let result_payload: Option<Vec<u8>> = row.get("result");
    let error_payload: Option<Vec<u8>> = row.get("error");

    let result = result_payload
        .as_deref()
        .map(rmp_serde::from_slice::<Value>)
        .transpose()
        .context("decode postgres result payload")?;
    let error = error_payload
        .as_deref()
        .map(rmp_serde::from_slice::<Value>)
        .transpose()
        .context("decode postgres error payload")?;

    Ok(canonicalize_outcome(outcome_from_payload(
        &prepared.program,
        result,
        error,
    )))
}

async fn run_runloop<B>(worker_pool: RemoteWorkerPool, backend: B, timeout: Duration) -> Result<()>
where
    B: CoreBackend + WorkflowRegistryBackend + Clone + Send + Sync + 'static,
{
    let mut runloop = RunLoop::new(
        worker_pool,
        backend,
        RunLoopSupervisorConfig {
            max_concurrent_instances: 16,
            executor_shards: 1,
            instance_done_batch_size: Some(16),
            poll_interval: Duration::from_millis(10),
            persistence_interval: Duration::from_millis(20),
            lock_uuid: Uuid::new_v4(),
            lock_ttl: Duration::from_secs(15),
            lock_heartbeat: Duration::from_secs(5),
            evict_sleep_threshold: Duration::from_secs(10),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );

    tokio::time::timeout(timeout, runloop.run())
        .await
        .with_context(|| format!("runloop timed out after {}s", timeout.as_secs()))??;

    Ok(())
}

fn build_queued_instance(
    instance_id: Uuid,
    workflow_version_id: Uuid,
    dag: Arc<DAG>,
    kwargs: &HashMap<String, Value>,
) -> Result<QueuedInstance> {
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);

    for (name, value) in kwargs {
        let expr = literal_from_value(value);
        let label = format!("input {name} = {value}");
        let _ = state
            .record_assignment(vec![name.clone()], &expr, None, Some(label))
            .map_err(|err| anyhow!(err.0))?;
    }

    let entry_template = dag
        .entry_node
        .clone()
        .ok_or_else(|| anyhow!("DAG entry node not found"))?;
    let entry_exec = state
        .queue_template_node(&entry_template, None)
        .map_err(|err| anyhow!(err.0))?;

    Ok(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    })
}

fn literal_from_value(value: &Value) -> ir::Expr {
    match value {
        Value::Bool(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::BoolValue(*value)),
            })),
            span: None,
        },
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(value)),
                    })),
                    span: None,
                }
            } else {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::FloatValue(
                            number.as_f64().unwrap_or(0.0),
                        )),
                    })),
                    span: None,
                }
            }
        }
        Value::String(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::StringValue(value.clone())),
            })),
            span: None,
        },
        Value::Array(items) => ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: items.iter().map(literal_from_value).collect(),
            })),
            span: None,
        },
        Value::Object(map) => {
            let entries = map
                .iter()
                .map(|(key, value)| ir::DictEntry {
                    key: Some(literal_from_value(&Value::String(key.clone()))),
                    value: Some(literal_from_value(value)),
                })
                .collect();
            ir::Expr {
                kind: Some(ir::expr::Kind::Dict(ir::DictExpr { entries })),
                span: None,
            }
        }
        Value::Null => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IsNone(true)),
            })),
            span: None,
        },
    }
}

fn outcome_from_payload(
    program: &ir::Program,
    result: Option<Value>,
    error: Option<Value>,
) -> CaseOutcome {
    if let Some(error) = error {
        return CaseOutcome {
            status: "error".to_string(),
            value: error,
        };
    }

    let raw_result = result.unwrap_or(Value::Null);
    CaseOutcome {
        status: "ok".to_string(),
        value: normalize_runtime_result(program, raw_result),
    }
}

fn normalize_runtime_result(program: &ir::Program, result: Value) -> Value {
    let Value::Object(map) = result else {
        return result;
    };

    let Some(main_fn) = program.functions.first() else {
        return Value::Object(map);
    };

    if let Some(io) = &main_fn.io {
        for output_name in &io.outputs {
            if let Some(value) = map.get(output_name) {
                return value.clone();
            }
        }
    }

    if let Some(value) = map.get("result") {
        return value.clone();
    }

    if map.len() == 1
        && let Some(value) = map.values().next()
    {
        return value.clone();
    }

    Value::Object(map)
}

fn canonicalize_outcome(outcome: CaseOutcome) -> CaseOutcome {
    CaseOutcome {
        status: outcome.status,
        value: canonicalize_json(outcome.value),
    }
}

fn validate_timeout_outcome(actual: &CaseOutcome) -> Option<String> {
    if actual.status != "error" {
        return Some(format!(
            "expected timeout status=error\nactual={}",
            serde_json::to_string(actual).expect("serialize actual")
        ));
    }

    let Value::Object(payload) = &actual.value else {
        return Some(format!(
            "expected timeout payload object\nactual={}",
            serde_json::to_string(actual).expect("serialize actual")
        ));
    };

    let error_type = payload.get("type").and_then(Value::as_str);
    if error_type != Some("ActionTimeout") {
        return Some(format!(
            "expected error type ActionTimeout\nactual={}",
            serde_json::to_string(actual).expect("serialize actual")
        ));
    }

    let timeout_seconds = payload.get("timeout_seconds").and_then(Value::as_i64);
    if timeout_seconds != Some(1) {
        return Some(format!(
            "expected timeout_seconds=1\nactual={}",
            serde_json::to_string(actual).expect("serialize actual")
        ));
    }

    let attempt = payload.get("attempt").and_then(Value::as_i64);
    if attempt != Some(1) {
        return Some(format!(
            "expected attempt=1\nactual={}",
            serde_json::to_string(actual).expect("serialize actual")
        ));
    }

    None
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize_json).collect()),
        Value::Object(map) => {
            let mut ordered = BTreeMap::new();
            for (key, item) in map {
                ordered.insert(key, canonicalize_json(item));
            }
            let mut normalized = serde_json::Map::new();
            for (key, item) in ordered {
                normalized.insert(key, item);
            }
            Value::Object(normalized)
        }
        other => other,
    }
}
