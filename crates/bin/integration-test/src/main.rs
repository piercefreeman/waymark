//! Fixture integration parity runner.
//!
//! For each curated fixture case:
//! 1. Ask a Python helper for ground-truth inline execution and compiled IR.
//! 2. Execute that IR through the VM over a Python worker pool — transiently
//!    (in-memory, no persistence) and durably (postgres-backed snapshots,
//!    action calls, and sleeps via the execution subsystem).
//! 3. Assert the VM workflow outcome matches inline Python output.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use waymark_convert_core::{Convert, TryConvert};
use waymark_proto::ast as ir;
use waymark_secret_string::SecretString;
use waymark_support_integration::{LOCAL_POSTGRES_DSN, connect_pool, ensure_local_postgres};
use waymark_worker_core::BaseWorkerPool as _;

#[derive(Parser, Debug)]
#[command(name = "integration_test")]
struct Args {
    /// Comma-separated execution mode list. Supported: transient,durable.
    #[arg(long, default_value = "transient,durable")]
    modes: String,

    /// Optional fixture case IDs to run.
    #[arg(long = "case")]
    cases: Vec<String>,

    /// Number of Python workers for VM execution.
    #[arg(long, default_value_t = 2.try_into().unwrap())]
    worker_count: NonZeroUsize,

    /// Timeout per case execution, in seconds.
    #[arg(long, default_value_t = 120)]
    timeout_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExecutionMode {
    Transient,
    Durable,
}

impl ExecutionMode {
    fn label(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Durable => "durable",
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
    ir_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
struct HelperOutput {
    expected: CaseOutcome,
    registration: HelperRegistration,
}

struct PreparedCase {
    case: FixtureCase,
    workflow_name: String,
    workflow_version: String,
    inputs: HashMap<String, waymark_system_vm::Value>,
    expected: CaseOutcome,
    program: waymark_vm_ast_old::Program,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct CaseOutcome {
    status: String,
    value: Value,
}

type PythonWorkerPool =
    Arc<waymark_worker_remote_pool::RemoteWorkerPool<waymark_worker_python::Spec>>;

#[tokio::main]
async fn main() -> Result<()> {
    waymark_fn_main_common::init()?;

    let args = Args::parse();
    let modes = parse_modes(&args.modes)?;
    let selected_cases = select_cases(&args.cases)?;
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
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

    let mut failures = Vec::new();
    let mut comparisons = 0usize;

    for mode in &modes {
        let mode_failures = match mode {
            ExecutionMode::Transient => {
                run_transient_mode(&repo_root, &prepared_cases, args.worker_count, timeout).await
            }
            ExecutionMode::Durable => {
                run_durable_mode(&repo_root, &prepared_cases, args.worker_count, timeout).await
            }
        }
        .with_context(|| format!("run {} execution mode", mode.label()))?;

        comparisons += prepared_cases.len();
        failures.extend(
            mode_failures
                .into_iter()
                .map(|failure| format!("mode={}\n{}", mode.label(), failure)),
        );
    }

    if !failures.is_empty() {
        eprintln!(
            "fixture integration parity failed: {} mismatches across {} comparisons",
            failures.len(),
            comparisons,
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
        "fixture integration parity passed: {} cases across {} mode comparisons",
        prepared_cases.len(),
        comparisons,
    );

    Ok(())
}

fn parse_modes(raw: &str) -> Result<Vec<ExecutionMode>> {
    let mut parsed = Vec::new();
    for item in raw.split(',') {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        match trimmed {
            "transient" => parsed.push(ExecutionMode::Transient),
            "durable" => parsed.push(ExecutionMode::Durable),
            other => bail!("unsupported execution mode '{other}'"),
        }
    }

    if parsed.is_empty() {
        bail!("no execution modes requested")
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
    let python = repo_root.join(".venv").join("bin").join("python");
    if python.exists() {
        Ok(python)
    } else {
        bail!(
            "python helper interpreter not found at {}; run 'uv sync'",
            python.display()
        )
    }
}

fn prepare_case(repo_root: &Path, case: FixtureCase) -> Result<PreparedCase> {
    let kwargs_value: Value = serde_json::from_str(case.kwargs_json)
        .with_context(|| format!("parse kwargs JSON for case '{}'", case.id))?;
    let Value::Object(kwargs) = kwargs_value else {
        bail!("case '{}' kwargs JSON must be an object", case.id)
    };

    let helper = run_python_helper(repo_root, &case)?;

    let program = <ir::Program as prost::Message>::decode(&helper.registration.ir_bytes[..])
        .with_context(|| {
            format!(
                "decode IR bytes for case '{}' ({})",
                case.id, case.workflow_class
            )
        })?;
    let program = waymark_vm_ast_old_proto::convert(program).with_context(|| {
        format!(
            "convert IR to the VM AST for case '{}' ({})",
            case.id, case.workflow_class
        )
    })?;

    let mut inputs = HashMap::new();
    for (name, value) in kwargs {
        let value: waymark_system_vm::Value =
            waymark_vm_value_convert_json::Converter::convert(value);
        inputs.insert(name, value);
    }

    Ok(PreparedCase {
        case,
        workflow_name: helper.registration.workflow_name,
        workflow_version: helper.registration.workflow_version,
        inputs,
        expected: canonicalize_outcome(helper.expected),
        program,
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

// ---------------------------------------------------------------------------
// Worker pool lifecycle (shared by both modes)
// ---------------------------------------------------------------------------

async fn setup_worker_pool(
    shutdown_token: tokio_util::sync::CancellationToken,
    repo_root: &Path,
    cases: &[PreparedCase],
    worker_count: NonZeroUsize,
) -> Result<(PythonWorkerPool, tokio::task::JoinHandle<()>)> {
    let mut modules = cases
        .iter()
        .map(|prepared| prepared.case.module_name.to_string())
        .collect::<Vec<_>>();
    modules.sort();
    modules.dedup();

    let config = waymark_worker_python::Config::new()
        .with_user_modules(modules)
        .with_python_paths(vec![
            repo_root.join("python"),
            repo_root.join("tests"),
            repo_root.join("tests/integration_tests"),
        ]);

    let (process_pool, bridge_server_task) = waymark_worker_remote_bringup::start(
        shutdown_token,
        None,
        |bridge_server_addr| waymark_worker_python::Spec {
            bridge_server_addr,
            config,
        },
        worker_count,
        None,
        10.try_into().unwrap(),
    )
    .await
    .context("create remote worker pool")?;

    let worker_pool = Arc::new(waymark_worker_remote_pool::RemoteWorkerPool::new(
        process_pool,
    ));

    Ok((worker_pool, bridge_server_task))
}

async fn teardown_worker_pool(
    shutdown_token: tokio_util::sync::CancellationToken,
    mut bridge_server_task: tokio::task::JoinHandle<()>,
    worker_pool: PythonWorkerPool,
) {
    shutdown_token.cancel();
    let bridge_server_shutdown =
        tokio::time::timeout(Duration::from_secs(5), &mut bridge_server_task).await;
    if bridge_server_shutdown.is_err() {
        tracing::warn!("bridge server did not stop in time, aborting it");
        bridge_server_task.abort();
        let _ = bridge_server_task.await;
    }

    if let Err(err) = worker_pool.shutdown_arc().await {
        eprintln!("failed to shutdown worker pool: {err}");
    }
}

// ---------------------------------------------------------------------------
// Transient execution mode
// ---------------------------------------------------------------------------

async fn run_transient_mode(
    repo_root: &Path,
    prepared_cases: &[PreparedCase],
    worker_count: NonZeroUsize,
    timeout: Duration,
) -> Result<Vec<String>> {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let (worker_pool, bridge_server_task) = setup_worker_pool(
        shutdown_token.clone(),
        repo_root,
        prepared_cases,
        worker_count,
    )
    .await
    .context("start transient worker pool")?;
    worker_pool
        .launch()
        .await
        .context("launch transient worker pool")?;

    let mut failures = Vec::new();
    for prepared in prepared_cases {
        let actual = run_case_transient(prepared, Arc::clone(&worker_pool), timeout).await;
        if let Some(mismatch) = check_case_outcome(prepared, actual) {
            failures.push(mismatch);
        }
    }

    teardown_worker_pool(shutdown_token, bridge_server_task, worker_pool).await;

    Ok(failures)
}

async fn run_case_transient(
    prepared: &PreparedCase,
    worker_pool: PythonWorkerPool,
    timeout: Duration,
) -> Result<CaseOutcome> {
    let runtime = waymark_transient_execution_bringup::setup_runtime(
        &prepared.program,
        prepared.inputs.clone(),
    )
    .with_context(|| format!("set up VM runtime for case '{}'", prepared.case.id))?;

    let cancel = tokio_util::sync::CancellationToken::new();
    let waymark_transient_execution_bringup::Execution {
        workflow_outcome_rx,
        driver_handle,
    } = waymark_transient_execution_worker_pool_bringup::execute(
        runtime,
        worker_pool,
        false,
        cancel.clone(),
    );

    let workflow_outcome = match tokio::time::timeout(timeout, workflow_outcome_rx).await {
        Ok(received) => received,
        Err(_elapsed) => {
            cancel.cancel();
            let Err(driver_exit) = driver_handle.await;
            tracing::debug!(?driver_exit, "vm driver exited after cancellation");
            bail!(
                "case '{}' timed out after {}s",
                prepared.case.id,
                timeout.as_secs()
            )
        }
    };

    // The driver terminates right after delivering the workflow outcome —
    // including on success — so join it unconditionally for its exit report.
    let Err(driver_exit) = driver_handle.await;
    tracing::debug!(?driver_exit, "vm driver exited");

    let workflow_outcome = workflow_outcome.map_err(|_recv_error| {
        anyhow!(
            "vm driver exited without delivering a workflow outcome for case '{}'",
            prepared.case.id
        )
    })?;

    Ok(canonicalize_outcome(outcome_from_vm(workflow_outcome)?))
}

// ---------------------------------------------------------------------------
// Durable execution mode
// ---------------------------------------------------------------------------

/// The postgres-backed services the durable mode drives directly: workflow
/// submission (compile + register) and outcome polling. Execution itself is
/// carried by the execution subsystem spawned in [`run_durable_mode`].
struct DurableStack {
    backend: waymark_backend_postgres::PostgresBackend,
    executables: waymark_workflow_service_vm_executables::ExecutablesService<
        waymark_backend_postgres::PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >,
    registration: waymark_workflow_service_vm_runtimes::RegistrationService<
        waymark_backend_postgres::PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
    >,
    outcome_polling: waymark_workflow_service_vm_runtimes::OutcomePollingService<
        waymark_backend_postgres::PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::ReadyValue,
    >,
}

async fn connect_durable_stack() -> Result<DurableStack> {
    let dsn = std::env::var("WAYMARK_DATABASE_URL")
        .map(SecretString::from)
        .unwrap_or_else(|_| SecretString::from(LOCAL_POSTGRES_DSN));

    if dsn.expose_secret() == LOCAL_POSTGRES_DSN.expose_secret() {
        ensure_local_postgres()
            .await
            .context("auto-bootstrap local postgres for integration runner")?;
    }

    let pool = connect_pool(&dsn)
        .await
        .with_context(|| format!("connect postgres backend: {dsn}"))?;
    waymark_backend_postgres_migrations::run(&pool)
        .await
        .context("run postgres migrations for integration runner")?;

    // Reset the durable-VM tables so stale runnable workloads from prior
    // (crashed) runs cannot be revived into this run's execution subsystem.
    sqlx::query(
        r#"
        TRUNCATE action_call_completions,
                 action_call_requests,
                 sleep_requests,
                 vm_executables,
                 vm_runtime_snapshots,
                 runnable_workloads,
                 vm_execution_results
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(&pool)
    .await
    .context("truncate durable-VM tables")?;

    let backend = waymark_backend_postgres::PostgresBackend::new(pool);
    let codec = waymark_vm_codec_rmp::RmpCodec;

    Ok(DurableStack {
        executables: waymark_workflow_service_vm_executables::ExecutablesService::new(
            backend.clone(),
            codec,
        ),
        registration: waymark_workflow_service_vm_runtimes::RegistrationService::new(
            backend.clone(),
            codec,
        ),
        outcome_polling: waymark_workflow_service_vm_runtimes::OutcomePollingService::new(
            backend.clone(),
            codec,
        ),
        backend,
    })
}

fn durable_execution_config() -> waymark_execution_bringup::Config<uuid::Uuid> {
    waymark_execution_bringup::Config {
        node_id: uuid::Uuid::new_v4(),
        action_effect_reconciler_lock_ttl: Duration::from_secs(15).try_into().unwrap(),
        action_effect_reconciler_lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
        max_pinned: 16.try_into().unwrap(),
        pinning_ttl: Duration::from_secs(15).try_into().unwrap(),
        pinning_heartbeat: Duration::from_secs(5).try_into().unwrap(),
        pinning_fencing_margin: Duration::from_secs(1).try_into().unwrap(),
        sleep_poll_interval: Duration::from_millis(250).try_into().unwrap(),
        vm_retention: Duration::from_secs(60).try_into().unwrap(),
        vm_sweep_interval: Duration::from_secs(10).try_into().unwrap(),
        executable_retention: Duration::from_secs(300).try_into().unwrap(),
        executable_sweep_interval: Duration::from_secs(60).try_into().unwrap(),
    }
}

async fn run_durable_mode(
    repo_root: &Path,
    prepared_cases: &[PreparedCase],
    worker_count: NonZeroUsize,
    timeout: Duration,
) -> Result<Vec<String>> {
    let stack = connect_durable_stack().await?;

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let force_shutdown_token = tokio_util::sync::CancellationToken::new();
    let (worker_pool, bridge_server_task) = setup_worker_pool(
        shutdown_token.clone(),
        repo_root,
        prepared_cases,
        worker_count,
    )
    .await
    .context("start durable worker pool")?;

    // The execution subsystem launches the worker pool itself.
    let execution_handles = waymark_execution_bringup::start(
        durable_execution_config(),
        Arc::new(stack.backend.clone()),
        Arc::clone(&worker_pool),
        shutdown_token.child_token(),
        force_shutdown_token.child_token(),
    )
    .await
    .context("start durable execution subsystem")?;

    let mut failures = Vec::new();
    for prepared in prepared_cases {
        let actual = run_case_durable(prepared, &stack, timeout).await;
        if let Some(mismatch) = check_case_outcome(prepared, actual) {
            failures.push(mismatch);
        }
    }

    // Every outcome has been received, so nothing is draining — force the
    // pinning manager out of its drain loop along with the graceful stop.
    shutdown_token.cancel();
    force_shutdown_token.cancel();
    shutdown_execution(execution_handles).await;
    teardown_worker_pool(shutdown_token, bridge_server_task, worker_pool).await;

    Ok(failures)
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

async fn run_case_durable(
    prepared: &PreparedCase,
    stack: &DurableStack,
    timeout: Duration,
) -> Result<CaseOutcome> {
    let (executable_id, executable, metadata) = stack
        .executables
        .compile_and_store(
            &prepared.workflow_name,
            &prepared.workflow_version,
            &prepared.program,
        )
        .await
        .map_err(|err| {
            anyhow!(
                "compile and store executable for case '{}': {err}",
                prepared.case.id
            )
        })?;

    let call_spec = waymark_vm_runtime_builder::builder(&metadata)
        .first_fn()
        .map_err(|err| {
            anyhow!(
                "select entry function for case '{}': {err}",
                prepared.case.id
            )
        })?
        .args(prepared.inputs.clone())
        .map_err(|err| {
            anyhow!(
                "match entry function arguments for case '{}': {err}",
                prepared.case.id
            )
        })?;

    let runtime = waymark_system_vm::Runtime::with_custom_entrypoint(
        waymark_system_vm::Interpreter::default(),
        Arc::new(executable),
        call_spec,
    )
    .map_err(|err| anyhow!("create VM runtime for case '{}': {err}", prepared.case.id))?;

    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    stack
        .registration
        .register_vm(vm_id, executable_id, |serializer| {
            runtime.snapshot(serializer)
        })
        .await
        .map_err(|err| anyhow!("register VM for case '{}': {err}", prepared.case.id))?;

    let workflow_outcome = tokio::time::timeout(
        timeout,
        stack
            .outcome_polling
            .wait_for_outcome(&vm_id, Duration::from_millis(100)),
    )
    .await
    .map_err(|_elapsed| {
        anyhow!(
            "case '{}' timed out after {}s",
            prepared.case.id,
            timeout.as_secs()
        )
    })?
    .map_err(|err| anyhow!("wait for outcome of case '{}': {err}", prepared.case.id))?;

    Ok(canonicalize_outcome(outcome_from_vm(workflow_outcome)?))
}

// ---------------------------------------------------------------------------
// Outcome comparison
// ---------------------------------------------------------------------------

fn check_case_outcome(prepared: &PreparedCase, actual: Result<CaseOutcome>) -> Option<String> {
    let mismatch = match actual {
        Ok(actual) if prepared.case.id == "timeout" => validate_timeout_outcome(&actual),
        Ok(actual) if actual != prepared.expected => Some(format!(
            "expected={}\nactual={}",
            serde_json::to_string(&prepared.expected).expect("serialize expected"),
            serde_json::to_string(&actual).expect("serialize actual"),
        )),
        Ok(_actual) => None,
        Err(err) => Some(format!("execution error: {err:#}")),
    };

    mismatch.map(|mismatch| format!("case={}\n{}", prepared.case.id, mismatch))
}

fn outcome_from_vm(
    outcome: waymark_workflow_completion_core::Outcome<waymark_system_vm::ReadyValue>,
) -> Result<CaseOutcome> {
    match outcome {
        waymark_workflow_completion_core::Outcome::Completion(value) => {
            let value: Value = waymark_vm_value_convert_json::Converter::try_convert(value)
                .context("convert workflow completion value to JSON")?;
            Ok(CaseOutcome {
                status: "ok".to_string(),
                value,
            })
        }
        waymark_workflow_completion_core::Outcome::Exception(exception) => {
            let value: Value = waymark_vm_value_convert_json::Converter::try_convert(exception)
                .context("convert workflow exception to JSON")?;
            Ok(CaseOutcome {
                status: "error".to_string(),
                value,
            })
        }
    }
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
