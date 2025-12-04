//! Benchmark binary for measuring Rappel throughput.
//!
//! This binary runs the benchmark workflow with configurable parameters
//! and outputs timing/throughput metrics as JSON.

use std::{
    collections::HashMap,
    env, fs,
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use prost::Message;
use serde::Serialize;
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::{Level, error, info};

use rappel::{
    ActionDispatchPayload, Database, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics,
    WorkerBridgeServer, WorkflowInstanceId, WorkflowVersionId, proto,
};

const BENCHMARK_WORKFLOW_MODULE: &str = include_str!("../../tests/fixtures/benchmark_workflow.py");

// ============================================================================
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "benchmark", about = "Run Rappel benchmarks")]
struct Args {
    /// Output results as JSON
    #[arg(long, default_value = "false")]
    json: bool,

    /// Number of parallel hash computations (indices 0..count)
    #[arg(long, default_value = "16")]
    count: u32,

    /// Hash iterations per action (CPU intensity)
    #[arg(long, default_value = "100")]
    iterations: u32,

    /// Number of Python workers
    #[arg(long, default_value = "4")]
    workers: u32,

    /// Log interval (0 = no logging during run)
    #[arg(long, default_value = "0")]
    log_interval: u32,

    /// Timeout in seconds
    #[arg(long, default_value = "300")]
    timeout: u64,
}

// ============================================================================
// Output Format
// ============================================================================

#[derive(Serialize, Debug)]
struct BenchmarkOutput {
    /// Total number of actions executed
    total: u64,
    /// Total elapsed time in seconds
    elapsed_s: f64,
    /// Actions per second
    throughput: f64,
    /// Average round-trip time in milliseconds
    avg_round_trip_ms: f64,
    /// P95 round-trip time in milliseconds
    p95_round_trip_ms: f64,
}

// ============================================================================
// gRPC Service
// ============================================================================

struct BenchmarkWorkflowService {
    database: Database,
}

impl BenchmarkWorkflowService {
    fn new(database: Database) -> Self {
        Self { database }
    }
}

#[tonic::async_trait]
impl proto::workflow_service_server::WorkflowService for BenchmarkWorkflowService {
    async fn register_workflow(
        &self,
        request: tonic::Request<proto::RegisterWorkflowRequest>,
    ) -> Result<tonic::Response<proto::RegisterWorkflowResponse>, tonic::Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| tonic::Status::invalid_argument("registration missing"))?;

        let version_id = self
            .database
            .upsert_workflow_version(
                &registration.workflow_name,
                &registration.ir_hash,
                &registration.ir,
                registration.concurrent,
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {}", e)))?;

        let initial_input = registration.initial_context.map(|ctx| ctx.encode_to_vec());
        let instance_id = self
            .database
            .create_instance(
                &registration.workflow_name,
                version_id,
                initial_input.as_deref(),
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {}", e)))?;

        Ok(tonic::Response::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_id: instance_id.to_string(),
        }))
    }

    async fn wait_for_instance(
        &self,
        _request: tonic::Request<proto::WaitForInstanceRequest>,
    ) -> Result<tonic::Response<proto::WaitForInstanceResponse>, tonic::Status> {
        // Not used in benchmarks
        Err(tonic::Status::unimplemented("not implemented"))
    }
}

async fn start_grpc_server(
    database: Database,
) -> Result<(SocketAddr, oneshot::Sender<()>, JoinHandle<()>)> {
    use proto::workflow_service_server::WorkflowServiceServer;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = BenchmarkWorkflowService::new(database);

    let handle = tokio::spawn(async move {
        let shutdown = async move {
            let _ = shutdown_rx.await;
        };
        let _ = Server::builder()
            .add_service(WorkflowServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await;
    });

    Ok((addr, shutdown_tx, handle))
}

// ============================================================================
// Python Environment
// ============================================================================

async fn setup_python_env(grpc_addr: SocketAddr, count: u32, iterations: u32) -> Result<TempDir> {
    let env_dir = TempDir::new().context("create python env dir")?;

    // Write workflow module
    fs::write(
        env_dir.path().join("benchmark_workflow.py"),
        BENCHMARK_WORKFLOW_MODULE.trim_start(),
    )?;

    // Write registration script
    // indices is a list of integers [0, 1, 2, ..., count-1]
    let register_script = format!(
        r#"
import asyncio
import os

from benchmark_workflow import BenchmarkFanOutWorkflow

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = BenchmarkFanOutWorkflow()
    indices = list(range({count}))
    result = await wf.run(indices=indices, iterations={iterations})
    print(f"Registration result: {{result}}")

asyncio.run(main())
"#
    );
    fs::write(
        env_dir.path().join("register.py"),
        register_script.trim_start(),
    )?;

    // Set up pyproject.toml
    let repo_python = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .canonicalize()?;

    let pyproject = format!(
        r#"[project]
name = "rappel-benchmark"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "rappel @ file://{}"
]
"#,
        repo_python.display()
    );
    fs::write(env_dir.path().join("pyproject.toml"), pyproject)?;

    // Run uv sync
    run_shell(env_dir.path(), "uv sync", &[]).await?;

    // Build PYTHONPATH
    let mut python_paths = vec![
        repo_python.join("src"),
        repo_python.join("proto"),
        repo_python.clone(),
    ];
    if let Some(existing) = env::var_os("PYTHONPATH") {
        python_paths.extend(env::split_paths(&existing));
    }
    let pythonpath = env::join_paths(&python_paths)
        .context("failed to join python path entries")?
        .into_string()
        .map_err(|_| anyhow!("python path contains invalid unicode"))?;

    let env_vars = vec![
        ("PYTHONPATH", pythonpath),
        ("CARABINER_SERVER_PORT", "9999".to_string()),
        ("CARABINER_GRPC_ADDR", grpc_addr.to_string()),
        ("CARABINER_SKIP_WAIT_FOR_INSTANCE", "1".to_string()),
    ];

    // Run registration
    run_shell_with_env(env_dir.path(), "uv run python register.py", &env_vars).await?;

    Ok(env_dir)
}

async fn run_shell(cwd: &std::path::Path, command: &str, envs: &[(&str, String)]) -> Result<()> {
    run_shell_with_env(cwd, command, envs).await
}

async fn run_shell_with_env(
    cwd: &std::path::Path,
    command: &str,
    envs: &[(&str, String)],
) -> Result<()> {
    let mut cmd = Command::new("bash");
    cmd.arg("-lc")
        .arg(command)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    for (key, value) in envs {
        cmd.env(key, value);
    }

    let status = cmd
        .spawn()
        .with_context(|| format!("failed to spawn `{command}`"))?
        .wait()
        .await?;

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("command `{command}` failed with {status}"))
    }
}

// ============================================================================
// Action Dispatch with DAG Runner Integration
// ============================================================================

/// Run the DAG runner and collect metrics from completed actions.
///
/// Unlike the simple dispatch loop, this uses the full DAGRunner which:
/// 1. Fetches actions from the queue
/// 2. Dispatches to workers
/// 3. On completion, traverses the DAG to find and enqueue next actions
async fn run_with_dag_runner(
    runner: &rappel::DAGRunner,
    database: &Arc<Database>,
    pool: &Arc<PythonWorkerPool>,
    timeout: Duration,
) -> Result<Vec<RoundTripMetrics>> {
    let mut completed = Vec::new();
    let start = std::time::Instant::now();

    // Run the dispatch loop
    let mut idle_cycles = 0usize;
    let max_idle_cycles = 20; // Wait longer since DAG progression takes time

    loop {
        // Check timeout
        if start.elapsed() > timeout {
            info!("Benchmark timeout reached");
            break;
        }

        // Dispatch actions
        let actions = database.dispatch_actions(64).await?;

        if actions.is_empty() {
            idle_cycles = idle_cycles.saturating_add(1);
            if idle_cycles >= max_idle_cycles && !completed.is_empty() {
                // Check if there are any pending actions in the queue
                let pending: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM action_queue WHERE status IN ('pending', 'running')",
                )
                .fetch_one(database.pool())
                .await?;

                if pending == 0 {
                    info!("No more pending actions, benchmark complete");
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }

        idle_cycles = 0;

        for action in actions {
            let kwargs = if action.dispatch_payload.is_empty() {
                proto::WorkflowArguments { arguments: vec![] }
            } else {
                let json: serde_json::Value = serde_json::from_slice(&action.dispatch_payload)
                    .context("failed to parse dispatch payload")?;
                json_to_workflow_args(&json)
            };

            let payload = ActionDispatchPayload {
                action_id: action.id.to_string(),
                instance_id: action.instance_id.to_string(),
                sequence: action.action_seq as u32,
                action_name: action.action_name.clone(),
                module_name: action.module_name.clone(),
                kwargs,
                timeout_seconds: action.timeout_seconds as u32,
                max_retries: action.max_retries as u32,
                attempt_number: action.attempt_number as u32,
                dispatch_token: action.delivery_token,
            };

            let worker = pool.next_worker();
            let metrics = worker
                .send_action(payload)
                .await
                .map_err(|e| anyhow!("worker send failed: {}", e))?;

            // Process the completion through the runner to trigger DAG progression
            let completion_result = runner
                .process_action_completion(
                    rappel::ActionId(action.id),
                    WorkflowInstanceId(action.instance_id),
                    action.node_id.clone(),
                    metrics.success,
                    metrics.response_payload.clone(),
                    metrics.error_message.clone(),
                    action.delivery_token,
                )
                .await;

            if let Err(e) = completion_result {
                error!("Failed to process completion: {}", e);
            }

            completed.push(metrics);
        }
    }

    Ok(completed)
}

fn json_to_workflow_args(json: &serde_json::Value) -> proto::WorkflowArguments {
    match json {
        serde_json::Value::Object(obj) => {
            let arguments = obj
                .iter()
                .map(|(k, v)| proto::WorkflowArgument {
                    key: k.clone(),
                    value: Some(json_to_workflow_value(v)),
                })
                .collect();
            proto::WorkflowArguments { arguments }
        }
        _ => proto::WorkflowArguments { arguments: vec![] },
    }
}

fn json_to_workflow_value(value: &serde_json::Value) -> proto::WorkflowArgumentValue {
    let kind = match value {
        serde_json::Value::Null => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::NullValue(0)),
            })
        }
        serde_json::Value::Bool(b) => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::BoolValue(*b)),
            })
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::IntValue(i)),
                })
            } else if let Some(f) = n.as_f64() {
                proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::DoubleValue(f)),
                })
            } else {
                proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::DoubleValue(0.0)),
                })
            }
        }
        serde_json::Value::String(s) => {
            proto::workflow_argument_value::Kind::Primitive(proto::PrimitiveWorkflowArgument {
                kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                    s.clone(),
                )),
            })
        }
        serde_json::Value::Array(arr) => {
            let items = arr.iter().map(json_to_workflow_value).collect();
            proto::workflow_argument_value::Kind::ListValue(proto::WorkflowListArgument { items })
        }
        serde_json::Value::Object(obj) => {
            let entries = obj
                .iter()
                .map(|(k, v)| proto::WorkflowArgument {
                    key: k.clone(),
                    value: Some(json_to_workflow_value(v)),
                })
                .collect();
            proto::workflow_argument_value::Kind::DictValue(proto::WorkflowDictArgument { entries })
        }
    };

    proto::WorkflowArgumentValue { kind: Some(kind) }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    if args.log_interval > 0 {
        tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    }

    // Connect to database
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://mountaineer:mountaineer@localhost:5432/mountaineer_daemons".to_string()
    });
    let database = Arc::new(Database::connect(&database_url).await?);

    // Clean up database
    sqlx::query("TRUNCATE action_queue, instance_context, loop_state, workflow_instances, workflow_versions CASCADE")
        .execute(database.pool())
        .await?;

    // Start gRPC server
    let (grpc_addr, grpc_shutdown, _grpc_handle) = start_grpc_server((*database).clone()).await?;
    info!(%grpc_addr, "gRPC server started");

    // Start worker bridge
    let worker_bridge = WorkerBridgeServer::start(None).await?;
    info!(addr = %worker_bridge.addr(), "worker bridge started");

    // Set up Python environment and run registration
    let python_env = setup_python_env(grpc_addr, args.count, args.iterations).await?;
    info!("workflow registered");

    // Find the workflow version and instance
    let versions = database.list_workflow_versions().await?;
    let version = versions
        .iter()
        .find(|v| v.workflow_name == "benchmarkfanoutworkflow")
        .context("benchmark workflow not found")?;
    let version_id = WorkflowVersionId(version.id);

    let instances: Vec<rappel::WorkflowInstance> = sqlx::query_as(
        "SELECT id, partition_id, workflow_name, workflow_version_id, \
         next_action_seq, input_payload, result_payload, status, \
         created_at, completed_at \
         FROM workflow_instances WHERE workflow_version_id = $1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(version_id.0)
    .fetch_all(database.pool())
    .await?;

    let instance_id = instances
        .first()
        .map(|i| WorkflowInstanceId(i.id))
        .context("no instance found")?;

    // Start worker pool
    let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .join(".venv")
        .join("bin")
        .join("rappel-worker");

    let worker_config = PythonWorkerConfig {
        script_path: worker_script,
        script_args: Vec::new(),
        user_modules: vec!["benchmark_workflow".to_string()],
        extra_python_paths: vec![python_env.path().to_path_buf()],
    };

    let worker_pool = Arc::new(
        PythonWorkerPool::new(
            worker_config,
            args.workers as usize,
            Arc::clone(&worker_bridge),
        )
        .await?,
    );
    info!(workers = args.workers, "worker pool ready");

    // Enqueue initial actions (this would normally be done by DAGRunner)
    // For the benchmark, we need to manually start the workflow
    let runner_config = rappel::RunnerConfig {
        batch_size: 64,
        max_slots_per_worker: 10,
        poll_interval_ms: 10,
        timeout_check_interval_ms: 1000,
    };
    let runner = rappel::DAGRunner::new(
        runner_config,
        Arc::clone(&database),
        Arc::clone(&worker_pool),
    );

    // Start the instance
    runner.start_instance(instance_id, HashMap::new()).await?;
    info!(%instance_id, "workflow instance started");

    // Run the benchmark with DAG runner integration
    let timeout = Duration::from_secs(args.timeout);
    let start = Instant::now();
    let metrics = run_with_dag_runner(&runner, &database, &worker_pool, timeout).await?;
    let elapsed = start.elapsed();

    // Calculate statistics
    let total = metrics.len() as u64;
    let elapsed_s = elapsed.as_secs_f64();
    let throughput = total as f64 / elapsed_s;

    let mut round_trips: Vec<f64> = metrics
        .iter()
        .map(|m| m.round_trip.as_secs_f64() * 1000.0)
        .collect();
    round_trips.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let avg_round_trip_ms = if round_trips.is_empty() {
        0.0
    } else {
        round_trips.iter().sum::<f64>() / round_trips.len() as f64
    };

    let p95_round_trip_ms = if round_trips.is_empty() {
        0.0
    } else {
        let idx = (round_trips.len() as f64 * 0.95) as usize;
        round_trips[idx.min(round_trips.len() - 1)]
    };

    let output = BenchmarkOutput {
        total,
        elapsed_s,
        throughput,
        avg_round_trip_ms,
        p95_round_trip_ms,
    };

    // Output results
    if args.json {
        println!("{}", serde_json::to_string(&output)?);
    } else {
        println!("\n=== Benchmark Results ===");
        println!("Actions executed: {}", output.total);
        println!("Elapsed time: {:.2}s", output.elapsed_s);
        println!("Throughput: {:.2} actions/sec", output.throughput);
        println!("Avg round-trip: {:.2}ms", output.avg_round_trip_ms);
        println!("P95 round-trip: {:.2}ms", output.p95_round_trip_ms);
    }

    // Cleanup
    let _ = grpc_shutdown.send(());
    worker_bridge.shutdown().await;
    drop(python_env);

    Ok(())
}
