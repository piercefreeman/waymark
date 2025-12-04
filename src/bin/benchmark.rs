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
use tracing::{error, info};

use rappel::{
    Database, PythonWorkerConfig, PythonWorkerPool, WorkerBridgeServer, WorkflowInstanceId,
    WorkflowVersionId, proto,
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

    /// Number of simulated hosts (each gets its own DAGRunner + worker pool)
    #[arg(long, default_value = "1")]
    hosts: u32,

    /// Number of Python workers per host
    #[arg(long, default_value = "4")]
    workers_per_host: u32,

    /// Number of workflow instances to run concurrently
    #[arg(long, default_value = "1")]
    instances: u32,

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
// Benchmark Runner
// ============================================================================

/// Wait for the workflow to complete by monitoring the database.
/// Returns action count and metrics collection.
async fn wait_for_completion(database: &Arc<Database>, timeout: Duration) -> Result<u64> {
    let start = std::time::Instant::now();
    let mut last_completed = 0u64;
    let mut idle_cycles = 0usize;
    let max_idle_cycles = 40; // 2 seconds of no progress

    loop {
        if start.elapsed() > timeout {
            info!("Benchmark timeout reached");
            break;
        }

        // Check how many actions have completed
        let completed: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM action_queue WHERE status = 'completed'")
                .fetch_one(database.pool())
                .await?;

        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM action_queue WHERE status IN ('pending', 'running')",
        )
        .fetch_one(database.pool())
        .await?;

        if pending == 0 && completed > 0 {
            info!(completed = completed, "All actions completed");
            return Ok(completed as u64);
        }

        if completed as u64 > last_completed {
            last_completed = completed as u64;
            idle_cycles = 0;
        } else {
            idle_cycles += 1;
            if idle_cycles >= max_idle_cycles && completed > 0 {
                info!(
                    completed = completed,
                    pending = pending,
                    "No progress, stopping"
                );
                break;
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(last_completed)
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging - respects RUST_LOG env var for filtering
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("hyper=warn".parse().unwrap())
                .add_directive("h2=warn".parse().unwrap())
                .add_directive("tower=warn".parse().unwrap())
                .add_directive("tonic=warn".parse().unwrap()),
        )
        .init();

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

    // Find the workflow version
    let versions = database.list_workflow_versions().await?;
    let version = versions
        .iter()
        .find(|v| v.workflow_name == "benchmarkfanoutworkflow")
        .context("benchmark workflow not found")?;
    let version_id = WorkflowVersionId(version.id);

    // Get the initial instance (created during registration)
    let initial_instances: Vec<rappel::WorkflowInstance> = sqlx::query_as(
        "SELECT id, partition_id, workflow_name, workflow_version_id, \
         next_action_seq, input_payload, result_payload, status, \
         created_at, completed_at \
         FROM workflow_instances WHERE workflow_version_id = $1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(version_id.0)
    .fetch_all(database.pool())
    .await?;

    let first_instance_id = initial_instances
        .first()
        .map(|i| WorkflowInstanceId(i.id))
        .context("no instance found")?;

    // Create additional instances if requested
    let mut instance_ids = vec![first_instance_id];
    for _ in 1..args.instances {
        let instance_id = database
            .create_instance("benchmarkfanoutworkflow", version_id, None)
            .await?;
        instance_ids.push(instance_id);
    }
    info!(count = instance_ids.len(), "workflow instances created");

    // Common worker configuration
    let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .join(".venv")
        .join("bin")
        .join("rappel-worker");

    let python_env_path = python_env.path().to_path_buf();

    // Prepare initial inputs for the workflow
    let mut initial_inputs = HashMap::new();
    let indices: Vec<serde_json::Value> = (0..args.count)
        .map(|i| serde_json::Value::Number(i.into()))
        .collect();
    initial_inputs.insert("indices".to_string(), serde_json::Value::Array(indices));
    initial_inputs.insert(
        "iterations".to_string(),
        serde_json::Value::Number(args.iterations.into()),
    );

    // Create multiple simulated hosts, each with its own runner + worker pool
    let mut hosts: Vec<(
        Arc<rappel::DAGRunner>,
        Arc<PythonWorkerPool>,
        JoinHandle<()>,
    )> = Vec::new();

    for host_id in 0..args.hosts {
        let worker_config = PythonWorkerConfig {
            script_path: worker_script.clone(),
            script_args: Vec::new(),
            user_modules: vec!["benchmark_workflow".to_string()],
            extra_python_paths: vec![python_env_path.clone()],
        };

        let worker_pool = Arc::new(
            PythonWorkerPool::new(
                worker_config,
                args.workers_per_host as usize,
                Arc::clone(&worker_bridge),
            )
            .await?,
        );

        let runner_config = rappel::RunnerConfig {
            batch_size: 64,
            max_slots_per_worker: 10,
            poll_interval_ms: 10,
            timeout_check_interval_ms: 1000,
        };

        let runner = Arc::new(rappel::DAGRunner::new(
            runner_config,
            Arc::clone(&database),
            Arc::clone(&worker_pool),
        ));

        // Start instances from the first host only (they'll be picked up by all hosts via DB)
        if host_id == 0 {
            for instance_id in &instance_ids {
                runner
                    .start_instance(*instance_id, initial_inputs.clone())
                    .await?;
            }
        }

        // Spawn the runner in its own tokio task
        let runner_clone = Arc::clone(&runner);
        let handle = tokio::spawn(async move {
            if let Err(e) = runner_clone.run().await {
                error!(host_id = host_id, "Runner failed: {}", e);
            }
        });

        hosts.push((runner, worker_pool, handle));
    }

    let total_workers = args.hosts * args.workers_per_host;
    info!(
        hosts = args.hosts,
        workers_per_host = args.workers_per_host,
        total_workers = total_workers,
        "all hosts ready"
    );
    info!(count = instance_ids.len(), "workflow instances started");

    // Wait for completion by monitoring the database
    let timeout = Duration::from_secs(args.timeout);
    let start = Instant::now();
    let total = wait_for_completion(&database, timeout).await?;
    let elapsed = start.elapsed();

    // Shutdown all runners
    for (runner, _, _) in &hosts {
        runner.shutdown();
    }

    // Wait for all runner tasks to complete
    for (_, _, handle) in &mut hosts {
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    }

    // Drop runners to release worker_pool references, then shutdown pools
    let worker_pools: Vec<_> = hosts
        .into_iter()
        .map(|(r, p, _)| {
            drop(r);
            p
        })
        .collect();

    for worker_pool in worker_pools {
        match Arc::try_unwrap(worker_pool) {
            Ok(pool) => {
                if let Err(e) = pool.shutdown().await {
                    error!("Failed to shutdown worker pool: {}", e);
                }
            }
            Err(_) => {
                error!("Worker pool still has references, cannot shut down cleanly");
            }
        }
    }

    // Calculate statistics
    let elapsed_s = elapsed.as_secs_f64();
    let throughput = total as f64 / elapsed_s;

    let output = BenchmarkOutput {
        total,
        elapsed_s,
        throughput,
        avg_round_trip_ms: 0.0,
        p95_round_trip_ms: 0.0,
    };

    // Output results
    if args.json {
        println!("{}", serde_json::to_string(&output)?);
    } else {
        println!("\n=== Benchmark Results ===");
        println!("Hosts: {}", args.hosts);
        println!("Workers per host: {}", args.workers_per_host);
        println!("Total workers: {}", total_workers);
        println!("Actions executed: {}", output.total);
        println!("Elapsed time: {:.2}s", output.elapsed_s);
        println!("Throughput: {:.2} actions/sec", output.throughput);
    }

    // Cleanup
    let _ = grpc_shutdown.send(());
    worker_bridge.shutdown().await;
    drop(python_env);

    Ok(())
}
