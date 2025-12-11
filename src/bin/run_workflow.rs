//! Run Workflow CLI - Execute a Python workflow file locally.
//!
//! This binary provides a simple way to run a Rappel workflow:
//! 1. Parses a Python workflow file to generate IR
//! 2. Converts to DAG and registers with the database
//! 3. Executes with Python workers
//! 4. Outputs the result
//!
//! Usage:
//!   cargo run --bin run-workflow -- path/to/workflow.py --input '{"items": [1,2,3]}'

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
use base64::Engine;
use clap::Parser;
use prost::Message;
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::{net::TcpListener, process::Command, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::{error, info, warn};

use rappel::{
    Database, PythonWorkerConfig, PythonWorkerPool, WorkerBridgeServer, WorkflowInstanceId,
    WorkflowVersionId, proto,
};

// ============================================================================
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug)]
#[command(
    name = "run-workflow",
    about = "Run a Rappel workflow from a Python file"
)]
struct Args {
    /// Path to the Python workflow file
    #[arg(required = true)]
    workflow_file: PathBuf,

    /// Input arguments as JSON object (e.g., '{"items": [1,2,3], "threshold": 5}')
    #[arg(short, long, default_value = "{}")]
    input: String,

    /// Number of Python workers
    #[arg(short, long, default_value = "4")]
    workers: u32,

    /// Timeout in seconds
    #[arg(short, long, default_value = "60")]
    timeout: u64,

    /// Output result as JSON (otherwise pretty-print)
    #[arg(long, default_value = "false")]
    json: bool,

    /// Verbose output (show IR and DAG info)
    #[arg(short, long, default_value = "false")]
    verbose: bool,
}

// ============================================================================
// gRPC Service for Workflow Registration
// ============================================================================

struct WorkflowService {
    database: Database,
}

impl WorkflowService {
    fn new(database: Database) -> Self {
        Self { database }
    }
}

#[tonic::async_trait]
impl proto::workflow_service_server::WorkflowService for WorkflowService {
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
        // Not needed for local execution
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
    let service = WorkflowService::new(database);

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
// IR Generation
// ============================================================================

/// Generate IR from a Python workflow file using the Python IR generator.
async fn generate_ir(workflow_file: &PathBuf) -> Result<(String, Vec<u8>)> {
    let script_path = std::env::current_dir()?.join("scripts/generate_ir.py");

    let output = Command::new("python3")
        .arg(&script_path)
        .arg(workflow_file)
        .arg("--format")
        .arg("base64")
        .output()
        .await
        .context("Failed to execute Python IR generator")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Python IR generation failed: {}", stderr);
    }

    let base64_ir = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in IR output")?
        .trim()
        .to_string();

    // Decode base64 to get the proto bytes
    let proto_bytes = base64::engine::general_purpose::STANDARD
        .decode(&base64_ir)
        .context("Failed to decode base64 IR")?;

    // Parse the proto to get the text representation for display
    let program = rappel::ir_ast::Program::decode(&proto_bytes[..])
        .context("Failed to decode program proto")?;

    let ir_text = rappel::ir_printer::print_program(&program);

    Ok((ir_text, proto_bytes))
}

// ============================================================================
// Python Environment Setup
// ============================================================================

async fn setup_python_env(
    grpc_addr: SocketAddr,
    workflow_file: &PathBuf,
    inputs: &HashMap<String, JsonValue>,
) -> Result<(TempDir, String)> {
    info!("Setting up Python environment...");
    let env_dir = TempDir::new().context("create python env dir")?;

    // Copy the workflow file to the temp directory
    let workflow_filename = workflow_file
        .file_name()
        .context("workflow file has no name")?
        .to_str()
        .context("workflow filename is not valid UTF-8")?;

    let workflow_content = fs::read_to_string(workflow_file)
        .with_context(|| format!("Failed to read workflow file: {}", workflow_file.display()))?;

    fs::write(env_dir.path().join(workflow_filename), &workflow_content)?;

    // Extract the workflow class name from the file
    // Look for @workflow decorated class
    let workflow_class = extract_workflow_class(&workflow_content)?;
    let workflow_name = workflow_class.to_lowercase();
    info!(class = %workflow_class, "Found workflow class");

    // Generate inputs as Python code
    let inputs_json = serde_json::to_string(inputs)?;

    // Write registration script
    let module_name = workflow_filename.trim_end_matches(".py");
    let register_script = format!(
        r#"
import asyncio
import json
import os

from {module_name} import {workflow_class}

async def main():
    os.environ.pop("PYTEST_CURRENT_TEST", None)
    wf = {workflow_class}()
    inputs = json.loads('''{inputs_json}''')
    result = await wf.run(**inputs)
    print(f"Result: {{result}}")

asyncio.run(main())
"#
    );
    fs::write(env_dir.path().join("run.py"), register_script.trim_start())?;

    // Set up pyproject.toml
    let repo_python = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .canonicalize()?;

    let pyproject = format!(
        r#"[project]
name = "rappel-workflow-runner"
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
    info!("Running uv sync (this may take a moment on first run)...");
    run_shell(env_dir.path(), "uv sync", &[]).await?;
    info!("Python environment ready");

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

    // Run registration (this triggers the workflow)
    info!("Registering workflow...");
    run_shell_with_env(env_dir.path(), "uv run python run.py", &env_vars).await?;

    Ok((env_dir, workflow_name))
}

/// Extract the workflow class name from Python source.
fn extract_workflow_class(source: &str) -> Result<String> {
    // Look for pattern: @workflow followed by class ClassName
    let lines: Vec<&str> = source.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        if line.trim() == "@workflow" {
            // Next non-empty line should be the class definition
            for next_line in &lines[i + 1..] {
                let trimmed = next_line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed.starts_with("class ") {
                    // Extract class name
                    let rest = trimmed.strip_prefix("class ").unwrap();
                    let class_name = rest
                        .split(['(', ':'])
                        .next()
                        .context("Failed to parse class name")?
                        .trim();
                    return Ok(class_name.to_string());
                }
                break;
            }
        }
    }
    Err(anyhow!(
        "No @workflow decorated class found in the Python file"
    ))
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
// Workflow Execution
// ============================================================================

/// Wait for the workflow to complete by monitoring the database.
async fn wait_for_completion(
    database: &Arc<Database>,
    instance_id: WorkflowInstanceId,
    timeout: Duration,
    verbose: bool,
) -> Result<Option<JsonValue>> {
    let start = Instant::now();
    let mut last_progress = Instant::now();
    let mut last_completed: i64 = 0;
    let mut last_pending: i64 = 0;

    loop {
        if start.elapsed() > timeout {
            return Err(anyhow!("Workflow execution timed out"));
        }

        // Check workflow instance status using a direct query to avoid pool contention
        // The database.get_instance uses the shared pool which may be contended by the runner
        let status: String =
            sqlx::query_scalar("SELECT status FROM workflow_instances WHERE id = $1")
                .bind(instance_id.0)
                .fetch_one(database.pool())
                .await
                .unwrap_or_else(|_| "unknown".to_string());

        match status.as_str() {
            "completed" => {
                // Return success marker - result fetching happens after runner shutdown
                return Ok(None);
            }
            "failed" => {
                return Err(anyhow!("Workflow execution failed"));
            }
            _ => {
                // Show progress periodically in verbose mode
                if verbose && last_progress.elapsed() > Duration::from_millis(500) {
                    let completed: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM action_queue WHERE instance_id = $1 AND status = 'completed'"
                    )
                    .bind(instance_id.0)
                    .fetch_one(database.pool())
                    .await
                    .unwrap_or(0);

                    let pending: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM action_queue WHERE instance_id = $1 AND status IN ('pending', 'dispatched')"
                    )
                    .bind(instance_id.0)
                    .fetch_one(database.pool())
                    .await
                    .unwrap_or(0);

                    // Only print if something changed
                    if completed != last_completed || pending != last_pending {
                        eprintln!(
                            "[run-workflow] Progress: {} actions completed, {} pending/dispatched",
                            completed, pending
                        );
                        last_completed = completed;
                        last_pending = pending;
                    }
                    last_progress = Instant::now();
                }

                // Still running, wait a bit and yield to let the runner make progress
                tokio::time::sleep(Duration::from_millis(50)).await;
                tokio::task::yield_now().await;
            }
        }
    }
}

/// Convert a protobuf WorkflowArgumentValue to a JSON Value.
fn proto_value_to_json(value: &proto::WorkflowArgumentValue) -> JsonValue {
    use proto::primitive_workflow_argument::Kind as PrimitiveKind;
    use proto::workflow_argument_value::Kind;

    match &value.kind {
        Some(Kind::Primitive(p)) => match &p.kind {
            Some(PrimitiveKind::IntValue(i)) => JsonValue::Number((*i).into()),
            Some(PrimitiveKind::DoubleValue(f)) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Some(PrimitiveKind::StringValue(s)) => JsonValue::String(s.clone()),
            Some(PrimitiveKind::BoolValue(b)) => JsonValue::Bool(*b),
            Some(PrimitiveKind::NullValue(_)) => JsonValue::Null,
            None => JsonValue::Null,
        },
        Some(Kind::ListValue(list)) => {
            let items: Vec<JsonValue> = list.items.iter().map(proto_value_to_json).collect();
            JsonValue::Array(items)
        }
        Some(Kind::DictValue(dict)) => {
            let entries: serde_json::Map<String, JsonValue> = dict
                .entries
                .iter()
                .filter_map(|arg| {
                    arg.value
                        .as_ref()
                        .map(|v| (arg.key.clone(), proto_value_to_json(v)))
                })
                .collect();
            JsonValue::Object(entries)
        }
        Some(Kind::TupleValue(tuple)) => {
            let items: Vec<JsonValue> = tuple.items.iter().map(proto_value_to_json).collect();
            JsonValue::Array(items)
        }
        Some(Kind::Basemodel(model)) => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "__class__".to_string(),
                JsonValue::String(model.name.clone()),
            );
            obj.insert(
                "__module__".to_string(),
                JsonValue::String(model.module.clone()),
            );
            if let Some(data_dict) = &model.data {
                for entry in &data_dict.entries {
                    if let Some(v) = &entry.value {
                        obj.insert(entry.key.clone(), proto_value_to_json(v));
                    }
                }
            }
            JsonValue::Object(obj)
        }
        Some(Kind::Exception(exc)) => {
            let mut obj = serde_json::Map::new();
            obj.insert("__exception__".to_string(), JsonValue::Bool(true));
            obj.insert("type".to_string(), JsonValue::String(exc.r#type.clone()));
            obj.insert(
                "message".to_string(),
                JsonValue::String(exc.message.clone()),
            );
            JsonValue::Object(obj)
        }
        None => JsonValue::Null,
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging - show more detail in verbose mode
    let log_filter = if args.verbose {
        tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("rappel::runner=debug".parse().unwrap())
            .add_directive("rappel::completion=debug".parse().unwrap())
            .add_directive("hyper=warn".parse().unwrap())
            .add_directive("h2=warn".parse().unwrap())
            .add_directive("tower=warn".parse().unwrap())
            .add_directive("tonic=warn".parse().unwrap())
    } else {
        // Note: We need at least info level for rappel::runner to ensure proper async task scheduling.
        // Setting rappel=warn causes timing issues with the runner task, so we use rappel::runner=info
        // but keep other rappel modules quiet. This is a workaround for what appears to be a
        // scheduling issue when tracing is completely disabled.
        tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("hyper=warn".parse().unwrap())
            .add_directive("h2=warn".parse().unwrap())
            .add_directive("tower=warn".parse().unwrap())
            .add_directive("tonic=warn".parse().unwrap())
            .add_directive("rappel=warn".parse().unwrap())
            .add_directive("rappel::runner=info".parse().unwrap())
            .add_directive("rappel::db=info".parse().unwrap())
    };

    tracing_subscriber::fmt().with_env_filter(log_filter).init();

    // Validate workflow file exists
    if !args.workflow_file.exists() {
        return Err(anyhow!(
            "Workflow file not found: {}",
            args.workflow_file.display()
        ));
    }

    // Parse input JSON
    let inputs: HashMap<String, JsonValue> = serde_json::from_str(&args.input)
        .context("Failed to parse input JSON. Expected format: '{\"key\": value}'")?;

    if args.verbose {
        info!(file = %args.workflow_file.display(), "Loading workflow");
        info!(inputs = ?inputs, "With inputs");
    }

    // Generate IR and display if verbose
    if args.verbose {
        let (ir_text, _proto_bytes) = generate_ir(&args.workflow_file).await?;
        println!("\n=== Generated IR ===\n{}\n", ir_text);
    }

    // Connect to database with a short timeout
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://mountaineer:mountaineer@localhost:5432/mountaineer_daemons".to_string()
    });
    eprintln!(
        "[run-workflow] Connecting to database at {}...",
        database_url
    );

    // Use a larger pool size to avoid contention between runner and polling
    let pool_size = (args.workers * 2).max(20);
    let database = tokio::time::timeout(
        Duration::from_secs(5),
        Database::connect_with_pool_size(&database_url, pool_size),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "Database connection timed out after 5 seconds.\n\
         Please ensure PostgreSQL is running and accessible at: {}\n\
         You can set DATABASE_URL environment variable to override.",
            database_url
        )
    })?
    .context("Failed to connect to database")?;

    let database = Arc::new(database);
    eprintln!("[run-workflow] Connected to database");

    // Clean up any previous state (for local testing)
    eprintln!("[run-workflow] Cleaning up previous state...");
    sqlx::query("TRUNCATE action_queue, instance_context, loop_state, workflow_instances, workflow_versions CASCADE")
        .execute(database.pool())
        .await?;

    // Start gRPC server for workflow registration
    let (grpc_addr, grpc_shutdown, _grpc_handle) = start_grpc_server((*database).clone()).await?;
    eprintln!("[run-workflow] gRPC server started on {}", grpc_addr);

    // Start worker bridge
    let worker_bridge = WorkerBridgeServer::start(None).await?;
    eprintln!(
        "[run-workflow] Worker bridge started on {}",
        worker_bridge.addr()
    );

    // Set up Python environment and register workflow
    eprintln!("[run-workflow] Setting up Python environment...");
    let (python_env, workflow_name) =
        setup_python_env(grpc_addr, &args.workflow_file, &inputs).await?;
    eprintln!("[run-workflow] Workflow '{}' registered", workflow_name);

    // Find the registered workflow version
    let versions = database.list_workflow_versions().await?;
    let version = versions
        .iter()
        .find(|v| v.workflow_name == workflow_name)
        .context("Workflow version not found after registration")?;
    let version_id = WorkflowVersionId(version.id);

    // Get the instance created during registration
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
        .context("No workflow instance found")?;

    eprintln!("[run-workflow] Instance ID: {}", instance_id.0);

    // Set up Python workers
    eprintln!("[run-workflow] Starting {} Python workers...", args.workers);
    let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .join(".venv")
        .join("bin")
        .join("rappel-worker");

    let workflow_module = args
        .workflow_file
        .file_stem()
        .and_then(|s| s.to_str())
        .context("Invalid workflow filename")?;

    let worker_config = PythonWorkerConfig {
        script_path: worker_script,
        script_args: Vec::new(),
        user_modules: vec![workflow_module.to_string()],
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
    eprintln!("[run-workflow] Python workers started");

    // Create and start the DAG runner
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

    // Start the instance
    eprintln!("[run-workflow] Starting workflow instance...");
    runner.start_instance(instance_id, inputs.clone()).await?;

    // Spawn the runner
    eprintln!("[run-workflow] Executing workflow...");
    let runner_clone = Arc::clone(&runner);
    let runner_handle = tokio::spawn(async move {
        if let Err(e) = runner_clone.run().await {
            error!("Runner failed: {}", e);
        }
    });

    // Wait for completion (just status polling, no result fetch)
    let start = Instant::now();
    let timeout = Duration::from_secs(args.timeout);
    let completed = wait_for_completion(&database, instance_id, timeout, args.verbose).await;

    let elapsed = start.elapsed();

    // Shutdown runner FIRST to release database connections
    runner.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(5), runner_handle).await;
    match Arc::try_unwrap(worker_pool) {
        Ok(pool) => {
            if let Err(e) = tokio::time::timeout(Duration::from_secs(3), pool.shutdown()).await {
                warn!("Worker pool shutdown timed out: {}", e);
            }
        }
        Err(_) => {
            warn!("Worker pool still has references");
        }
    }

    let _ = grpc_shutdown.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), worker_bridge.shutdown()).await;
    drop(python_env);

    // Now fetch result after runner is shutdown (database connections freed)
    match completed {
        Ok(_) => {
            // Fetch the result payload now that runner is shutdown
            let result_payload: Option<Vec<u8>> =
                sqlx::query_scalar("SELECT result_payload FROM workflow_instances WHERE id = $1")
                    .bind(instance_id.0)
                    .fetch_one(database.pool())
                    .await?;

            if let Some(result_payload) = result_payload {
                let result = proto::WorkflowArguments::decode(&result_payload[..])
                    .context("Failed to decode result")?;

                // Convert to JSON
                let mut result_map = serde_json::Map::new();
                for arg in result.arguments {
                    if let Some(value) = arg.value {
                        result_map.insert(arg.key, proto_value_to_json(&value));
                    }
                }
                let result_value = JsonValue::Object(result_map);

                if args.json {
                    println!("{}", serde_json::to_string_pretty(&result_value)?);
                } else {
                    println!("\n=== Workflow Result ===");
                    println!("{}", serde_json::to_string_pretty(&result_value)?);
                    println!("\nCompleted in {:.2}s", elapsed.as_secs_f64());
                }
            } else {
                println!("Workflow completed with no return value");
                println!("Completed in {:.2}s", elapsed.as_secs_f64());
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
