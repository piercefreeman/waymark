//! Integration test harness.
//!
//! Provides a complete test environment with:
//! - Database connection
//! - Worker bridge server
//! - Python worker pool
//! - DAGRunner for workflow execution
//! - gRPC service for workflow registration

#![allow(dead_code)]

use std::{
    collections::HashMap,
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use prost::Message;
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    process::Command,
    sync::oneshot,
    task::JoinHandle,
    time::{Duration, timeout},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;

use rappel::{
    ActionDispatchPayload, DAGRunner, Database, PythonWorkerConfig, PythonWorkerPool,
    RoundTripMetrics, RunnerConfig, WorkerBridgeServer, WorkflowInstanceId, WorkflowVersionId,
    proto,
};

const SCRIPT_TIMEOUT: Duration = Duration::from_secs(60);

// ============================================================================
// Test gRPC Service for Workflow Registration
// ============================================================================

/// gRPC service that handles workflow registration and stores in the database.
struct TestWorkflowService {
    database: Database,
}

impl TestWorkflowService {
    fn new(database: Database) -> Self {
        Self { database }
    }
}

#[tonic::async_trait]
impl proto::workflow_service_server::WorkflowService for TestWorkflowService {
    async fn register_workflow(
        &self,
        request: tonic::Request<proto::RegisterWorkflowRequest>,
    ) -> Result<tonic::Response<proto::RegisterWorkflowResponse>, tonic::Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| tonic::Status::invalid_argument("registration missing"))?;

        info!(
            workflow_name = %registration.workflow_name,
            ir_hash = %registration.ir_hash,
            ir_len = registration.ir.len(),
            "received workflow registration"
        );

        // Store the workflow version in the database
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

        // Create a workflow instance
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

        info!(
            %version_id,
            %instance_id,
            "workflow registered and instance created"
        );

        Ok(tonic::Response::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_id: instance_id.to_string(),
        }))
    }

    async fn wait_for_instance(
        &self,
        request: tonic::Request<proto::WaitForInstanceRequest>,
    ) -> Result<tonic::Response<proto::WaitForInstanceResponse>, tonic::Status> {
        let inner = request.into_inner();
        let instance_id: uuid::Uuid = inner.instance_id.parse().map_err(|err| {
            tonic::Status::invalid_argument(format!("invalid instance_id: {err}"))
        })?;

        // Simple polling for completion (for testing)
        let interval = Duration::from_secs_f64(inner.poll_interval_secs.clamp(0.1, 30.0));
        let timeout_duration = Duration::from_secs(300); // 5 minute timeout
        let start = std::time::Instant::now();

        loop {
            let instance = self
                .database
                .get_instance(WorkflowInstanceId(instance_id))
                .await
                .map_err(|e| tonic::Status::internal(format!("database error: {}", e)))?;

            if instance.status == "completed" || instance.status == "failed" {
                return Ok(tonic::Response::new(proto::WaitForInstanceResponse {
                    payload: instance.result_payload.unwrap_or_default(),
                }));
            }

            if start.elapsed() > timeout_duration {
                return Err(tonic::Status::deadline_exceeded(
                    "wait_for_instance timed out",
                ));
            }

            tokio::time::sleep(interval).await;
        }
    }
}

/// Start a gRPC server for workflow registration.
async fn start_workflow_grpc_server(
    database: Database,
) -> Result<(SocketAddr, oneshot::Sender<()>, JoinHandle<()>)> {
    use proto::workflow_service_server::WorkflowServiceServer;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = TestWorkflowService::new(database);

    let handle = tokio::spawn(async move {
        let shutdown = async move {
            let _ = shutdown_rx.await;
        };
        let result = Server::builder()
            .add_service(WorkflowServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await;
        if let Err(err) = result {
            tracing::error!(?err, "test workflow gRPC server error");
        }
    });

    info!(%addr, "test workflow gRPC server started");
    Ok((addr, shutdown_tx, handle))
}

// ============================================================================
// Integration Test Harness
// ============================================================================

/// Configuration for the integration test harness.
pub struct HarnessConfig<'a> {
    /// Files to create in the Python environment (filename, contents)
    pub files: &'a [(&'static str, &'static str)],
    /// Script to run for workflow registration
    pub entrypoint: &'static str,
    /// Name of the workflow (lowercase, no spaces)
    pub workflow_name: &'static str,
    /// Python module containing the workflow/actions
    pub user_module: &'static str,
    /// Input arguments for the workflow
    pub inputs: &'a [(&'static str, &'static str)],
}

/// Integration test harness that manages the full runtime stack.
pub struct IntegrationHarness {
    database: Arc<Database>,
    worker_bridge: Arc<WorkerBridgeServer>,
    worker_pool: Arc<PythonWorkerPool>,
    runner: Arc<DAGRunner>,
    python_env: TempDir,
    version_id: WorkflowVersionId,
    instance_id: WorkflowInstanceId,
    grpc_shutdown: Option<oneshot::Sender<()>>,
    grpc_handle: Option<JoinHandle<()>>,
}

impl IntegrationHarness {
    /// Create a new test harness with the given configuration.
    ///
    /// Returns `None` if DATABASE_URL is not set (skips test).
    pub async fn new(config: HarnessConfig<'_>) -> Result<Option<Self>> {
        let database_url = match env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("skipping integration test: DATABASE_URL not set");
                return Ok(None);
            }
        };

        // Connect to database
        let database = Arc::new(Database::connect(&database_url).await?);
        cleanup_database(&database).await?;

        // Start the workflow registration gRPC server
        let (grpc_addr, grpc_shutdown, grpc_handle) =
            start_workflow_grpc_server((*database).clone()).await?;
        info!(%grpc_addr, "workflow registration gRPC server started");

        // Start worker bridge (for worker connections)
        let worker_bridge = WorkerBridgeServer::start(None).await?;
        info!(addr = %worker_bridge.addr(), "worker bridge started");

        // Set up Python environment and run registration script
        let env_vars = vec![
            ("CARABINER_SERVER_PORT", "9999".to_string()),
            ("CARABINER_GRPC_ADDR", grpc_addr.to_string()),
            ("CARABINER_SKIP_WAIT_FOR_INSTANCE", "1".to_string()),
        ];
        let python_env = run_in_env(config.files, &[], &env_vars, config.entrypoint).await?;

        // Find the registered workflow version
        let versions = database.list_workflow_versions().await?;
        let version = versions
            .iter()
            .find(|v| v.workflow_name == config.workflow_name)
            .with_context(|| {
                format!(
                    "workflow '{}' not found after registration",
                    config.workflow_name
                )
            })?;
        let version_id = WorkflowVersionId(version.id);

        // Find the instance that was created during registration
        let instances: Vec<rappel::WorkflowInstance> = sqlx::query_as(
            "SELECT id, partition_id, workflow_name, workflow_version_id, \
             next_action_seq, input_payload, result_payload, status, \
             created_at, completed_at \
             FROM workflow_instances WHERE workflow_version_id = $1 ORDER BY created_at DESC LIMIT 1"
        )
        .bind(version_id.0)
        .fetch_all(database.pool())
        .await?;

        let instance_id = instances
            .first()
            .map(|i| WorkflowInstanceId(i.id))
            .with_context(|| "no instance found after registration")?;

        info!(%instance_id, "found workflow instance");

        // Start worker pool
        let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("python")
            .join(".venv")
            .join("bin")
            .join("rappel-worker");

        let worker_config = PythonWorkerConfig {
            script_path: worker_script,
            script_args: Vec::new(),
            user_modules: vec![config.user_module.to_string()],
            extra_python_paths: vec![python_env.path().to_path_buf()],
        };
        let worker_pool =
            Arc::new(PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_bridge)).await?);
        info!("worker pool ready");

        // Create DAGRunner with the proper components
        let runner_config = RunnerConfig {
            batch_size: 10,
            max_slots_per_worker: 5,
            poll_interval_ms: 50,
            timeout_check_interval_ms: 1000,
        };
        let runner = Arc::new(DAGRunner::new(
            runner_config,
            Arc::clone(&database),
            Arc::clone(&worker_pool),
        ));

        // Start the workflow instance using the DAGRunner
        let initial_inputs = build_initial_inputs(config.inputs);
        runner
            .start_instance(instance_id, initial_inputs)
            .await
            .context("failed to start workflow instance")?;

        Ok(Some(Self {
            database,
            worker_bridge,
            worker_pool,
            runner,
            python_env,
            version_id,
            instance_id,
            grpc_shutdown: Some(grpc_shutdown),
            grpc_handle: Some(grpc_handle),
        }))
    }

    /// Run the DAGRunner until the workflow completes or times out.
    ///
    /// This starts the DAGRunner's main loop and waits for the workflow instance
    /// to reach a terminal state (completed or failed).
    pub async fn run_to_completion(&self, timeout_secs: u64) -> Result<Vec<RoundTripMetrics>> {
        let runner = Arc::clone(&self.runner);
        let instance_id = self.instance_id;
        let database = Arc::clone(&self.database);

        // Start the runner in a background task
        let runner_handle = tokio::spawn(async move {
            let _ = runner.run().await;
        });

        // Wait for workflow completion
        let timeout_duration = Duration::from_secs(timeout_secs);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout_duration {
                self.runner.shutdown();
                let _ = runner_handle.await;
                return Err(anyhow!(
                    "workflow did not complete within {}s",
                    timeout_secs
                ));
            }

            let instance = database.get_instance(instance_id).await?;
            if instance.status == "completed" || instance.status == "failed" {
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Shutdown the runner
        self.runner.shutdown();
        let _ = runner_handle.await;

        // Return empty metrics for now - the full metrics would require
        // changes to track them through the runner
        Ok(vec![])
    }

    /// Dispatch all queued actions and wait for completion.
    ///
    /// This uses the DAGRunner for completion handling, which triggers DAG
    /// traversal to enqueue successor actions. This properly handles:
    /// - Sequential workflows (action chains)
    /// - Parallel workflows (gather/spread)
    /// - Loops with multiple iterations
    pub async fn dispatch_all(&self) -> Result<Vec<RoundTripMetrics>> {
        dispatch_all_actions_with_runner(&self.database, &self.worker_pool, &self.runner).await
    }

    /// Get the stored workflow result.
    pub async fn stored_result(&self) -> Result<Option<Vec<u8>>> {
        let instance = self.database.get_instance(self.instance_id).await?;
        Ok(instance.result_payload)
    }

    /// Get the database handle.
    pub fn database(&self) -> &Database {
        &self.database
    }

    /// Get the workflow instance ID.
    pub fn instance_id(&self) -> WorkflowInstanceId {
        self.instance_id
    }

    /// Get the DAGRunner.
    pub fn runner(&self) -> &DAGRunner {
        &self.runner
    }

    /// Shut down the harness.
    pub async fn shutdown(mut self) -> Result<()> {
        // Shutdown runner first
        self.runner.shutdown();

        // Shutdown gRPC server
        if let Some(shutdown_tx) = self.grpc_shutdown.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.grpc_handle.take() {
            let _ = handle.await;
        }

        // Drop runner to release its Arc reference to worker_pool
        drop(self.runner);

        // Try to get ownership of worker_pool for clean shutdown
        match Arc::try_unwrap(self.worker_pool) {
            Ok(pool) => {
                pool.shutdown().await?;
            }
            Err(_arc) => {
                // Other references still exist - pool will be cleaned up on drop
                tracing::warn!("worker pool has other references, skipping explicit shutdown");
            }
        }

        self.worker_bridge.shutdown().await;
        drop(self.python_env);
        Ok(())
    }
}

/// Build initial inputs from string pairs.
fn build_initial_inputs(pairs: &[(&str, &str)]) -> HashMap<String, JsonValue> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), JsonValue::String((*v).to_string())))
        .collect()
}

/// Clean up the database before each test.
async fn cleanup_database(db: &Database) -> Result<()> {
    sqlx::query("TRUNCATE action_queue, instance_context, loop_state, workflow_instances, workflow_versions CASCADE")
        .execute(db.pool())
        .await?;
    Ok(())
}

/// Run a Python script in a temporary environment.
pub async fn run_in_env(
    files: &[(&str, &str)],
    requirements: &[&str],
    env_vars: &[(&str, String)],
    entrypoint: &str,
) -> Result<TempDir> {
    let env_dir = TempDir::new().context("create python env dir")?;

    // Write files
    for (relative, contents) in files {
        let path = env_dir.path().join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, contents.trim_start())?;
    }

    // Set up pyproject.toml
    let repo_python = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .canonicalize()?;

    let mut deps = vec![format!("rappel @ file://{}", repo_python.display())];
    deps.extend(requirements.iter().map(|s| s.to_string()));

    let deps_toml = deps
        .iter()
        .map(|dep| format!(r#""{dep}""#))
        .collect::<Vec<_>>()
        .join(",\n    ");

    let pyproject = format!(
        r#"[project]
name = "rappel-integration"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    {deps_toml}
]
"#
    );
    fs::write(env_dir.path().join("pyproject.toml"), pyproject)?;

    // Run uv sync
    run_shell(env_dir.path(), "uv sync", &[], None).await?;

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

    let mut run_envs = env_vars.to_vec();
    run_envs.push(("PYTHONPATH", pythonpath));

    // Run the entrypoint
    run_shell(
        env_dir.path(),
        &format!("uv run python {entrypoint}"),
        &run_envs,
        Some(SCRIPT_TIMEOUT),
    )
    .await?;

    Ok(env_dir)
}

async fn run_shell(
    cwd: &Path,
    command: &str,
    envs: &[(&str, String)],
    timeout_limit: Option<Duration>,
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

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn `{command}`"))?;

    let wait_future = child.wait();
    let status = if let Some(limit) = timeout_limit {
        match timeout(limit, wait_future).await {
            Ok(result) => result?,
            Err(_) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(anyhow!(
                    "command `{command}` timed out after {}s",
                    limit.as_secs()
                ));
            }
        }
    } else {
        wait_future.await?
    };

    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("command `{command}` failed with {status}"))
    }
}

/// Dispatch all actions from the queue to workers with DAG runner integration.
///
/// This dispatch loop:
/// 1. Fetches queued actions
/// 2. Dispatches them to workers
/// 3. Uses DAGRunner to process completions (which enqueues successor actions)
/// 4. Repeats until no more work is available
async fn dispatch_all_actions_with_runner(
    database: &Arc<Database>,
    pool: &Arc<PythonWorkerPool>,
    runner: &DAGRunner,
) -> Result<Vec<RoundTripMetrics>> {
    let mut completed = Vec::new();
    let mut max_iterations = 500;
    let mut idle_cycles = 0usize;

    while max_iterations > 0 {
        max_iterations -= 1;

        let actions = database.dispatch_actions(16).await?;
        if actions.is_empty() {
            idle_cycles = idle_cycles.saturating_add(1);
            if idle_cycles >= 10 && !completed.is_empty() {
                // Check if there are any pending actions in the queue
                let pending: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM action_queue WHERE status IN ('pending', 'running')",
                )
                .fetch_one(database.pool())
                .await?;

                if pending == 0 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }

        idle_cycles = 0;
        for action in actions {
            // Build dispatch payload from JSON kwargs
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

            // Process completion through DAGRunner to trigger DAG traversal
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
                tracing::error!("Failed to process completion: {}", e);
            }

            completed.push(metrics);
        }
    }

    Ok(completed)
}

/// Convert JSON to WorkflowArguments proto.
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
