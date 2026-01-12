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
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    process::Command,
    sync::{OnceCell, oneshot},
    task::JoinHandle,
    time::{Duration, timeout},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;

use rappel::{
    DAGRunner, Database, PythonWorkerConfig, PythonWorkerPool, RunnerConfig, WorkerBridgeServer,
    WorkflowInstanceId, WorkflowValue, WorkflowVersionId, proto, validate_program,
};

const SCRIPT_TIMEOUT: Duration = Duration::from_secs(60);
static PY_ENV_READY: OnceCell<()> = OnceCell::const_new();

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

        let program = rappel::ir_ast::Program::decode(&registration.ir[..])
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid IR: {e}")))?;
        if let Err(err) = validate_program(&program) {
            return Err(tonic::Status::invalid_argument(format!(
                "invalid IR: {err}"
            )));
        }

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
                None,
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

    async fn register_schedule(
        &self,
        _request: tonic::Request<proto::RegisterScheduleRequest>,
    ) -> Result<tonic::Response<proto::RegisterScheduleResponse>, tonic::Status> {
        // Not needed for tests
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn update_schedule_status(
        &self,
        _request: tonic::Request<proto::UpdateScheduleStatusRequest>,
    ) -> Result<tonic::Response<proto::UpdateScheduleStatusResponse>, tonic::Status> {
        // Not needed for tests
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn delete_schedule(
        &self,
        _request: tonic::Request<proto::DeleteScheduleRequest>,
    ) -> Result<tonic::Response<proto::DeleteScheduleResponse>, tonic::Status> {
        // Not needed for tests
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn list_schedules(
        &self,
        request: tonic::Request<proto::ListSchedulesRequest>,
    ) -> Result<tonic::Response<proto::ListSchedulesResponse>, tonic::Status> {
        let inner = request.into_inner();

        let schedules = self
            .database
            .list_schedules(inner.status_filter.as_deref())
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        fn format_opt_datetime(dt: Option<chrono::DateTime<chrono::Utc>>) -> String {
            dt.map(|d| d.to_rfc3339()).unwrap_or_default()
        }

        let schedule_infos: Vec<proto::ScheduleInfo> = schedules
            .into_iter()
            .map(|s| {
                let schedule_type = match s.schedule_type.as_str() {
                    "cron" => proto::ScheduleType::Cron,
                    "interval" => proto::ScheduleType::Interval,
                    _ => proto::ScheduleType::Unspecified,
                };
                let status = match s.status.as_str() {
                    "active" => proto::ScheduleStatus::Active,
                    "paused" => proto::ScheduleStatus::Paused,
                    _ => proto::ScheduleStatus::Unspecified,
                };
                proto::ScheduleInfo {
                    id: s.id.to_string(),
                    workflow_name: s.workflow_name,
                    schedule_name: s.schedule_name,
                    schedule_type: schedule_type.into(),
                    cron_expression: s.cron_expression.unwrap_or_default(),
                    interval_seconds: s.interval_seconds.unwrap_or(0),
                    status: status.into(),
                    next_run_at: format_opt_datetime(s.next_run_at),
                    last_run_at: format_opt_datetime(s.last_run_at),
                    last_instance_id: s
                        .last_instance_id
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                    created_at: format_opt_datetime(Some(s.created_at)),
                    updated_at: format_opt_datetime(Some(s.updated_at)),
                    jitter_seconds: s.jitter_seconds,
                }
            })
            .collect();

        Ok(tonic::Response::new(proto::ListSchedulesResponse {
            schedules: schedule_infos,
        }))
    }
}

/// Start a gRPC server for workflow registration.
pub async fn start_workflow_grpc_server(
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
    /// Returns an error if RAPPEL_DATABASE_URL is not set.
    pub async fn new(config: HarnessConfig<'_>) -> Result<Option<Self>> {
        Self::new_internal(config, true).await
    }

    /// Create a new test harness without starting the workflow instance.
    ///
    /// Returns an error if RAPPEL_DATABASE_URL is not set.
    pub async fn new_without_start(config: HarnessConfig<'_>) -> Result<Option<Self>> {
        Self::new_internal(config, false).await
    }

    async fn new_internal(config: HarnessConfig<'_>, start_instance: bool) -> Result<Option<Self>> {
        let database_url = env::var("RAPPEL_DATABASE_URL")
            .context("RAPPEL_DATABASE_URL is required for integration tests")?;

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
        // Use the new environment variable names for the bridge gRPC address
        let env_vars = vec![
            ("RAPPEL_BRIDGE_GRPC_PORT", "9999".to_string()),
            ("RAPPEL_BRIDGE_GRPC_ADDR", grpc_addr.to_string()),
            ("RAPPEL_SKIP_WAIT_FOR_INSTANCE", "1".to_string()),
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
             schedule_id, next_action_seq, input_payload, result_payload, status, \
             created_at, completed_at \
             FROM workflow_instances WHERE workflow_version_id = $1 ORDER BY created_at DESC LIMIT 1"
        )
        .bind(version_id.0)
        .fetch_all(database.pool())
        .await?;

        let instance = instances
            .first()
            .with_context(|| "no instance found after registration")?;
        let instance_id = WorkflowInstanceId(instance.id);

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
        let worker_pool = Arc::new(
            PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_bridge), None).await?,
        );
        info!("worker pool ready");

        // Create DAGRunner with the proper components
        let runner_config = RunnerConfig {
            batch_size: 10,
            max_slots_per_worker: 5,
            poll_interval_ms: 50,
            timeout_check_interval_ms: 1000,
            timeout_check_batch_size: 100,
            ..Default::default()
        };
        let runner = Arc::new(DAGRunner::new(
            runner_config,
            Arc::clone(&database),
            Arc::clone(&worker_pool),
        ));

        if start_instance {
            // Parse the stored input_payload from registration (contains initial context)
            let stored_inputs = if let Some(payload) = &instance.input_payload {
                parse_input_payload(payload)?
            } else {
                HashMap::new()
            };

            // Start the workflow instance using the DAGRunner
            // Merge stored inputs from Python registration with harness-provided inputs
            // (harness inputs override stored inputs if there's a conflict)
            let mut initial_inputs = stored_inputs;
            for (k, v) in build_initial_inputs(config.inputs) {
                initial_inputs.insert(k, v);
            }
            runner
                .start_instance(instance_id, initial_inputs)
                .await
                .context("failed to start workflow instance")?;
        }

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
    pub async fn run_to_completion(&self, timeout_secs: u64) -> Result<()> {
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

        Ok(())
    }

    /// Dispatch all queued actions and wait for completion.
    ///
    /// This uses the DAGRunner's main loop for completion handling, which triggers DAG
    /// traversal to enqueue successor actions. This properly handles:
    /// - Sequential workflows (action chains)
    /// - Parallel workflows (gather/spread)
    /// - Loops with multiple iterations
    pub async fn dispatch_all(&self) -> Result<()> {
        // Use run_to_completion with a reasonable timeout
        self.run_to_completion(60).await?;
        Ok(())
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

        // Give runner background tasks a moment to observe shutdown and drop references.
        let mut attempts = 0;
        while Arc::strong_count(&self.worker_pool) > 1 && attempts < 50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            attempts += 1;
        }

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
fn build_initial_inputs(pairs: &[(&str, &str)]) -> HashMap<String, WorkflowValue> {
    pairs
        .iter()
        .map(|(k, v)| {
            let parsed = serde_json::from_str::<serde_json::Value>(v).unwrap_or_else(|_| {
                // Fallback to string if not valid JSON literal
                serde_json::Value::String((*v).to_string())
            });
            ((*k).to_string(), WorkflowValue::from_json(&parsed))
        })
        .collect()
}

/// Parse stored input_payload (protobuf WorkflowArguments) to HashMap.
fn parse_input_payload(payload: &[u8]) -> Result<HashMap<String, WorkflowValue>> {
    if payload.is_empty() {
        return Ok(HashMap::new());
    }
    let arguments = proto::WorkflowArguments::decode(payload)
        .map_err(|err| anyhow::anyhow!("decode workflow arguments: {err}"))?;

    let mut result = HashMap::new();
    for arg in arguments.arguments {
        if let Some(value) = arg.value {
            result.insert(arg.key, WorkflowValue::from_proto(&value));
        }
    }
    Ok(result)
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

    let repo_python = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("python")
        .canonicalize()?;

    PY_ENV_READY
        .get_or_try_init(|| async { run_shell(repo_python.as_path(), "uv sync", &[], None).await })
        .await?;

    if !requirements.is_empty() {
        let extra_deps_toml = requirements
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
    "rappel",
    {extra_deps_toml}
]

[tool.uv.sources]
rappel = {{ path = "{}", editable = true }}
"#,
            repo_python.display()
        );
        fs::write(env_dir.path().join("pyproject.toml"), pyproject)?;

        run_shell(env_dir.path(), "uv sync", &[], None).await?;
    }

    // Build PYTHONPATH
    let mut python_paths = vec![
        env_dir.path().to_path_buf(),
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
    let run_command = if requirements.is_empty() {
        format!(
            "uv run --project {} python {entrypoint}",
            repo_python.display()
        )
    } else {
        format!("uv run python {entrypoint}")
    };
    run_shell(
        env_dir.path(),
        &run_command,
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
