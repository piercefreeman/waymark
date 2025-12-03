//! Integration test harness.
//!
//! Provides a complete test environment with:
//! - Database connection
//! - Worker bridge server
//! - Python worker pool
//! - gRPC service for workflow registration
//! - Utilities for workflow registration and dispatch

#![allow(dead_code)]

use std::{
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
    sync::oneshot,
    task::JoinHandle,
    time::{Duration, timeout},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;

use rappel::{
    ActionDispatchPayload, BackoffKind, Database, NewAction, PythonWorkerConfig, PythonWorkerPool,
    RoundTripMetrics, WorkerBridgeServer, WorkflowInstanceId, WorkflowVersionId, proto,
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
        let interval = Duration::from_secs_f64(inner.poll_interval_secs.max(0.1).min(30.0));
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
    database: Database,
    worker_bridge: Arc<WorkerBridgeServer>,
    worker_pool: PythonWorkerPool,
    python_env: TempDir,
    version_id: WorkflowVersionId,
    instance_id: WorkflowInstanceId,
    expected_actions: usize,
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
        let database = Database::connect(&database_url).await?;
        cleanup_database(&database).await?;

        // Start the workflow registration gRPC server
        let (grpc_addr, grpc_shutdown, grpc_handle) =
            start_workflow_grpc_server(database.clone()).await?;
        info!(%grpc_addr, "workflow registration gRPC server started");

        // Start worker bridge (for worker connections)
        let worker_bridge = WorkerBridgeServer::start(None).await?;
        info!(addr = %worker_bridge.addr(), "worker bridge started");

        // Set up Python environment and run registration script
        // - CARABINER_SERVER_PORT: Set to prevent Python from trying to boot a server
        //   (The Python bridge checks this before trying to spawn boot-rappel-singleton)
        // - CARABINER_GRPC_ADDR: Points to our workflow registration server
        // - CARABINER_SKIP_WAIT_FOR_INSTANCE: Tells Python not to block waiting for results
        let env_vars = vec![
            ("CARABINER_SERVER_PORT", "9999".to_string()), // Dummy port to prevent singleton boot
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

        // Load the version to get action count for expected completions
        let full_version = database.get_workflow_version(version_id).await?;

        // Decode program to count actions
        let program = rappel::ir_ast::Program::decode(&full_version.program_proto[..])
            .context("decode program proto")?;
        let expected_actions = count_actions_in_program(&program);
        info!(expected_actions, "workflow registered with actions");

        // Find the instance that was created during registration
        // (the RegisterWorkflow gRPC call creates both version and instance)
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

        // Enqueue the first action(s) based on the DAG
        enqueue_initial_actions(&database, instance_id, &program, config.user_module).await?;

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
            PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_bridge)).await?;
        info!("worker pool ready");

        Ok(Some(Self {
            database,
            worker_bridge,
            worker_pool,
            python_env,
            version_id,
            instance_id,
            expected_actions,
            grpc_shutdown: Some(grpc_shutdown),
            grpc_handle: Some(grpc_handle),
        }))
    }

    /// Dispatch all queued actions and wait for completion.
    pub async fn dispatch_all(&self) -> Result<Vec<RoundTripMetrics>> {
        dispatch_all_actions(&self.database, &self.worker_pool, self.expected_actions).await
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

    /// Get the expected number of actions.
    pub fn expected_actions(&self) -> usize {
        self.expected_actions
    }

    /// Shut down the harness.
    pub async fn shutdown(mut self) -> Result<()> {
        self.worker_pool.shutdown().await?;
        self.worker_bridge.shutdown().await;

        // Shutdown gRPC server
        if let Some(shutdown_tx) = self.grpc_shutdown.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.grpc_handle.take() {
            let _ = handle.await;
        }

        drop(self.python_env);
        Ok(())
    }
}

/// Clean up the database before each test.
async fn cleanup_database(db: &Database) -> Result<()> {
    sqlx::query("TRUNCATE action_queue, instance_context, loop_state, workflow_instances, workflow_versions CASCADE")
        .execute(db.pool())
        .await?;
    Ok(())
}

/// Purge instances with empty input (created during registration).
async fn purge_empty_instances(db: &Database) -> Result<()> {
    sqlx::query("DELETE FROM workflow_instances WHERE input_payload IS NULL")
        .execute(db.pool())
        .await?;
    Ok(())
}

/// Count action calls in a program.
fn count_actions_in_program(program: &rappel::ir_ast::Program) -> usize {
    let mut count = 0;
    for func in &program.functions {
        if let Some(body) = &func.body {
            count += count_actions_in_block(body);
        }
    }
    count.max(1) // At least 1 action expected
}

fn count_actions_in_block(block: &rappel::ir_ast::Block) -> usize {
    use rappel::ir_ast::statement::Kind;

    let mut count = 0;
    for stmt in &block.statements {
        match &stmt.kind {
            Some(Kind::ActionCall(_)) => count += 1,
            Some(Kind::Conditional(cond)) => {
                // Count actions in the if branch
                if let Some(if_branch) = &cond.if_branch {
                    if let Some(body) = &if_branch.body {
                        count += count_actions_in_block(body);
                    }
                }
                // Count actions in elif branches
                for elif in &cond.elif_branches {
                    if let Some(body) = &elif.body {
                        count += count_actions_in_block(body);
                    }
                }
                // Count actions in else branch
                if let Some(else_branch) = &cond.else_branch {
                    if let Some(body) = &else_branch.body {
                        count += count_actions_in_block(body);
                    }
                }
            }
            Some(Kind::ForLoop(for_loop)) => {
                if let Some(body) = &for_loop.body {
                    count += count_actions_in_block(body);
                }
            }
            Some(Kind::TryExcept(try_except)) => {
                if let Some(body) = &try_except.try_body {
                    count += count_actions_in_block(body);
                }
                for handler in &try_except.handlers {
                    if let Some(handler_block) = &handler.body {
                        count += count_actions_in_block(handler_block);
                    }
                }
            }
            Some(Kind::SpreadAction(_)) => count += 1,
            Some(Kind::ParallelBlock(parallel)) => {
                count += parallel.calls.len();
            }
            _ => {}
        }
    }
    count
}

/// Enqueue initial actions from the workflow program.
async fn enqueue_initial_actions(
    db: &Database,
    instance_id: WorkflowInstanceId,
    program: &rappel::ir_ast::Program,
    module_name: &str,
) -> Result<()> {
    use rappel::ir_ast::statement::Kind;

    // Find action calls in the first function
    if let Some(func) = program.functions.first() {
        if let Some(body) = &func.body {
            for (idx, stmt) in body.statements.iter().enumerate() {
                if let Some(Kind::ActionCall(action_call)) = &stmt.kind {
                    // Build kwargs JSON
                    let mut kwargs = serde_json::Map::new();
                    for kwarg in &action_call.kwargs {
                        if let Some(value) = &kwarg.value {
                            kwargs.insert(kwarg.name.clone(), expr_to_json(value));
                        }
                    }

                    let dispatch_payload = serde_json::to_vec(&serde_json::Value::Object(kwargs))?;

                    // Use module_name from action_call if available, otherwise fall back to config
                    let action_module = if action_call.module_name.is_none()
                        || action_call
                            .module_name
                            .as_ref()
                            .map(|s| s.is_empty())
                            .unwrap_or(true)
                    {
                        module_name.to_string()
                    } else {
                        action_call.module_name.clone().unwrap_or_default()
                    };

                    let action = NewAction {
                        instance_id,
                        module_name: action_module,
                        action_name: action_call.action_name.clone(),
                        dispatch_payload,
                        timeout_seconds: 300,
                        max_retries: 3,
                        backoff_kind: BackoffKind::Exponential,
                        backoff_base_delay_ms: 1000,
                        node_id: Some(format!("node_{}", idx)),
                    };

                    db.enqueue_action(action).await?;
                    info!(action_name = %action_call.action_name, "enqueued initial action");
                }
            }
        }
    }
    Ok(())
}

/// Convert an IR expression to JSON (for kwargs).
fn expr_to_json(expr: &rappel::ir_ast::Expr) -> serde_json::Value {
    use rappel::ir_ast::expr::Kind;

    match &expr.kind {
        Some(Kind::Literal(lit)) => literal_to_json(lit),
        Some(Kind::Variable(var)) => serde_json::Value::String(format!("${}", var.name)),
        Some(Kind::List(list)) => {
            let items: Vec<_> = list.elements.iter().map(expr_to_json).collect();
            serde_json::Value::Array(items)
        }
        Some(Kind::Dict(dict)) => {
            let mut map = serde_json::Map::new();
            for entry in &dict.entries {
                if let (Some(key), Some(value)) = (&entry.key, &entry.value) {
                    let key_str = match expr_to_json(key) {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    map.insert(key_str, expr_to_json(value));
                }
            }
            serde_json::Value::Object(map)
        }
        _ => serde_json::Value::Null,
    }
}

fn literal_to_json(lit: &rappel::ir_ast::Literal) -> serde_json::Value {
    use rappel::ir_ast::literal::Value;

    match &lit.value {
        Some(Value::IntValue(i)) => serde_json::Value::Number((*i).into()),
        Some(Value::FloatValue(f)) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(Value::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(Value::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(Value::IsNone(true)) => serde_json::Value::Null,
        _ => serde_json::Value::Null,
    }
}

/// Encode workflow input as protobuf bytes.
fn encode_workflow_input(pairs: &[(&str, &str)]) -> Vec<u8> {
    let arguments: Vec<proto::WorkflowArgument> = pairs
        .iter()
        .map(|(key, value)| proto::WorkflowArgument {
            key: (*key).to_string(),
            value: Some(proto::WorkflowArgumentValue {
                kind: Some(proto::workflow_argument_value::Kind::Primitive(
                    proto::PrimitiveWorkflowArgument {
                        kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                            (*value).to_string(),
                        )),
                    },
                )),
            }),
        })
        .collect();

    proto::WorkflowArguments { arguments }.encode_to_vec()
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

/// Dispatch all actions from the queue to workers.
async fn dispatch_all_actions(
    database: &Database,
    pool: &PythonWorkerPool,
    max_actions: usize,
) -> Result<Vec<RoundTripMetrics>> {
    let mut completed = Vec::new();
    let mut max_iterations = max_actions.saturating_mul(20).max(100);
    let mut idle_cycles = 0usize;

    while max_iterations > 0 {
        max_iterations -= 1;

        let actions = database.dispatch_actions(16).await?;
        if actions.is_empty() {
            idle_cycles = idle_cycles.saturating_add(1);
            if idle_cycles >= 5 && !completed.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
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

            // Mark action as completed
            let record = rappel::CompletionRecord {
                action_id: rappel::ActionId(action.id),
                success: metrics.success,
                result_payload: metrics.response_payload.clone(),
                delivery_token: metrics.dispatch_token.unwrap_or(action.delivery_token),
                error_message: metrics.error_message.clone(),
            };
            database.complete_action(record).await?;
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
