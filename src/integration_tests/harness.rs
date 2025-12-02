//! Integration test harness using the new Store API.

use std::{env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration, sync::Once};

static INIT_TRACING: Once = Once::new();

fn init_tracing() {
    INIT_TRACING.call_once(|| {
        if std::env::var("RUST_LOG").is_ok() {
            tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_test_writer()
                .init();
        }
    });
}

use anyhow::{Context, Result, anyhow};
use prost::Message;
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle, time::sleep};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response as GrpcResponse, Status, async_trait, transport::Server};

use crate::{
    Store, PythonWorkerConfig, PythonWorkerPool, WorkflowInstanceId,
    dag::Dag,
    messages::proto::{self, NodeDispatch, WorkflowArguments, workflow_service_server::WorkflowServiceServer},
    server_worker::WorkerBridgeServer,
    store::ActionCompletion,
    worker::{ActionDispatchPayload, RoundTripMetrics},
};
use sqlx::postgres::PgPoolOptions;

/// Configuration for a workflow test
pub struct WorkflowHarnessConfig<'a> {
    pub files: &'a [(&'static str, &'static str)],
    pub entrypoint: &'static str,
    pub workflow_name: &'static str,
    pub user_module: &'static str,
    pub inputs: &'a [(&'static str, &'static str)],
}

/// Test harness that manages store, workers, and dispatching
pub struct WorkflowHarness {
    store: Arc<Store>,
    worker_server: Arc<WorkerBridgeServer>,
    pool: PythonWorkerPool,
    _python_env: TempDir,
    _registration_server: TestRegistrationServer,
    dag: Dag,
    expected_actions: usize,
    instance_id: WorkflowInstanceId,
}

impl WorkflowHarness {
    pub async fn new(config: WorkflowHarnessConfig<'_>) -> Result<Option<Self>> {
        init_tracing();

        let database_url = match env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("skipping integration test: DATABASE_URL not set");
                return Ok(None);
            }
        };

        // Connect to database
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await
            .context("Failed to connect to database")?;

        let store = Arc::new(Store::new(pool));
        store.init_schema().await.context("Failed to initialize schema")?;

        // Clean up any existing data
        cleanup_store(&store).await?;

        // Start GRPC registration server
        let registration_server = TestRegistrationServer::start(Arc::clone(&store)).await?;
        let grpc_port = registration_server.port();

        // Set up Python environment and run registration
        let python_env = setup_python_env(config.files, config.entrypoint, grpc_port).await?;

        // Wait a bit for registration to complete
        sleep(Duration::from_millis(500)).await;

        // Load the registered workflow
        let (workflow_id, dag) = store
            .get_workflow_by_name(config.workflow_name)
            .await?
            .ok_or_else(|| anyhow!("Workflow {} not registered", config.workflow_name))?;

        let expected_actions = dag.nodes.len();

        // Get the instance that was created during registration
        // (registration creates both the workflow and an initial instance)
        let instance_id = store
            .get_latest_instance_for_workflow(workflow_id)
            .await?
            .ok_or_else(|| anyhow!("No instance found for workflow {}", config.workflow_name))?;

        // Set up workers
        let worker_server = WorkerBridgeServer::start(None).await?;
        let worker_script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("python")
            .join(".venv")
            .join("bin")
            .join("rappel-worker");
        let worker_config = PythonWorkerConfig {
            script_path: worker_script,
            script_args: Vec::new(),
            user_module: config.user_module.to_string(),
            extra_python_paths: vec![python_env.path().to_path_buf()],
        };
        let pool = PythonWorkerPool::new(worker_config, 1, Arc::clone(&worker_server)).await?;

        Ok(Some(Self {
            store,
            worker_server,
            pool,
            _python_env: python_env,
            _registration_server: registration_server,
            dag,
            expected_actions,
            instance_id,
        }))
    }

    /// Dispatch all actions and process completions until workflow is done
    pub async fn dispatch_all(&self) -> Result<Vec<RoundTripMetrics>> {
        let mut completed = Vec::new();
        let mut max_iterations = self.expected_actions.saturating_mul(20).max(100);
        let mut idle_cycles = 0usize;

        while max_iterations > 0 {
            max_iterations -= 1;

            // Dequeue actions
            let mut actions = Vec::new();
            for _ in 0..16 {
                match self.store.dequeue_action().await? {
                    Some(action) => actions.push(action),
                    None => break,
                }
            }

            if actions.is_empty() {
                idle_cycles = idle_cycles.saturating_add(1);
                if idle_cycles >= 3 && completed.len() >= self.expected_actions {
                    break;
                }
                // Check if workflow is complete
                if let Some((status, _)) = self.store.get_instance(self.instance_id).await? {
                    if status == "completed" || status == "failed" {
                        break;
                    }
                }
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            idle_cycles = 0;

            for action in actions {
                // Decode and dispatch to worker
                let dispatch: NodeDispatch = serde_json::from_str(&action.dispatch_json)
                    .context("Failed to decode dispatch")?;

                // Check if this is a sleep action (no-op)
                let is_sleep = dispatch.node.as_ref()
                    .map(|n| n.action == "__sleep__")
                    .unwrap_or(false);

                if is_sleep {
                    // Sleep actions complete immediately (the delay was in the queue)
                    let completion = ActionCompletion {
                        action_id: action.id,
                        node_id: action.node_id,
                        instance_id: action.instance_id,
                        success: true,
                        result: None,
                        exception_type: None,
                        exception_module: None,
                    };
                    self.store.complete_action(completion).await?;
                    continue;
                }

                let payload = ActionDispatchPayload {
                    action_id: action.id,
                    instance_id: action.instance_id,
                    sequence: action.attempt,
                    dispatch,
                    timeout_seconds: action.timeout_seconds,
                    max_retries: action.max_retries,
                    attempt_number: action.attempt,
                    dispatch_token: action.id,
                };

                let worker = self.pool.next_worker();
                let metrics = worker.send_action(payload).await?;

                // Process completion
                let decoded = decode_result(&metrics.response_payload);
                let completion = ActionCompletion {
                    action_id: metrics.action_id,
                    node_id: action.node_id,
                    instance_id: action.instance_id,
                    success: metrics.success,
                    result: decoded.result,
                    exception_type: decoded.exception_type,
                    exception_module: decoded.exception_module,
                };
                self.store.complete_action(completion).await?;

                completed.push(metrics);
            }
        }

        Ok(completed)
    }

    /// Get the stored result for the workflow instance
    pub async fn stored_result(&self) -> Result<Option<serde_json::Value>> {
        if let Some((status, result)) = self.store.get_instance(self.instance_id).await? {
            if status == "completed" {
                return Ok(result);
            }
        }
        Ok(None)
    }

    pub fn dag(&self) -> &Dag {
        &self.dag
    }

    pub fn expected_actions(&self) -> usize {
        self.expected_actions
    }

    pub fn instance_id(&self) -> WorkflowInstanceId {
        self.instance_id
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub async fn shutdown(self) -> Result<()> {
        self.pool.shutdown().await?;
        self.worker_server.shutdown().await;
        Ok(())
    }
}

/// Clean up database tables
async fn cleanup_store(store: &Store) -> Result<()> {
    // Delete in order respecting foreign keys
    sqlx::query("DELETE FROM action_queue")
        .execute(store.pool())
        .await
        .ok(); // Ignore errors if table doesn't exist
    sqlx::query("DELETE FROM instances")
        .execute(store.pool())
        .await
        .ok();
    sqlx::query("DELETE FROM workflows")
        .execute(store.pool())
        .await
        .ok();
    Ok(())
}

/// Set up Python environment with test files
async fn setup_python_env(
    files: &[(&str, &str)],
    entrypoint: &str,
    grpc_port: u16,
) -> Result<TempDir> {
    use std::process::Command;
    use std::fs;

    let temp_dir = TempDir::new()?;

    // Write all test files
    for (name, content) in files {
        let path = temp_dir.path().join(name);
        fs::write(&path, content)?;
    }

    // Write entrypoint script
    let entrypoint_path = temp_dir.path().join("__entrypoint__.py");
    fs::write(&entrypoint_path, entrypoint)?;

    // Get Python paths
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let python_src = repo_root.join("python").join("src");
    let python_dir = repo_root.join("python");
    let proto_dir = python_dir.join("proto");

    let python_path = format!(
        "{}:{}:{}:{}",
        temp_dir.path().display(),
        python_src.display(),
        python_dir.display(),
        proto_dir.display()
    );

    // Run the entrypoint
    let python_bin = env::var("PYTHON_BIN").unwrap_or_else(|_| "uv".to_string());
    let mut cmd = Command::new(&python_bin);

    // Check if using uv
    if python_bin.ends_with("uv") || python_bin == "uv" {
        cmd.arg("run").arg("python");
    }

    // The GRPC_ADDR directly specifies where to connect for GRPC
    // The SERVER_PORT is needed to prevent the bridge from trying to boot a singleton
    cmd.arg(&entrypoint_path)
        .env("PYTHONPATH", &python_path)
        .env("CARABINER_SKIP_WAIT_FOR_INSTANCE", "1")
        .env("CARABINER_GRPC_ADDR", format!("127.0.0.1:{}", grpc_port))
        .env("CARABINER_SERVER_PORT", (grpc_port - 1).to_string())
        .current_dir(&python_dir);

    // Use spawn + wait_with_output with a timeout to avoid hanging
    let child = cmd.spawn().context("Failed to spawn Python entrypoint")?;
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::task::spawn_blocking(move || {
            child.wait_with_output()
        })
    ).await
    .context("Python entrypoint timed out after 30s")?
    .context("Task join failed")?
    .context("Failed to run Python entrypoint")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(anyhow!(
            "Python entrypoint failed:\nstdout: {}\nstderr: {}",
            stdout,
            stderr
        ));
    }

    Ok(temp_dir)
}

/// Encode workflow inputs as protobuf
fn encode_workflow_input(inputs: &[(&str, &str)]) -> Vec<u8> {
    let arguments: Vec<proto::WorkflowArgument> = inputs
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

    let args = WorkflowArguments { arguments };
    args.encode_to_vec()
}

/// Result of decoding a worker response payload
struct DecodedResult {
    result: Option<serde_json::Value>,
    exception_type: Option<String>,
    exception_module: Option<String>,
}

/// Decode result payload from worker response
fn decode_result(payload: &[u8]) -> DecodedResult {
    if payload.is_empty() {
        return DecodedResult {
            result: None,
            exception_type: None,
            exception_module: None,
        };
    }

    let args = match proto::WorkflowArguments::decode(payload) {
        Ok(a) => a,
        Err(_) => return DecodedResult {
            result: None,
            exception_type: None,
            exception_module: None,
        },
    };

    // Extract result (for success case)
    let result = args.arguments.iter()
        .find(|a| a.key == "result")
        .and_then(|arg| arg.value.as_ref())
        .map(proto_value_to_json);

    // Extract exception info (for failure case)
    let (exception_type, exception_module) = args.arguments.iter()
        .find(|a| a.key == "error")
        .and_then(|arg| arg.value.as_ref())
        .and_then(|v| {
            use proto::workflow_argument_value::Kind;
            if let Some(Kind::Exception(e)) = &v.kind {
                Some((Some(e.r#type.clone()), Some(e.module.clone())))
            } else {
                None
            }
        })
        .unwrap_or((None, None));

    DecodedResult {
        result,
        exception_type,
        exception_module,
    }
}

/// Convert proto value to JSON
fn proto_value_to_json(value: &proto::WorkflowArgumentValue) -> serde_json::Value {
    use proto::workflow_argument_value::Kind;
    use proto::primitive_workflow_argument::Kind as PrimitiveKind;

    match &value.kind {
        Some(Kind::Primitive(p)) => match &p.kind {
            Some(PrimitiveKind::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(PrimitiveKind::IntValue(i)) => serde_json::Value::Number((*i).into()),
            Some(PrimitiveKind::DoubleValue(d)) => {
                serde_json::Number::from_f64(*d)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
            Some(PrimitiveKind::BoolValue(b)) => serde_json::Value::Bool(*b),
            Some(PrimitiveKind::NullValue(_)) => serde_json::Value::Null,
            None => serde_json::Value::Null,
        },
        Some(Kind::ListValue(list)) => {
            serde_json::Value::Array(list.items.iter().map(proto_value_to_json).collect())
        }
        Some(Kind::TupleValue(tuple)) => {
            serde_json::Value::Array(tuple.items.iter().map(proto_value_to_json).collect())
        }
        Some(Kind::DictValue(dict)) => {
            let map: serde_json::Map<String, serde_json::Value> = dict.entries.iter()
                .filter_map(|entry| {
                    entry.value.as_ref().map(|v| (entry.key.clone(), proto_value_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(map)
        }
        Some(Kind::Basemodel(model)) => {
            // Convert Pydantic BaseModel to JSON object with "variables" key
            // The model.data contains the dict entries (e.g., {"variables": {...}})
            if let Some(data) = &model.data {
                let data_map: serde_json::Map<String, serde_json::Value> = data.entries.iter()
                    .filter_map(|entry| {
                        entry.value.as_ref().map(|v| (entry.key.clone(), proto_value_to_json(v)))
                    })
                    .collect();
                serde_json::Value::Object(data_map)
            } else {
                serde_json::Value::Null
            }
        }
        _ => serde_json::Value::Null,
    }
}

/// GRPC server for workflow registration during tests
struct TestRegistrationServer {
    addr: SocketAddr,
    handle: JoinHandle<()>,
}

impl TestRegistrationServer {
    async fn start(store: Arc<Store>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = TcpListenerStream::new(listener);

        let service = TestWorkflowService { store };
        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(WorkflowServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await;
        });

        Ok(Self { addr, handle })
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }

    fn shutdown(self) {
        self.handle.abort();
    }
}

#[derive(Clone)]
struct TestWorkflowService {
    store: Arc<Store>,
}

#[async_trait]
impl proto::workflow_service_server::WorkflowService for TestWorkflowService {
    async fn register_workflow(
        &self,
        request: Request<proto::RegisterWorkflowRequest>,
    ) -> Result<GrpcResponse<proto::RegisterWorkflowResponse>, Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| Status::invalid_argument("registration missing"))?;

        let (version_id, instance_id) = self.store
            .register_workflow(&registration)
            .await
            .map_err(|e| {
                eprintln!("register_workflow error: {:?}", e);
                Status::internal(format!("{:?}", e))
            })?;

        Ok(GrpcResponse::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_id: instance_id.to_string(),
        }))
    }

    async fn wait_for_instance(
        &self,
        _request: Request<proto::WaitForInstanceRequest>,
    ) -> Result<GrpcResponse<proto::WaitForInstanceResponse>, Status> {
        // Not used in tests - we skip waiting
        Err(Status::unimplemented("not used in tests"))
    }
}
