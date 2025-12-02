//! Action benchmark harness using the new Store/Scheduler architecture.
//!
//! This benchmark tests raw action dispatch throughput by running a simple
//! echo workflow that exercises the full action dispatch path without
//! complex workflow logic.

use anyhow::{Context, Result, anyhow};
use prost::Message;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response as GrpcResponse, Status, async_trait, transport::Server};
use tracing::info;

use crate::benchmark::common::BenchmarkSummary;
use crate::benchmark::fixtures;
use crate::messages::proto::{self, NodeDispatch, workflow_service_server::WorkflowServiceServer};
use crate::server_worker::WorkerBridgeServer;
use crate::store::{ActionCompletion, Store};
use crate::worker::{ActionDispatchPayload, PythonWorkerConfig, PythonWorkerPool};

/// Configuration for action benchmark
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub total_messages: usize,
    pub in_flight: usize,
    pub payload_size: usize,
    pub progress_interval: Option<Duration>,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            total_messages: 10_000,
            in_flight: 32,
            payload_size: 4096,
            progress_interval: None,
        }
    }
}

/// Action benchmark harness
pub struct BenchmarkHarness {
    store: Arc<Store>,
    worker_server: Arc<WorkerBridgeServer>,
    pool: PythonWorkerPool,
    _python_env: TempDir,
    _registration_server: BenchmarkRegistrationServer,
    workflow_id: uuid::Uuid,
}

impl BenchmarkHarness {
    /// Create a new benchmark harness
    pub async fn new(
        worker_config: PythonWorkerConfig,
        worker_count: usize,
        store: Store,
    ) -> Result<Self> {
        let store = Arc::new(store);
        store
            .init_schema()
            .await
            .context("Failed to initialize schema")?;

        // Clean up any existing data
        cleanup_store(&store).await?;

        // Start GRPC registration server
        let registration_server = BenchmarkRegistrationServer::start(Arc::clone(&store)).await?;
        let grpc_port = registration_server.port();

        // Set up Python environment and run registration
        let python_env = setup_python_env(grpc_port).await?;

        // Wait for registration to complete
        sleep(Duration::from_millis(500)).await;

        // Load the registered workflow
        let (workflow_id, _dag) = store
            .get_workflow_by_name("benchmark.echo_action")
            .await?
            .ok_or_else(|| anyhow!("Benchmark workflow not registered"))?;

        // Set up workers
        let worker_server = WorkerBridgeServer::start(None).await?;
        let pool =
            PythonWorkerPool::new(worker_config, worker_count, Arc::clone(&worker_server)).await?;

        Ok(Self {
            store,
            worker_server,
            pool,
            _python_env: python_env,
            _registration_server: registration_server,
            workflow_id,
        })
    }

    /// Run the benchmark with the given configuration
    pub async fn run(&self, config: &HarnessConfig) -> Result<BenchmarkSummary> {
        let mut results = Vec::with_capacity(config.total_messages);
        let start = Instant::now();
        let mut last_progress = Instant::now();

        // Build payload for all instances
        let payload = "x".repeat(config.payload_size);
        let workflow_input = encode_workflow_input(&[("payload", &payload)]);

        // Create instances to drive the benchmark
        let mut pending_instances = Vec::new();
        let mut completed_count = 0usize;

        while completed_count < config.total_messages {
            // Launch instances up to in_flight limit
            while pending_instances.len() < config.in_flight
                && (pending_instances.len() + completed_count) < config.total_messages
            {
                let instance_id = self
                    .store
                    .create_instance_with_input(self.workflow_id, &workflow_input)
                    .await?;
                pending_instances.push(instance_id);
            }

            // Process pending actions
            let batch_results = self.process_pending_actions().await?;
            results.extend(batch_results);

            // Check for completed instances
            let mut still_pending = Vec::new();
            for instance_id in pending_instances.drain(..) {
                match self.store.get_instance(instance_id).await? {
                    Some((status, _)) if status == "completed" || status == "failed" => {
                        completed_count += 1;
                    }
                    _ => {
                        still_pending.push(instance_id);
                    }
                }
            }
            pending_instances = still_pending;

            // Progress reporting
            if let Some(interval) = config.progress_interval
                && last_progress.elapsed() >= interval
            {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = completed_count as f64 / elapsed;
                info!(
                    completed = completed_count,
                    total = config.total_messages,
                    rate = format!("{:.0} msg/s", rate),
                    "benchmark progress"
                );
                last_progress = Instant::now();
            }

            // Small delay to prevent busy-loop
            if pending_instances.is_empty() && completed_count < config.total_messages {
                sleep(Duration::from_millis(1)).await;
            }
        }

        let elapsed = start.elapsed();
        Ok(BenchmarkSummary::from_results(results, elapsed))
    }

    /// Process all pending actions in the queue
    async fn process_pending_actions(
        &self,
    ) -> Result<Vec<crate::benchmark::common::BenchmarkResult>> {
        let mut results = Vec::new();

        loop {
            let action = match self.store.dequeue_action().await? {
                Some(a) => a,
                None => break,
            };

            // Decode and dispatch to worker
            let dispatch: NodeDispatch =
                serde_json::from_str(&action.dispatch_json).context("Failed to decode dispatch")?;

            // Check if this is a sleep action (no-op)
            let is_sleep = dispatch
                .node
                .as_ref()
                .map(|n| n.action == "__sleep__")
                .unwrap_or(false);

            if is_sleep {
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

            results.push(metrics.into());
        }

        Ok(results)
    }

    /// Shutdown the harness
    pub async fn shutdown(self) -> Result<()> {
        self.pool.shutdown().await?;
        self.worker_server.shutdown().await;
        Ok(())
    }
}

/// Clean up database tables
async fn cleanup_store(store: &Store) -> Result<()> {
    sqlx::query("DELETE FROM action_queue")
        .execute(store.pool())
        .await
        .ok();
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

/// Set up Python environment with benchmark fixtures
async fn setup_python_env(grpc_port: u16) -> Result<TempDir> {
    use std::fs;
    use std::process::Command;

    let temp_dir = TempDir::new()?;

    // Write fixture files
    fs::write(temp_dir.path().join("__init__.py"), fixtures::INIT_PY)?;
    fs::write(
        temp_dir.path().join("benchmark_common.py"),
        fixtures::BENCHMARK_COMMON,
    )?;
    fs::write(
        temp_dir.path().join("benchmark_actions.py"),
        fixtures::BENCHMARK_ACTIONS,
    )?;

    // Write entrypoint script that registers the workflow by calling run()
    let entrypoint = r#"
import asyncio
from benchmark_actions import EchoActionWorkflow

if __name__ == "__main__":
    # Running the workflow triggers registration with the gRPC server
    asyncio.run(EchoActionWorkflow().run())
"#;
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
    let python_bin = std::env::var("PYTHON_BIN").unwrap_or_else(|_| "uv".to_string());
    let mut cmd = Command::new(&python_bin);

    if python_bin.ends_with("uv") || python_bin == "uv" {
        cmd.arg("run").arg("python");
    }

    cmd.arg(&entrypoint_path)
        .env("PYTHONPATH", &python_path)
        .env("CARABINER_SKIP_WAIT_FOR_INSTANCE", "1")
        .env("CARABINER_GRPC_ADDR", format!("127.0.0.1:{}", grpc_port))
        .env("CARABINER_SERVER_PORT", (grpc_port - 1).to_string())
        .current_dir(&python_dir);

    let child = cmd.spawn().context("Failed to spawn Python entrypoint")?;
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::task::spawn_blocking(move || child.wait_with_output()),
    )
    .await
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

    let args = proto::WorkflowArguments { arguments };
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
        Err(_) => {
            return DecodedResult {
                result: None,
                exception_type: None,
                exception_module: None,
            };
        }
    };

    let result = args
        .arguments
        .iter()
        .find(|a| a.key == "result")
        .and_then(|arg| arg.value.as_ref())
        .map(proto_value_to_json);

    let (exception_type, exception_module) = args
        .arguments
        .iter()
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
    use proto::primitive_workflow_argument::Kind as PrimitiveKind;
    use proto::workflow_argument_value::Kind;

    match &value.kind {
        Some(Kind::Primitive(p)) => match &p.kind {
            Some(PrimitiveKind::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(PrimitiveKind::IntValue(i)) => serde_json::Value::Number((*i).into()),
            Some(PrimitiveKind::DoubleValue(d)) => serde_json::Number::from_f64(*d)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
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
            let map: serde_json::Map<String, serde_json::Value> = dict
                .entries
                .iter()
                .filter_map(|entry| {
                    entry
                        .value
                        .as_ref()
                        .map(|v| (entry.key.clone(), proto_value_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(map)
        }
        Some(Kind::Basemodel(model)) => {
            if let Some(data) = &model.data {
                let data_map: serde_json::Map<String, serde_json::Value> = data
                    .entries
                    .iter()
                    .filter_map(|entry| {
                        entry
                            .value
                            .as_ref()
                            .map(|v| (entry.key.clone(), proto_value_to_json(v)))
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

/// GRPC server for workflow registration during benchmarks
struct BenchmarkRegistrationServer {
    addr: std::net::SocketAddr,
    handle: JoinHandle<()>,
}

impl BenchmarkRegistrationServer {
    async fn start(store: Arc<Store>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = TcpListenerStream::new(listener);

        let service = BenchmarkWorkflowService { store };
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
}

impl Drop for BenchmarkRegistrationServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Clone)]
struct BenchmarkWorkflowService {
    store: Arc<Store>,
}

#[async_trait]
impl proto::workflow_service_server::WorkflowService for BenchmarkWorkflowService {
    async fn register_workflow(
        &self,
        request: Request<proto::RegisterWorkflowRequest>,
    ) -> Result<GrpcResponse<proto::RegisterWorkflowResponse>, Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| Status::invalid_argument("registration missing"))?;

        let (version_id, instance_id) = self
            .store
            .register_workflow(&registration)
            .await
            .map_err(|e| Status::internal(format!("{:?}", e)))?;

        Ok(GrpcResponse::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_id: instance_id.to_string(),
        }))
    }

    async fn wait_for_instance(
        &self,
        _request: Request<proto::WaitForInstanceRequest>,
    ) -> Result<GrpcResponse<proto::WaitForInstanceResponse>, Status> {
        Err(Status::unimplemented("not used in benchmarks"))
    }
}
