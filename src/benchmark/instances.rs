use std::{
    env, fs,
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use prost::Message;
use tempfile::TempDir;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{Instrument, info, warn};

use crate::{
    WorkflowVersionId,
    benchmark::{
        common::{BenchmarkResult, BenchmarkSummary, spawn_completion_worker},
        fixtures,
    },
    db::{CompletionRecord, Database},
    instances,
    messages::{
        MessageError,
        proto::{self, WorkflowRegistration},
    },
    server_worker::WorkerBridgeServer,
    worker::{ActionDispatchPayload, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics},
};

#[derive(Debug, Clone)]
pub struct WorkflowBenchmarkConfig {
    pub instance_count: usize,
    pub in_flight: usize,
    pub batch_size: usize,
    pub payload_size: usize,
    pub progress_interval: Option<Duration>,
}

impl Default for WorkflowBenchmarkConfig {
    fn default() -> Self {
        Self {
            instance_count: 100,
            in_flight: 32,
            batch_size: 4,
            payload_size: 1024,
            progress_interval: None,
        }
    }
}

pub struct WorkflowBenchmarkHarness {
    worker_server: Arc<WorkerBridgeServer>,
    database: Database,
    workers: PythonWorkerPool,
    completion_tx: mpsc::Sender<CompletionRecord>,
    completion_handle: JoinHandle<()>,
    _temp_dir: TempDir,
    _user_module: String,
    workflow_name: String,
    workflow_version_id: WorkflowVersionId,
    dag_node_count: usize,
}

impl WorkflowBenchmarkHarness {
    pub async fn new(
        database_url: &str,
        database: Database,
        worker_count: usize,
        mut worker_config: PythonWorkerConfig,
    ) -> Result<Self> {
        let temp_dir = TempDir::new().context("create temp dir for workflow benchmark")?;
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let package_dir = temp_dir.path().join("workflow_bench");
        fs::create_dir_all(&package_dir).context("create workflow package dir")?;
        fs::write(
            package_dir.join("__init__.py"),
            fixtures::INIT_PY.trim_start().as_bytes(),
        )
        .context("write package __init__")?;
        fs::write(
            package_dir.join("benchmark_common.py"),
            fixtures::BENCHMARK_COMMON.trim_start().as_bytes(),
        )
        .context("write benchmark_common fixture")?;
        fs::write(
            package_dir.join("benchmark_instances.py"),
            fixtures::BENCHMARK_INSTANCES.trim_start().as_bytes(),
        )
        .context("write benchmark_instances fixture")?;
        let user_module = "workflow_bench.benchmark_instances".to_string();

        let python_paths = vec![
            temp_dir.path().to_path_buf(),
            repo_root.join("python/src"),
            repo_root.join("python"),
        ];
        let registration_payload = build_registration_payload(&python_paths, &user_module)?;
        let registration = WorkflowRegistration::decode(registration_payload.as_slice())
            .context("decode workflow registration")?;
        let (version_id, _) = instances::run_instance_payload(database_url, &registration_payload)
            .await
            .context("register workflow version")?;
        worker_config.user_module = user_module.clone();
        worker_config.extra_python_paths = vec![temp_dir.path().to_path_buf()];
        let worker_server = WorkerBridgeServer::start(None).await?;
        let workers =
            PythonWorkerPool::new(worker_config, worker_count, Arc::clone(&worker_server)).await?;
        let (completion_tx, completion_handle) = spawn_completion_worker(database.clone());
        let dag_node_count = registration
            .dag
            .as_ref()
            .map(|dag| dag.nodes.len())
            .unwrap_or(0);
        Ok(Self {
            worker_server,
            database,
            workers,
            completion_tx,
            completion_handle,
            _temp_dir: temp_dir,
            _user_module: user_module,
            workflow_name: registration.workflow_name,
            workflow_version_id: version_id,
            dag_node_count,
        })
    }

    pub async fn run(&self, config: &WorkflowBenchmarkConfig) -> Result<BenchmarkSummary> {
        self.database.reset_workflow_state().await?;
        let input_payload = build_workflow_input(config.batch_size, config.payload_size);
        for _ in 0..config.instance_count {
            self.database
                .create_workflow_instance(
                    &self.workflow_name,
                    self.workflow_version_id,
                    Some(&input_payload),
                )
                .await?;
        }

        let total_actions = config.instance_count * self.dag_node_count.max(1);
        let mut completed = Vec::with_capacity(total_actions);
        let mut inflight: FuturesUnordered<BoxFuture<'_, Result<RoundTripMetrics, MessageError>>> =
            FuturesUnordered::new();
        let mut dispatched = 0usize;
        let start = Instant::now();
        let worker_count = self.workers.len().max(1);
        let max_inflight = config.in_flight.max(1) * worker_count;
        let mut last_report = start;

        while completed.len() < total_actions {
            while inflight.len() < max_inflight && dispatched < total_actions {
                let needed = (max_inflight - inflight.len()).min(total_actions - dispatched);
                let actions = self.database.dispatch_actions(needed as i64).await?;
                if actions.is_empty() {
                    break;
                }
                for action in actions {
                    let dispatch =
                        proto::WorkflowNodeDispatch::decode(action.dispatch_payload.as_slice())
                            .map_err(|err| anyhow!("failed to decode workflow dispatch: {err}"))?;
                    let payload = ActionDispatchPayload {
                        action_id: action.id,
                        instance_id: action.instance_id,
                        sequence: action.action_seq,
                        dispatch,
                    };
                    let worker = self.workers.next_worker();
                    let span = tracing::debug_span!(
                        "dispatch",
                        action_id = %payload.action_id,
                        instance_id = %payload.instance_id,
                        sequence = payload.sequence
                    );
                    let fut: BoxFuture<'_, Result<RoundTripMetrics, MessageError>> =
                        Box::pin(async move { worker.send_action(payload).await }.instrument(span));
                    inflight.push(fut);
                    dispatched += 1;
                }
            }

            match inflight.next().await {
                Some(Ok(metrics)) => {
                    let record = CompletionRecord {
                        action_id: metrics.action_id,
                        success: metrics.success,
                        delivery_id: metrics.delivery_id,
                        result_payload: metrics.response_payload.clone(),
                    };
                    if let Err(err) = self.completion_tx.send(record).await {
                        warn!(?err, "completion channel closed");
                    }
                    completed.push(BenchmarkResult::from(metrics));
                    if let Some(interval) = config.progress_interval {
                        let now = Instant::now();
                        if now.duration_since(last_report) >= interval {
                            let elapsed = now.duration_since(start);
                            let throughput =
                                completed.len() as f64 / elapsed.as_secs_f64().max(1e-9);
                            info!(
                                processed = completed.len(),
                                total = total_actions,
                                elapsed = %format!("{:.1}s", elapsed.as_secs_f64()),
                                throughput = %format!("{:.0} msg/s", throughput),
                                in_flight = inflight.len(),
                                worker_count,
                                db_queue = dispatched - completed.len(),
                                dispatched,
                                "benchmark progress",
                            );
                            last_report = now;
                        }
                    }
                }
                Some(Err(err)) => return Err(err.into()),
                None => {
                    if dispatched >= total_actions {
                        break;
                    }
                }
            }
        }

        let elapsed = start.elapsed();
        Ok(BenchmarkSummary::from_results(completed, elapsed))
    }

    pub async fn shutdown(self) -> Result<()> {
        let WorkflowBenchmarkHarness {
            worker_server,
            workers,
            completion_tx,
            completion_handle,
            ..
        } = self;
        drop(completion_tx);
        workers.shutdown().await?;
        worker_server.shutdown().await;
        if let Err(err) = completion_handle.await {
            warn!(?err, "completion worker failed");
        }
        Ok(())
    }

    pub fn actions_per_instance(&self) -> usize {
        self.dag_node_count
    }
}

fn build_registration_payload(python_paths: &[PathBuf], module: &str) -> Result<Vec<u8>> {
    let script = format!(
        r#"
import base64
import importlib

module = importlib.import_module('{module}')
workflow_cls = getattr(module, 'BenchmarkInstancesWorkflow')
payload = workflow_cls._build_registration_payload()
print(base64.b64encode(payload.SerializeToString()).decode(), end='')
"#
    );
    let python_exec = std::env::var("PYTHON_BIN").unwrap_or_else(|_| "python3".to_string());
    let mut command = Command::new(python_exec);
    command.arg("-c").arg(script);
    command.env(
        "PYTHONPATH",
        python_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(":"),
    );
    let output = command
        .output()
        .context("run python registration builder")?;
    if !output.status.success() {
        return Err(anyhow!(
            "python registration command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let encoded = String::from_utf8(output.stdout)?.trim().to_string();
    let bytes = general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .context("decode workflow registration base64")?;
    Ok(bytes)
}

fn build_workflow_input(batch_size: usize, payload_size: usize) -> Vec<u8> {
    let mut arguments = proto::WorkflowArguments {
        arguments: Vec::new(),
    };
    arguments.arguments.push(proto::WorkflowArgument {
        key: "batch_size".to_string(),
        value: Some(primitive_argument(
            proto::primitive_workflow_argument::Kind::IntValue(batch_size as i64),
        )),
    });
    arguments.arguments.push(proto::WorkflowArgument {
        key: "payload_size".to_string(),
        value: Some(primitive_argument(
            proto::primitive_workflow_argument::Kind::IntValue(payload_size as i64),
        )),
    });
    arguments.encode_to_vec()
}

fn primitive_argument(
    kind: proto::primitive_workflow_argument::Kind,
) -> proto::WorkflowArgumentValue {
    proto::WorkflowArgumentValue {
        kind: Some(proto::workflow_argument_value::Kind::Primitive(
            proto::PrimitiveWorkflowArgument { kind: Some(kind) },
        )),
    }
}
