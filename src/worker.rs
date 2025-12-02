use std::{
    collections::HashMap,
    env,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result as AnyResult, anyhow};
use prost::Message;
use tokio::{
    process::{Child, Command},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tracing::{debug, error, info, warn};

use crate::{
    LedgerActionId, WorkflowInstanceId,
    messages::{self, MessageError, proto},
    server_worker::{WorkerBridgeChannels, WorkerBridgeServer},
};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct PythonWorkerConfig {
    pub script_path: PathBuf,
    pub script_args: Vec<String>,
    pub user_module: String,
    pub extra_python_paths: Vec<PathBuf>,
}

impl Default for PythonWorkerConfig {
    fn default() -> Self {
        let (script_path, script_args) = default_runner();
        Self {
            script_path,
            script_args,
            user_module: "benchmark.fixtures.benchmark_actions".to_string(),
            extra_python_paths: vec![PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")],
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoundTripMetrics {
    pub action_id: LedgerActionId,
    pub instance_id: WorkflowInstanceId,
    pub delivery_id: u64,
    pub sequence: u32,
    pub ack_latency: Duration,
    pub round_trip: Duration,
    pub worker_duration: Duration,
    pub response_payload: Vec<u8>,
    pub success: bool,
    pub dispatch_token: Option<Uuid>,
    pub control: Option<proto::WorkflowNodeControl>,
}

struct SharedState {
    pending_acks: HashMap<u64, oneshot::Sender<Instant>>,
    pending_responses: HashMap<u64, oneshot::Sender<(proto::ActionResult, Instant)>>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            pending_acks: HashMap::new(),
            pending_responses: HashMap::new(),
        }
    }
}

fn default_runner() -> (PathBuf, Vec<String>) {
    if let Some(path) = find_executable("rappel-worker") {
        return (path, Vec::new());
    }
    (
        PathBuf::from("uv"),
        vec![
            "run".to_string(),
            "python".to_string(),
            "-m".to_string(),
            "rappel.worker".to_string(),
        ],
    )
}

fn find_executable(bin: &str) -> Option<PathBuf> {
    let path_var = env::var_os("PATH")?;
    for dir in env::split_paths(&path_var) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let exe_candidate = dir.join(format!("{bin}.exe"));
            if exe_candidate.is_file() {
                return Some(exe_candidate);
            }
        }
    }
    None
}

#[derive(Debug, Clone)]
pub struct ActionDispatchPayload {
    pub action_id: LedgerActionId,
    pub instance_id: WorkflowInstanceId,
    pub sequence: i32,
    pub dispatch: proto::NodeDispatch,
    pub timeout_seconds: i32,
    pub max_retries: i32,
    pub attempt_number: i32,
    pub dispatch_token: Uuid,
}

pub struct PythonWorker {
    child: Child,
    sender: mpsc::Sender<proto::Envelope>,
    shared: Arc<Mutex<SharedState>>,
    next_delivery: AtomicU64,
    reader_handle: Option<JoinHandle<()>>,
}

impl PythonWorker {
    async fn spawn(config: PythonWorkerConfig, bridge: Arc<WorkerBridgeServer>) -> AnyResult<Self> {
        let (worker_id, connection_rx) = bridge.reserve_worker().await;
        let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");
        let working_dir = if package_root.is_dir() {
            Some(package_root.clone())
        } else {
            None
        };
        let mut module_paths = Vec::new();
        if let Some(root) = working_dir.as_ref() {
            module_paths.push(root.clone());
            let src_dir = root.join("src");
            if src_dir.exists() {
                module_paths.push(src_dir);
            }
            let proto_dir = root.join("proto");
            if proto_dir.exists() {
                module_paths.push(proto_dir);
            }
        }
        module_paths.extend(config.extra_python_paths.clone());
        let joined_python_path = module_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(":");
        let python_path = match env::var("PYTHONPATH") {
            Ok(existing) if !existing.is_empty() => format!("{existing}:{joined_python_path}"),
            _ => joined_python_path,
        };
        info!(python_path = %python_path, "configured python path for worker");

        let mut command = Command::new(&config.script_path);
        command.args(&config.script_args);
        command
            .arg("--bridge")
            .arg(bridge.addr().to_string())
            .arg("--worker-id")
            .arg(worker_id.to_string())
            .arg("--user-module")
            .arg(&config.user_module)
            .stderr(Stdio::inherit())
            .env("PYTHONPATH", python_path);

        if let Some(dir) = working_dir {
            info!(?dir, "using package root for worker process");
            command.current_dir(dir);
        } else {
            let cwd = env::current_dir().context("failed to resolve current directory")?;
            info!(
                ?cwd,
                "package root missing, using current directory for worker process"
            );
            command.current_dir(cwd);
        }

        let mut child = match command.spawn().context("failed to launch python worker") {
            Ok(child) => child,
            Err(err) => {
                bridge.cancel_worker(worker_id).await;
                return Err(err);
            }
        };

        info!(
            pid = child.id(),
            script = %config.script_path.display(),
            worker_id,
            "spawned python worker"
        );

        let connection = match timeout(Duration::from_secs(15), connection_rx).await {
            Ok(Ok(channels)) => channels,
            Ok(Err(_)) => {
                bridge.cancel_worker(worker_id).await;
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(anyhow!("worker bridge channel closed before attach"));
            }
            Err(_) => {
                bridge.cancel_worker(worker_id).await;
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(anyhow!("timed out waiting for worker bridge attachment"));
            }
        };

        let WorkerBridgeChannels {
            to_worker,
            mut from_worker,
        } = connection;
        let shared = Arc::new(Mutex::new(SharedState::new()));
        let reader_shared = Arc::clone(&shared);
        let reader_handle = tokio::spawn(async move {
            if let Err(err) = Self::reader_loop(&mut from_worker, reader_shared).await {
                error!(?err, "python worker stream exited");
            }
        });

        Ok(Self {
            child,
            sender: to_worker,
            shared,
            next_delivery: AtomicU64::new(1),
            reader_handle: Some(reader_handle),
        })
    }

    pub async fn send_action(
        &self,
        dispatch: ActionDispatchPayload,
    ) -> Result<RoundTripMetrics, MessageError> {
        let delivery_id = self.next_delivery.fetch_add(1, Ordering::SeqCst);
        let send_instant = Instant::now();
        let action_node = dispatch
            .dispatch
            .node
            .as_ref()
            .map(|node| (node.module.clone(), node.action.clone()))
            .unwrap_or_default();
        tracing::debug!(
            action_id = %dispatch.action_id,
            instance_id = %dispatch.instance_id,
            sequence = dispatch.sequence,
            module = %action_node.0,
            function = %action_node.1,
            "worker.send_action"
        );
        let (ack_tx, ack_rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();

        {
            let mut shared = self.shared.lock().await;
            shared.pending_acks.insert(delivery_id, ack_tx);
            shared.pending_responses.insert(delivery_id, response_tx);
        }

        let command = proto::ActionDispatch {
            action_id: dispatch.action_id.to_string(),
            instance_id: dispatch.instance_id.to_string(),
            sequence: dispatch.sequence as u32,
            dispatch: Some(dispatch.dispatch.clone()),
            timeout_seconds: Some(dispatch.timeout_seconds.max(0) as u32),
            max_retries: Some(dispatch.max_retries.max(0) as u32),
            attempt_number: Some(dispatch.attempt_number.max(0) as u32),
            dispatch_token: Some(dispatch.dispatch_token.to_string()),
        };

        let envelope = proto::Envelope {
            delivery_id,
            partition_id: 0,
            kind: proto::MessageKind::ActionDispatch as i32,
            payload: messages::encode_message(&command),
        };

        self.send_envelope(envelope).await?;

        let ack_instant = ack_rx.await.map_err(|_| MessageError::ChannelClosed)?;
        let (response, response_instant) =
            response_rx.await.map_err(|_| MessageError::ChannelClosed)?;

        let ack_latency = ack_instant
            .checked_duration_since(send_instant)
            .unwrap_or_default();
        let round_trip = response_instant
            .checked_duration_since(send_instant)
            .unwrap_or_default();
        let worker_duration = Duration::from_nanos(
            response
                .worker_end_ns
                .saturating_sub(response.worker_start_ns),
        );

        Ok(RoundTripMetrics {
            action_id: dispatch.action_id,
            instance_id: dispatch.instance_id,
            delivery_id,
            sequence: dispatch.sequence as u32,
            ack_latency,
            round_trip,
            worker_duration,
            response_payload: response
                .payload
                .as_ref()
                .map(|payload| payload.encode_to_vec())
                .unwrap_or_default(),
            success: response.success,
            dispatch_token: response
                .dispatch_token
                .as_ref()
                .and_then(|token| Uuid::parse_str(token).ok()),
            control: response.control.clone(),
        })
    }

    async fn send_envelope(&self, envelope: proto::Envelope) -> Result<(), MessageError> {
        self.sender
            .send(envelope)
            .await
            .map_err(|_| MessageError::ChannelClosed)
    }

    async fn reader_loop(
        incoming: &mut mpsc::Receiver<proto::Envelope>,
        shared: Arc<Mutex<SharedState>>,
    ) -> Result<(), MessageError> {
        while let Some(envelope) = incoming.recv().await {
            let kind = proto::MessageKind::try_from(envelope.kind)
                .unwrap_or(proto::MessageKind::Unspecified);

            match kind {
                proto::MessageKind::Ack => {
                    let ack = messages::decode_message::<proto::Ack>(&envelope.payload)?;
                    let mut guard = shared.lock().await;
                    if let Some(sender) = guard.pending_acks.remove(&ack.acked_delivery_id) {
                        let _ = sender.send(Instant::now());
                    } else {
                        warn!(delivery = ack.acked_delivery_id, "unexpected ACK");
                    }
                }
                proto::MessageKind::ActionResult => {
                    let response =
                        messages::decode_message::<proto::ActionResult>(&envelope.payload)?;
                    {
                        let mut guard = shared.lock().await;
                        if let Some(sender) = guard.pending_responses.remove(&envelope.delivery_id)
                        {
                            let _ = sender.send((response, Instant::now()));
                        } else {
                            warn!(delivery = envelope.delivery_id, "orphan response");
                        }
                    }
                }
                proto::MessageKind::Heartbeat => {
                    debug!(delivery = envelope.delivery_id, "heartbeat");
                }
                other => {
                    warn!(?other, "unhandled message");
                }
            }
        }

        Ok(())
    }

    pub async fn shutdown(mut self) -> AnyResult<()> {
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        self.child.start_kill()?;
        let _ = self.child.wait().await?;
        Ok(())
    }
}

impl Drop for PythonWorker {
    fn drop(&mut self) {
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
        }
        if let Err(err) = self.child.start_kill() {
            warn!(?err, "failed to kill python worker during drop");
        }
    }
}

pub struct PythonWorkerPool {
    workers: Vec<Arc<PythonWorker>>,
    cursor: AtomicUsize,
}

impl PythonWorkerPool {
    pub async fn new(
        config: PythonWorkerConfig,
        count: usize,
        bridge: Arc<WorkerBridgeServer>,
    ) -> AnyResult<Self> {
        let worker_count = count.max(1);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker = PythonWorker::spawn(config.clone(), Arc::clone(&bridge)).await?;
            workers.push(Arc::new(worker));
        }
        Ok(Self {
            workers,
            cursor: AtomicUsize::new(0),
        })
    }

    pub fn next_worker(&self) -> Arc<PythonWorker> {
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&self.workers[idx % self.workers.len()])
    }

    pub fn len(&self) -> usize {
        self.workers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    pub async fn shutdown(self) -> AnyResult<()> {
        for worker in self.workers {
            match Arc::try_unwrap(worker) {
                Ok(worker) => {
                    worker.shutdown().await?;
                }
                Err(_) => warn!("python worker still in use during shutdown; skipping"),
            }
        }
        Ok(())
    }
}
