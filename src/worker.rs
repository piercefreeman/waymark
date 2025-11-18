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

#[derive(Clone, Debug)]
pub struct PythonWorkerConfig {
    pub script_path: PathBuf,
    pub partition_id: u32,
    pub user_module: String,
    pub extra_python_paths: Vec<PathBuf>,
}

impl Default for PythonWorkerConfig {
    fn default() -> Self {
        Self {
            script_path: PathBuf::from("carabiner-worker"),
            partition_id: 0,
            user_module: "fixtures.benchmark_actions".to_string(),
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

#[derive(Debug, Clone)]
pub struct ActionDispatchPayload {
    pub action_id: LedgerActionId,
    pub instance_id: WorkflowInstanceId,
    pub sequence: i32,
    pub payload: Vec<u8>,
}

pub struct PythonWorker {
    child: Child,
    sender: mpsc::Sender<proto::Envelope>,
    shared: Arc<Mutex<SharedState>>,
    next_delivery: AtomicU64,
    partition_id: u32,
    reader_handle: Option<JoinHandle<()>>,
}

impl PythonWorker {
    async fn spawn(config: PythonWorkerConfig, bridge: Arc<WorkerBridgeServer>) -> AnyResult<Self> {
        let (worker_id, connection_rx) = bridge.reserve_worker().await;
        let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");
        let mut module_paths = Vec::new();
        if package_root.exists() {
            module_paths.push(package_root.clone());
            let src_dir = package_root.join("src");
            if src_dir.exists() {
                module_paths.push(src_dir);
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

        let mut command = Command::new(&config.script_path);
        command
            .arg("--bridge")
            .arg(bridge.addr().to_string())
            .arg("--worker-id")
            .arg(worker_id.to_string())
            .arg("--user-module")
            .arg(&config.user_module)
            .stderr(Stdio::inherit())
            .env("PYTHONPATH", python_path);

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
            partition_id: config.partition_id,
            reader_handle: Some(reader_handle),
        })
    }

    pub async fn send_action(
        &self,
        dispatch: ActionDispatchPayload,
    ) -> Result<RoundTripMetrics, MessageError> {
        let delivery_id = self.next_delivery.fetch_add(1, Ordering::SeqCst);
        let send_instant = Instant::now();
        tracing::debug!(
            action_id = %dispatch.action_id,
            instance_id = %dispatch.instance_id,
            sequence = dispatch.sequence,
            payload_len = dispatch.payload.len(),
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
            payload: dispatch.payload.clone(),
        };

        let envelope = proto::Envelope {
            delivery_id,
            partition_id: self.partition_id,
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
            response_payload: response.payload,
            success: response.success,
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
