use std::{
    collections::HashMap,
    convert::TryFrom,
    env,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result as AnyResult};
use tokio::{
    io::{BufReader, BufWriter},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, oneshot},
    task::JoinHandle,
};
use tracing::{debug, error, info, warn};

use crate::messages::{self, MessageError, proto};

#[derive(Clone, Debug)]
pub struct PythonWorkerConfig {
    pub script_path: PathBuf,
    pub python_binary: PathBuf,
    pub partition_id: u32,
}

impl Default for PythonWorkerConfig {
    fn default() -> Self {
        Self {
            script_path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/worker.py"),
            python_binary: PathBuf::from("python3"),
            partition_id: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoundTripMetrics {
    pub delivery_id: u64,
    pub sequence: u32,
    pub ack_latency: Duration,
    pub round_trip: Duration,
    pub worker_duration: Duration,
}

struct SharedState {
    pending_acks: HashMap<u64, oneshot::Sender<Instant>>,
    pending_responses: HashMap<u64, oneshot::Sender<(proto::BenchmarkResponse, Instant)>>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            pending_acks: HashMap::new(),
            pending_responses: HashMap::new(),
        }
    }
}

pub struct PythonWorker {
    child: Child,
    writer: Arc<Mutex<BufWriter<ChildStdin>>>,
    shared: Arc<Mutex<SharedState>>,
    next_delivery: AtomicU64,
    partition_id: u32,
    reader_handle: Option<JoinHandle<()>>,
}

impl PythonWorker {
    pub async fn spawn(config: PythonWorkerConfig) -> AnyResult<Self> {
        let script_dir = config
            .script_path
            .parent()
            .map(|path| path.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let python_path = match env::var("PYTHONPATH") {
            Ok(existing) if !existing.is_empty() => {
                format!("{existing}:{}", script_dir.display())
            }
            _ => script_dir.display().to_string(),
        };

        let mut command = Command::new(&config.python_binary);
        command
            .arg(&config.script_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .env("PYTHONPATH", python_path);

        let mut child = command.spawn().context("failed to launch python worker")?;
        let stdin = child.stdin.take().context("python worker stdin missing")?;
        let stdout = child
            .stdout
            .take()
            .context("python worker stdout missing")?;

        info!(
            pid = child.id(),
            script = %config.script_path.display(),
            "spawned python worker"
        );

        let writer = Arc::new(Mutex::new(BufWriter::new(stdin)));
        let shared = Arc::new(Mutex::new(SharedState::new()));
        let reader_shared = Arc::clone(&shared);
        let reader_handle = tokio::spawn(async move {
            if let Err(err) = Self::reader_loop(stdout, reader_shared).await {
                error!(?err, "python stdout loop exited");
            }
        });

        Ok(Self {
            child,
            writer,
            shared,
            next_delivery: AtomicU64::new(1),
            partition_id: config.partition_id,
            reader_handle: Some(reader_handle),
        })
    }

    pub async fn send_benchmark_command(
        &self,
        sequence: u32,
        payload_size: usize,
    ) -> Result<RoundTripMetrics, MessageError> {
        let delivery_id = self.next_delivery.fetch_add(1, Ordering::SeqCst);
        let send_instant = Instant::now();
        let (ack_tx, ack_rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();

        {
            let mut shared = self.shared.lock().await;
            shared.pending_acks.insert(delivery_id, ack_tx);
            shared.pending_responses.insert(delivery_id, response_tx);
        }

        let command = proto::BenchmarkCommand {
            monotonic_ns: messages::now_monotonic_ns(),
            sequence,
            payload_size: payload_size as u32,
            payload: vec![0; payload_size],
        };

        let envelope = proto::Envelope {
            delivery_id,
            partition_id: self.partition_id,
            kind: proto::MessageKind::BenchmarkCommand as i32,
            payload: messages::encode_message(&command),
        };

        self.write_envelope(&envelope).await?;

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
            delivery_id,
            sequence,
            ack_latency,
            round_trip,
            worker_duration,
        })
    }

    async fn write_envelope(&self, envelope: &proto::Envelope) -> Result<(), MessageError> {
        let mut writer = self.writer.lock().await;
        messages::write_envelope(&mut *writer, envelope).await
    }

    async fn reader_loop(
        stdout: ChildStdout,
        shared: Arc<Mutex<SharedState>>,
    ) -> Result<(), MessageError> {
        let mut reader = BufReader::new(stdout);
        loop {
            let envelope = match messages::read_envelope(&mut reader).await {
                Ok(frame) => frame,
                Err(MessageError::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("python stdout closed");
                    break;
                }
                Err(other) => return Err(other),
            };

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
                proto::MessageKind::BenchmarkResponse => {
                    let response =
                        messages::decode_message::<proto::BenchmarkResponse>(&envelope.payload)?;
                    {
                        let mut guard = shared.lock().await;
                        if let Some(sender) = guard
                            .pending_responses
                            .remove(&response.correlated_delivery_id)
                        {
                            let _ = sender.send((response, Instant::now()));
                        } else {
                            warn!(
                                delivery = response.correlated_delivery_id,
                                "orphan response"
                            );
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
