use std::{
    env,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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
use tracing::{error, info, trace, warn};
use uuid::Uuid;

use waymark_proto::messages as proto;
use waymark_worker_metrics::RoundTripMetrics;
use waymark_worker_remote_connecting::Reservation;

use crate::{Config, shared_state::SharedState};

/// Errors that can occur during message encoding/decoding
#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to decode message: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("Failed to encode message: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("Channel closed")]
    ChannelClosed,
}

/// A single Python worker process.
///
/// Manages the lifecycle of a Python subprocess that executes actions.
/// Communication happens via gRPC streaming through the WorkerBridge.
///
/// Workers are not meant to be used directly - use [`ProcessPool`] instead.
pub struct Process {
    /// The child process
    child: Child,

    /// Channel to send envelopes to the worker
    sender: mpsc::Sender<proto::Envelope>,

    /// Shared state for pending requests
    shared: Arc<Mutex<SharedState>>,

    /// Counter for delivery IDs (monotonically increasing)
    next_delivery: AtomicU64,

    /// Handle to the reader task
    reader_handle: Option<JoinHandle<()>>,

    /// Worker ID for logging
    worker_id: u64,
}

pub struct SpawnParams {
    pub worker_id: waymark_worker_remote_core::WorkerId,
}

impl Process {
    /// Spawn a new Python worker process.
    ///
    /// This will:
    /// 1. Reserve a worker ID on the bridge
    /// 2. Spawn the Python process with appropriate arguments
    /// 3. Wait for the worker to connect (15s timeout)
    /// 4. Set up bidirectional communication channels
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The process fails to spawn
    /// - The worker doesn't connect within 15 seconds
    /// - The bridge connection is dropped
    pub async fn spawn(config: Config, reservation: Reservation) -> AnyResult<Self> {
        let Reservation {
            worker_id,
            channels_rx,
        } = reservation;

        // Determine working directory and module paths
        let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");
        let working_dir = if package_root.is_dir() {
            Some(package_root.clone())
        } else {
            None
        };

        // Build PYTHONPATH with all necessary directories
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

        info!(python_path = %python_path, worker_id, "configured python path for worker");

        // Build the command
        let mut command = Command::new(&config.script_path);
        command.args(&config.script_args);
        command
            .arg("--bridge")
            .arg(bridge.addr().to_string())
            .arg("--worker-id")
            .arg(worker_id.to_string());

        // Add user modules
        for module in &config.user_modules {
            command.arg("--user-module").arg(module);
        }

        command
            .stderr(Stdio::inherit())
            .env("PYTHONPATH", python_path);

        if let Some(dir) = working_dir {
            info!(?dir, worker_id, "using package root for worker process");
            command.current_dir(dir);
        } else {
            let cwd = env::current_dir().context("failed to resolve current directory")?;
            info!(
                ?cwd,
                worker_id, "package root missing, using current directory for worker process"
            );
            command.current_dir(cwd);
        }

        // Spawn the process
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

        // Wait for the worker to connect (with timeout)
        let channels = match timeout(Duration::from_secs(15), channels_rx).await {
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
                return Err(anyhow!(
                    "timed out waiting for worker {} to connect (15s)",
                    worker_id
                ));
            }
        };

        let WorkerBridgeChannels {
            to_worker,
            mut from_worker,
        } = connection;

        // Set up shared state and spawn reader task
        let shared = Arc::new(Mutex::new(SharedState::default()));
        let reader_shared = Arc::clone(&shared);
        let reader_worker_id = worker_id;
        let reader_handle = tokio::spawn(async move {
            if let Err(err) = Self::reader_loop(&mut from_worker, reader_shared).await {
                error!(
                    ?err,
                    worker_id = reader_worker_id,
                    "python worker stream exited"
                );
            }
        });

        info!(worker_id, "worker connected and ready");

        Ok(Self {
            child,
            sender: to_worker,
            shared,
            next_delivery: AtomicU64::new(1),
            reader_handle: Some(reader_handle),
            worker_id,
        })
    }

    /// Send an action to the worker and wait for the result.
    ///
    /// This method:
    /// 1. Allocates a delivery ID
    /// 2. Creates channels for ACK and response
    /// 3. Sends the action dispatch
    /// 4. Waits for ACK (immediate)
    /// 5. Waits for result (after execution)
    /// 6. Returns metrics including latencies
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The worker channel is closed (worker crashed)
    /// - Response decoding fails
    pub async fn send_action(
        &self,
        dispatch: ActionDispatchPayload,
    ) -> Result<RoundTripMetrics, MessageError> {
        let delivery_id = self.next_delivery.fetch_add(1, Ordering::SeqCst);
        let send_instant = Instant::now();

        trace!(
            action_id = %dispatch.action_id,
            instance_id = %dispatch.instance_id,
            sequence = dispatch.sequence,
            module = %dispatch.module_name,
            function = %dispatch.action_name,
            worker_id = self.worker_id,
            delivery_id,
            "sending action to worker"
        );

        // Create channels for receiving ACK and response
        let (ack_tx, ack_rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();

        // Register pending requests
        {
            let mut shared = self.shared.lock().await;
            shared.pending_acks.insert(delivery_id, ack_tx);
            shared.pending_responses.insert(delivery_id, response_tx);
        }

        // Build and send the dispatch envelope
        let command = proto::ActionDispatch {
            action_id: dispatch.action_id.clone(),
            instance_id: dispatch.instance_id.clone(),
            sequence: dispatch.sequence,
            action_name: dispatch.action_name.clone(),
            module_name: dispatch.module_name.clone(),
            kwargs: Some(dispatch.kwargs.clone()),
            timeout_seconds: Some(dispatch.timeout_seconds),
            max_retries: Some(dispatch.max_retries),
            attempt_number: Some(dispatch.attempt_number),
            dispatch_token: Some(dispatch.dispatch_token.to_string()),
        };

        let envelope = proto::Envelope {
            delivery_id,
            partition_id: 0,
            kind: proto::MessageKind::ActionDispatch as i32,
            payload: command.encode_to_vec(),
        };

        self.send_envelope(envelope).await?;

        // Wait for ACK (should be immediate)
        let ack_instant = ack_rx.await.map_err(|_| MessageError::ChannelClosed)?;

        // Wait for the actual response (after execution)
        let (response, response_instant) =
            response_rx.await.map_err(|_| MessageError::ChannelClosed)?;

        // Calculate metrics
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

        trace!(
            action_id = %dispatch.action_id,
            worker_id = self.worker_id,
            ack_latency_us = ack_latency.as_micros(),
            round_trip_ms = round_trip.as_millis(),
            worker_duration_ms = worker_duration.as_millis(),
            success = response.success,
            "action completed"
        );

        Ok(RoundTripMetrics {
            action_id: dispatch.action_id,
            instance_id: dispatch.instance_id,
            delivery_id,
            sequence: dispatch.sequence,
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
            error_type: response.error_type,
            error_message: response.error_message,
        })
    }

    /// Send an envelope to the worker.
    async fn send_envelope(&self, envelope: proto::Envelope) -> Result<(), MessageError> {
        self.sender
            .send(envelope)
            .await
            .map_err(|_| MessageError::ChannelClosed)
    }

    /// Background task that reads messages from the worker.
    async fn reader_loop(
        incoming: &mut mpsc::Receiver<proto::Envelope>,
        shared: Arc<Mutex<SharedState>>,
    ) -> Result<(), MessageError> {
        while let Some(envelope) = incoming.recv().await {
            let kind = proto::MessageKind::try_from(envelope.kind)
                .unwrap_or(proto::MessageKind::Unspecified);

            match kind {
                proto::MessageKind::Ack => {
                    let ack = proto::Ack::decode(envelope.payload.as_slice())?;
                    let mut guard = shared.lock().await;
                    if let Some(sender) = guard.pending_acks.remove(&ack.acked_delivery_id) {
                        let _ = sender.send(Instant::now());
                    } else {
                        warn!(delivery = ack.acked_delivery_id, "unexpected ACK");
                    }
                }
                proto::MessageKind::ActionResult => {
                    let response = proto::ActionResult::decode(envelope.payload.as_slice())?;
                    let mut guard = shared.lock().await;
                    if let Some(sender) = guard.pending_responses.remove(&envelope.delivery_id) {
                        let _ = sender.send((response, Instant::now()));
                    } else {
                        warn!(delivery = envelope.delivery_id, "orphan response");
                    }
                }
                proto::MessageKind::Heartbeat => {
                    trace!(delivery = envelope.delivery_id, "heartbeat");
                }
                other => {
                    warn!(?other, "unhandled message kind");
                }
            }
        }

        Ok(())
    }

    /// Gracefully shut down the worker.
    pub async fn shutdown(mut self) -> AnyResult<()> {
        info!(worker_id = self.worker_id, "shutting down worker");

        // Abort the reader task
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        // Kill the child process
        self.child.start_kill()?;
        let _ = self.child.wait().await?;

        info!(worker_id = self.worker_id, "worker shutdown complete");
        Ok(())
    }

    /// Get the worker ID.
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
        }
        if let Err(err) = self.child.start_kill() {
            warn!(
                ?err,
                worker_id = self.worker_id,
                "failed to kill python worker during drop"
            );
        }
    }
}
