//! Python worker process management.
//!
//! This module provides the core infrastructure for spawning and managing
//! Python worker processes that execute workflow actions.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                           PythonWorkerPool                               │
//! │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐                        │
//! │  │PythonWorker │ │PythonWorker │ │PythonWorker │  ... (N workers)       │
//! │  │  (process)  │ │  (process)  │ │  (process)  │                        │
//! │  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘                        │
//! │         │               │               │                                │
//! │         └───────────────┼───────────────┘                                │
//! │                         │ gRPC streaming                                 │
//! │                         ▼                                                │
//! │              ┌─────────────────────┐                                     │
//! │              │  WorkerBridgeServer │                                     │
//! │              └─────────────────────┘                                     │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Worker Lifecycle
//!
//! 1. Pool spawns N worker processes, each connecting to the WorkerBridge
//! 2. Workers send `WorkerHello` to complete the handshake
//! 3. Pool sends `ActionDispatch` messages, workers respond with `ActionResult`
//! 4. Workers send `Ack` immediately upon receiving a dispatch (for latency tracking)
//! 5. On shutdown, workers are terminated gracefully
//!
//! ## Error Handling
//!
//! - Worker spawn failures are propagated immediately
//! - Connection timeouts (15s) trigger worker process termination
//! - Dropped channels indicate worker crash and are propagated as errors
//! - Round-robin selection ensures load distribution even with slow workers

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
use uuid::Uuid;

use crate::{
    messages::{self, MessageError, proto},
    server_worker::{WorkerBridgeChannels, WorkerBridgeServer},
};

/// Configuration for spawning Python workers.
#[derive(Clone, Debug)]
pub struct PythonWorkerConfig {
    /// Path to the script/executable to run (e.g., "uv" or "rappel-worker")
    pub script_path: PathBuf,
    /// Arguments to pass before the worker-specific args
    pub script_args: Vec<String>,
    /// Python module(s) to preload (contains @action definitions)
    pub user_modules: Vec<String>,
    /// Additional paths to add to PYTHONPATH
    pub extra_python_paths: Vec<PathBuf>,
}

impl Default for PythonWorkerConfig {
    fn default() -> Self {
        let (script_path, script_args) = default_runner();
        Self {
            script_path,
            script_args,
            user_modules: vec![],
            extra_python_paths: vec![],
        }
    }
}

impl PythonWorkerConfig {
    /// Create a new config with default runner detection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the user module to preload.
    pub fn with_user_module(mut self, module: &str) -> Self {
        self.user_modules = vec![module.to_string()];
        self
    }

    /// Set multiple user modules to preload.
    pub fn with_user_modules(mut self, modules: Vec<String>) -> Self {
        self.user_modules = modules;
        self
    }

    /// Add extra paths to PYTHONPATH.
    pub fn with_python_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.extra_python_paths = paths;
        self
    }
}

/// Find the default Python runner.
/// Prefers `rappel-worker` if in PATH, otherwise uses `uv run`.
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

/// Search PATH for an executable.
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

/// Metrics from a single action round-trip.
#[derive(Debug, Clone)]
pub struct RoundTripMetrics {
    /// The action ID that was executed
    pub action_id: String,
    /// The workflow instance this action belongs to
    pub instance_id: String,
    /// Delivery ID used for correlation
    pub delivery_id: u64,
    /// Sequence number within the instance
    pub sequence: u32,
    /// Time from send to ACK receipt
    pub ack_latency: Duration,
    /// Time from send to result receipt
    pub round_trip: Duration,
    /// Time the worker spent executing (from worker's perspective)
    pub worker_duration: Duration,
    /// Serialized result payload
    pub response_payload: Vec<u8>,
    /// Whether the action succeeded
    pub success: bool,
    /// Dispatch token for correlation (echoed back)
    pub dispatch_token: Option<Uuid>,
    /// Error type if the action failed
    pub error_type: Option<String>,
    /// Error message if the action failed
    pub error_message: Option<String>,
}

/// Payload for dispatching an action to a worker.
#[derive(Debug, Clone)]
pub struct ActionDispatchPayload {
    /// Unique action identifier
    pub action_id: String,
    /// Workflow instance this action belongs to
    pub instance_id: String,
    /// Sequence number within the instance
    pub sequence: u32,
    /// Name of the action function to call
    pub action_name: String,
    /// Python module containing the action
    pub module_name: String,
    /// Keyword arguments for the action
    pub kwargs: proto::WorkflowArguments,
    /// Timeout in seconds (0 = no timeout)
    pub timeout_seconds: u32,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Current attempt number
    pub attempt_number: u32,
    /// Dispatch token for correlation
    pub dispatch_token: Uuid,
}

/// Internal state shared between worker sender and reader tasks.
struct SharedState {
    /// Pending ACK receivers, keyed by delivery_id
    pending_acks: HashMap<u64, oneshot::Sender<Instant>>,
    /// Pending result receivers, keyed by delivery_id
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

/// A single Python worker process.
///
/// Manages the lifecycle of a Python subprocess that executes actions.
/// Communication happens via gRPC streaming through the WorkerBridge.
///
/// Workers are not meant to be used directly - use [`PythonWorkerPool`] instead.
pub struct PythonWorker {
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

impl PythonWorker {
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
    pub async fn spawn(config: PythonWorkerConfig, bridge: Arc<WorkerBridgeServer>) -> AnyResult<Self> {
        let (worker_id, connection_rx) = bridge.reserve_worker().await;

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
                worker_id,
                "package root missing, using current directory for worker process"
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
        let shared = Arc::new(Mutex::new(SharedState::new()));
        let reader_shared = Arc::clone(&shared);
        let reader_worker_id = worker_id;
        let reader_handle = tokio::spawn(async move {
            if let Err(err) = Self::reader_loop(&mut from_worker, reader_shared).await {
                error!(?err, worker_id = reader_worker_id, "python worker stream exited");
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

        debug!(
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
            payload: messages::encode_message(&command),
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

        debug!(
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
                    let mut guard = shared.lock().await;
                    if let Some(sender) = guard.pending_responses.remove(&envelope.delivery_id) {
                        let _ = sender.send((response, Instant::now()));
                    } else {
                        warn!(delivery = envelope.delivery_id, "orphan response");
                    }
                }
                proto::MessageKind::Heartbeat => {
                    debug!(delivery = envelope.delivery_id, "heartbeat");
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

impl Drop for PythonWorker {
    fn drop(&mut self) {
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
        }
        if let Err(err) = self.child.start_kill() {
            warn!(?err, worker_id = self.worker_id, "failed to kill python worker during drop");
        }
    }
}

/// Pool of Python workers for action execution.
///
/// Provides round-robin load balancing across multiple worker processes.
/// Workers are spawned eagerly on pool creation.
///
/// # Example
///
/// ```ignore
/// let bridge = WorkerBridgeServer::start(None).await?;
/// let config = PythonWorkerConfig::new()
///     .with_user_module("my_app.actions");
/// let pool = PythonWorkerPool::new(config, 4, bridge).await?;
///
/// let metrics = pool.next_worker().send_action(dispatch).await?;
/// ```
pub struct PythonWorkerPool {
    /// The workers in the pool
    workers: Vec<Arc<PythonWorker>>,
    /// Cursor for round-robin selection
    cursor: AtomicUsize,
}

impl PythonWorkerPool {
    /// Create a new worker pool with the given configuration.
    ///
    /// Spawns `count` worker processes. All workers must successfully
    /// spawn and connect before this returns.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for worker processes
    /// * `count` - Number of workers to spawn (minimum 1)
    /// * `bridge` - The WorkerBridge server workers will connect to
    ///
    /// # Errors
    ///
    /// Returns an error if any worker fails to spawn or connect.
    pub async fn new(
        config: PythonWorkerConfig,
        count: usize,
        bridge: Arc<WorkerBridgeServer>,
    ) -> AnyResult<Self> {
        let worker_count = count.max(1);
        info!(count = worker_count, "spawning python worker pool");

        let mut workers = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            match PythonWorker::spawn(config.clone(), Arc::clone(&bridge)).await {
                Ok(worker) => {
                    workers.push(Arc::new(worker));
                }
                Err(err) => {
                    // Clean up already-spawned workers
                    warn!(
                        worker_index = i,
                        "failed to spawn worker, cleaning up {} already spawned",
                        workers.len()
                    );
                    for worker in workers {
                        if let Ok(worker) = Arc::try_unwrap(worker) {
                            let _ = worker.shutdown().await;
                        }
                    }
                    return Err(err.context(format!("failed to spawn worker {}", i)));
                }
            }
        }

        info!(count = workers.len(), "worker pool ready");

        Ok(Self {
            workers,
            cursor: AtomicUsize::new(0),
        })
    }

    /// Get the next worker using round-robin selection.
    ///
    /// This is lock-free and O(1). Workers are selected in order,
    /// wrapping around when all have been used.
    pub fn next_worker(&self) -> Arc<PythonWorker> {
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&self.workers[idx % self.workers.len()])
    }

    /// Get the number of workers in the pool.
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    /// Get all workers in the pool (for health checks, etc.)
    pub fn workers(&self) -> &[Arc<PythonWorker>] {
        &self.workers
    }

    /// Gracefully shut down all workers in the pool.
    ///
    /// Workers are shut down in order. Any workers still in use
    /// (shared references exist) are skipped with a warning.
    pub async fn shutdown(self) -> AnyResult<()> {
        info!(count = self.workers.len(), "shutting down worker pool");

        for worker in self.workers {
            match Arc::try_unwrap(worker) {
                Ok(worker) => {
                    worker.shutdown().await?;
                }
                Err(arc) => {
                    warn!(
                        worker_id = arc.worker_id(),
                        "worker still in use during shutdown; skipping"
                    );
                }
            }
        }

        info!("worker pool shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = PythonWorkerConfig::new()
            .with_user_module("my_module")
            .with_python_paths(vec![PathBuf::from("/extra/path")]);

        assert_eq!(config.user_modules, vec!["my_module".to_string()]);
        assert_eq!(config.extra_python_paths, vec![PathBuf::from("/extra/path")]);
    }

    #[test]
    fn test_config_with_multiple_modules() {
        let config = PythonWorkerConfig::new()
            .with_user_modules(vec!["module1".to_string(), "module2".to_string()]);

        assert_eq!(config.user_modules, vec!["module1", "module2"]);
    }
}
