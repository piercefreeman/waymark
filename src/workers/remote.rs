//! Remote worker process management.
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
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::sync::RwLock;

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

use super::base::{
    ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError, WorkerPoolMetrics,
    error_to_value,
};
pub use super::base::{RoundTripMetrics, WorkerThroughputSnapshot};
use super::status::{WorkerPoolStats, WorkerPoolStatsSnapshot};
use crate::{
    messages::{self, MessageError, proto},
    server_worker::{WorkerBridgeChannels, WorkerBridgeServer},
};

const LATENCY_SAMPLE_SIZE: usize = 256;
const THROUGHPUT_WINDOW_SECS: u64 = 1;

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
    pub async fn spawn(
        config: PythonWorkerConfig,
        bridge: Arc<WorkerBridgeServer>,
    ) -> AnyResult<Self> {
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

impl Drop for PythonWorker {
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

/// Pool of Python workers for action execution.
///
/// Provides round-robin load balancing across multiple worker processes.
/// Workers are spawned eagerly on pool creation.
///
/// # Example
///
/// ```ignore
/// let config = PythonWorkerConfig::new()
///     .with_user_module("my_app.actions");
/// let pool = PythonWorkerPool::new_with_bridge_addr(config, 4, None, None, 10).await?;
///
/// let metrics = pool.get_worker(0).await.send_action(dispatch).await?;
/// ```
pub struct PythonWorkerPool {
    /// The workers in the pool (RwLock for recycling support)
    workers: RwLock<Vec<Arc<PythonWorker>>>,
    /// Cursor for round-robin selection
    cursor: AtomicUsize,
    /// Shared metrics tracker for throughput + latency.
    metrics: StdMutex<WorkerPoolMetrics>,
    /// Action counts per worker slot (for lifecycle tracking)
    action_counts: Vec<AtomicU64>,
    /// In-flight action counts per worker slot (for concurrency control)
    in_flight_counts: Vec<AtomicUsize>,
    /// Maximum concurrent actions per worker
    max_concurrent_per_worker: usize,
    /// Maximum actions per worker before recycling (None = no limit)
    max_action_lifecycle: Option<u64>,
    /// Bridge server for spawning replacement workers
    bridge: Arc<WorkerBridgeServer>,
    /// Worker configuration for spawning replacements
    config: PythonWorkerConfig,
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
    /// * `max_action_lifecycle` - Maximum actions per worker before recycling (None = no limit)
    /// * `max_concurrent_per_worker` - Maximum concurrent actions per worker (default 10)
    ///
    /// # Errors
    ///
    /// Returns an error if any worker fails to spawn or connect.
    pub async fn new(
        config: PythonWorkerConfig,
        count: usize,
        bridge: Arc<WorkerBridgeServer>,
        max_action_lifecycle: Option<u64>,
    ) -> AnyResult<Self> {
        Self::new_with_concurrency(config, count, bridge, max_action_lifecycle, 10).await
    }

    /// Create a new worker pool with explicit concurrency limit.
    pub async fn new_with_concurrency(
        config: PythonWorkerConfig,
        count: usize,
        bridge: Arc<WorkerBridgeServer>,
        max_action_lifecycle: Option<u64>,
        max_concurrent_per_worker: usize,
    ) -> AnyResult<Self> {
        let worker_count = count.max(1);
        info!(
            count = worker_count,
            max_action_lifecycle = ?max_action_lifecycle,
            "spawning python worker pool"
        );

        // Spawn all workers in parallel to reduce boot time.
        let spawn_handles: Vec<_> = (0..worker_count)
            .map(|_| {
                let cfg = config.clone();
                let br = Arc::clone(&bridge);
                tokio::spawn(async move { PythonWorker::spawn(cfg, br).await })
            })
            .collect();

        let mut workers = Vec::with_capacity(worker_count);
        for (i, handle) in spawn_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(worker)) => {
                    workers.push(Arc::new(worker));
                }
                result @ (Ok(Err(_)) | Err(_)) => {
                    let err = match result {
                        Ok(Err(e)) => e,
                        Err(e) => anyhow::Error::from(e),
                        _ => unreachable!(),
                    };
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

        let worker_ids = workers.iter().map(|worker| worker.worker_id()).collect();
        let action_counts = (0..worker_count).map(|_| AtomicU64::new(0)).collect();
        let in_flight_counts = (0..worker_count).map(|_| AtomicUsize::new(0)).collect();
        Ok(Self {
            workers: RwLock::new(workers),
            cursor: AtomicUsize::new(0),
            metrics: StdMutex::new(WorkerPoolMetrics::new(
                worker_ids,
                Duration::from_secs(THROUGHPUT_WINDOW_SECS),
                LATENCY_SAMPLE_SIZE,
            )),
            action_counts,
            in_flight_counts,
            max_concurrent_per_worker: max_concurrent_per_worker.max(1),
            max_action_lifecycle,
            bridge,
            config,
        })
    }

    /// Create a new worker pool and spawn its own bridge server.
    pub async fn new_with_bridge_addr(
        config: PythonWorkerConfig,
        count: usize,
        bind_addr: Option<SocketAddr>,
        max_action_lifecycle: Option<u64>,
        max_concurrent_per_worker: usize,
    ) -> AnyResult<Self> {
        let bridge = WorkerBridgeServer::start(bind_addr).await?;
        match Self::new_with_concurrency(
            config,
            count,
            Arc::clone(&bridge),
            max_action_lifecycle,
            max_concurrent_per_worker,
        )
        .await
        {
            Ok(pool) => Ok(pool),
            Err(err) => {
                bridge.shutdown().await;
                Err(err)
            }
        }
    }

    /// Return the bridge address for worker connections.
    pub fn bridge_addr(&self) -> SocketAddr {
        self.bridge.addr()
    }

    /// Get a worker by index.
    ///
    /// Returns a clone of the Arc for the worker at the given index.
    pub async fn get_worker(&self, idx: usize) -> Arc<PythonWorker> {
        let workers = self.workers.read().await;
        Arc::clone(&workers[idx % workers.len()])
    }

    /// Get the next worker index using round-robin selection.
    ///
    /// This is lock-free and O(1). Returns the index that can be used
    /// with `get_worker` to fetch the actual worker.
    pub fn next_worker_idx(&self) -> usize {
        self.cursor.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the number of workers in the pool.
    pub fn len(&self) -> usize {
        self.action_counts.len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.action_counts.is_empty()
    }

    /// Get the maximum concurrent actions per worker.
    pub fn max_concurrent_per_worker(&self) -> usize {
        self.max_concurrent_per_worker
    }

    /// Get total capacity (worker_count * max_concurrent_per_worker).
    pub fn total_capacity(&self) -> usize {
        self.len() * self.max_concurrent_per_worker
    }

    /// Get total in-flight actions across all workers.
    pub fn total_in_flight(&self) -> usize {
        self.in_flight_counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }

    /// Get available capacity (total_capacity - total_in_flight).
    pub fn available_capacity(&self) -> usize {
        self.total_capacity().saturating_sub(self.total_in_flight())
    }

    /// Try to acquire a slot for the next available worker.
    ///
    /// Returns `Some(worker_idx)` if a slot was acquired, `None` if all workers
    /// are at capacity. Uses round-robin selection among workers with capacity.
    pub fn try_acquire_slot(&self) -> Option<usize> {
        let worker_count = self.len();
        if worker_count == 0 {
            return None;
        }

        // Try each worker starting from the current cursor position
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for i in 0..worker_count {
            let idx = (start + i) % worker_count;
            if self.try_acquire_slot_for_worker(idx) {
                return Some(idx);
            }
        }
        None
    }

    /// Try to acquire a slot for a specific worker.
    ///
    /// Returns `true` if the slot was acquired, `false` if the worker is at capacity.
    pub fn try_acquire_slot_for_worker(&self, worker_idx: usize) -> bool {
        let Some(counter) = self.in_flight_counts.get(worker_idx % self.len()) else {
            return false;
        };

        // CAS loop to atomically increment if below limit
        loop {
            let current = counter.load(Ordering::Acquire);
            if current >= self.max_concurrent_per_worker {
                return false;
            }
            match counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // Retry
            }
        }
    }

    /// Release a slot for a worker.
    ///
    /// Should be called when an action completes (via `record_completion`).
    pub fn release_slot(&self, worker_idx: usize) {
        if let Some(counter) = self.in_flight_counts.get(worker_idx % self.len()) {
            // Saturating sub to avoid underflow in case of bugs
            let prev = counter.fetch_sub(1, Ordering::Release);
            if prev == 0 {
                warn!(worker_idx, "release_slot called with zero in-flight count");
                counter.store(0, Ordering::Release);
            }
        }
    }

    /// Get in-flight count for a specific worker.
    pub fn in_flight_for_worker(&self, worker_idx: usize) -> usize {
        self.in_flight_counts
            .get(worker_idx % self.len())
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get a snapshot of all workers in the pool.
    pub async fn workers_snapshot(&self) -> Vec<Arc<PythonWorker>> {
        self.workers.read().await.clone()
    }

    /// Get throughput snapshots for all workers.
    ///
    /// Returns worker throughput metrics including completion counts and rates.
    pub fn throughput_snapshots(&self) -> Vec<WorkerThroughputSnapshot> {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.throughput_snapshots(Instant::now())
        } else {
            Vec::new()
        }
    }

    /// Record the latest latency measurements for median reporting.
    pub fn record_latency(&self, ack_latency: Duration, worker_duration: Duration) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.record_latency(ack_latency, worker_duration);
        }
    }

    /// Return the current median dequeue/handling latencies in milliseconds.
    pub fn median_latencies_ms(&self) -> (Option<i64>, Option<i64>) {
        if let Ok(metrics) = self.metrics.lock() {
            metrics.median_latencies_ms()
        } else {
            (None, None)
        }
    }

    /// Get queue statistics: (dispatch_queue_size, total_in_flight).
    pub fn queue_stats(&self) -> (usize, usize) {
        let total_in_flight: usize = self
            .in_flight_counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum();
        // dispatch_queue_size would require access to the bridge's queue
        // For now, return 0 as placeholder
        (0, total_in_flight)
    }

    /// Record an action completion for a worker and trigger recycling if needed.
    ///
    /// This decrements the in-flight count and increments the action count for
    /// the worker at the given index. If `max_action_lifecycle` is set and the
    /// count reaches or exceeds the threshold, a background task is spawned to
    /// recycle the worker.
    pub fn record_completion(&self, worker_idx: usize, pool: Arc<PythonWorkerPool>) {
        // Release the in-flight slot
        self.release_slot(worker_idx);

        // Update throughput tracking
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.record_completion(worker_idx);
            if tracing::enabled!(tracing::Level::TRACE) {
                let snapshots = metrics.throughput_snapshots(Instant::now());
                if let Some(snapshot) = snapshots.get(worker_idx) {
                    trace!(
                        worker_id = snapshot.worker_id,
                        throughput_per_min = snapshot.throughput_per_min,
                        total_completed = snapshot.total_completed,
                        last_action_at = ?snapshot.last_action_at,
                        "worker throughput snapshot"
                    );
                }
            }
        }

        // Increment action count
        if let Some(counter) = self.action_counts.get(worker_idx) {
            let new_count = counter.fetch_add(1, Ordering::SeqCst) + 1;

            // Check if recycling is needed
            if let Some(max_lifecycle) = self.max_action_lifecycle
                && new_count >= max_lifecycle
            {
                info!(
                    worker_idx,
                    action_count = new_count,
                    max_lifecycle,
                    "worker reached action lifecycle limit, scheduling recycle"
                );
                // Spawn a background task to recycle this worker
                tokio::spawn(async move {
                    if let Err(err) = pool.recycle_worker(worker_idx).await {
                        error!(worker_idx, ?err, "failed to recycle worker");
                    }
                });
            }
        }
    }

    /// Recycle a worker at the given index.
    ///
    /// Spawns a new worker and replaces the old one. The old worker
    /// will be shut down once all in-flight actions complete (when
    /// its Arc reference count drops to zero).
    async fn recycle_worker(&self, worker_idx: usize) -> AnyResult<()> {
        // Spawn the replacement worker first
        let new_worker = PythonWorker::spawn(self.config.clone(), Arc::clone(&self.bridge)).await?;
        let new_worker_id = new_worker.worker_id();

        // Replace the worker in the pool
        let old_worker = {
            let mut workers = self.workers.write().await;
            let idx = worker_idx % workers.len();
            std::mem::replace(&mut workers[idx], Arc::new(new_worker))
        };

        // Reset the action count for this slot
        if let Some(counter) = self
            .action_counts
            .get(worker_idx % self.action_counts.len())
        {
            counter.store(0, Ordering::SeqCst);
        }

        // Update throughput tracker with new worker ID
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.reset_worker(worker_idx, new_worker_id);
        }

        info!(
            worker_idx,
            old_worker_id = old_worker.worker_id(),
            new_worker_id,
            "recycled worker"
        );

        // The old worker will be cleaned up when its Arc drops
        // (once all in-flight actions complete)

        Ok(())
    }

    /// Get the current action count for a worker slot.
    ///
    /// Returns the number of actions that have been completed by the worker
    /// at the given index since it was last spawned/recycled.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn get_action_count(&self, worker_idx: usize) -> u64 {
        self.action_counts
            .get(worker_idx)
            .map(|c| c.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Get the maximum action lifecycle setting.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn max_lifecycle(&self) -> Option<u64> {
        self.max_action_lifecycle
    }

    /// Gracefully shut down all workers in the pool.
    ///
    /// Workers are shut down in order. Any workers still in use
    /// (shared references exist) are skipped with a warning.
    pub async fn shutdown(self) -> AnyResult<()> {
        let workers = self.workers.into_inner();
        info!(count = workers.len(), "shutting down worker pool");

        for worker in workers {
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

        self.bridge.shutdown().await;
        info!("worker pool shutdown complete");
        Ok(())
    }
}

fn kwargs_to_workflow_arguments(kwargs: &HashMap<String, Value>) -> proto::WorkflowArguments {
    let mut arguments = Vec::with_capacity(kwargs.len());
    for (key, value) in kwargs {
        let arg_value = messages::json_to_workflow_argument_value(value);
        arguments.push(proto::WorkflowArgument {
            key: key.clone(),
            value: Some(arg_value),
        });
    }
    proto::WorkflowArguments { arguments }
}

fn normalize_error_value(error: Value) -> Value {
    let Value::Object(mut map) = error else {
        return error;
    };

    if let Some(Value::Object(exception)) = map.remove("__exception__") {
        return ensure_error_fields(exception);
    }

    ensure_error_fields(map)
}

fn ensure_error_fields(mut map: serde_json::Map<String, Value>) -> Value {
    let error_type = map
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("RemoteWorkerError")
        .to_string();
    let error_message = map
        .get("message")
        .and_then(|value| value.as_str())
        .unwrap_or("remote worker error")
        .to_string();
    if !map.contains_key("type") {
        map.insert("type".to_string(), Value::String(error_type));
    }
    if !map.contains_key("message") {
        map.insert("message".to_string(), Value::String(error_message));
    }
    Value::Object(map)
}

fn decode_action_result(metrics: &RoundTripMetrics) -> Value {
    let payload =
        messages::workflow_arguments_to_json(&metrics.response_payload).unwrap_or(Value::Null);
    if metrics.success {
        if let Value::Object(mut map) = payload {
            if let Some(result) = map.remove("result") {
                return result;
            }
            return Value::Object(map);
        }
        return payload;
    }

    if let Value::Object(mut map) = payload {
        if let Some(error) = map.remove("error") {
            return normalize_error_value(error);
        }
        return Value::Object(map);
    }

    let error_type = metrics.error_type.as_deref().unwrap_or("RemoteWorkerError");
    let error_message = metrics
        .error_message
        .as_deref()
        .unwrap_or("remote worker error");
    error_to_value(&WorkerPoolError::new(error_type, error_message))
}

async fn execute_remote_request(
    pool: Arc<PythonWorkerPool>,
    request: ActionRequest,
) -> ActionCompletion {
    let executor_id = request.executor_id;
    let execution_id = request.execution_id;
    let Some(module_name) = request.module_name.clone() else {
        return ActionCompletion {
            executor_id,
            execution_id,
            result: error_to_value(&WorkerPoolError::new(
                "RemoteWorkerPoolError",
                "missing module name for action request",
            )),
        };
    };

    let worker_idx = loop {
        if let Some(idx) = pool.try_acquire_slot() {
            break idx;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };

    let worker = pool.get_worker(worker_idx).await;
    let dispatch = ActionDispatchPayload {
        action_id: execution_id.to_string(),
        instance_id: executor_id.to_string(),
        sequence: 0,
        action_name: request.action_name,
        module_name,
        kwargs: kwargs_to_workflow_arguments(&request.kwargs),
        timeout_seconds: 0,
        max_retries: 0,
        attempt_number: 0,
        dispatch_token: Uuid::new_v4(),
    };

    match worker.send_action(dispatch).await {
        Ok(metrics) => {
            pool.record_latency(metrics.ack_latency, metrics.worker_duration);
            pool.record_completion(worker_idx, Arc::clone(&pool));
            ActionCompletion {
                executor_id,
                execution_id,
                result: decode_action_result(&metrics),
            }
        }
        Err(err) => {
            pool.release_slot(worker_idx);
            ActionCompletion {
                executor_id,
                execution_id,
                result: error_to_value(&WorkerPoolError::new(
                    "RemoteWorkerPoolError",
                    err.to_string(),
                )),
            }
        }
    }
}

struct RemoteWorkerPoolInner {
    pool: Arc<PythonWorkerPool>,
    request_tx: mpsc::Sender<ActionRequest>,
    request_rx: StdMutex<Option<mpsc::Receiver<ActionRequest>>>,
    completion_tx: mpsc::Sender<ActionCompletion>,
    completion_rx: Mutex<mpsc::Receiver<ActionCompletion>>,
    launched: AtomicBool,
}

/// BaseWorkerPool implementation backed by a Python worker cluster.
#[derive(Clone)]
pub struct RemoteWorkerPool {
    inner: Arc<RemoteWorkerPoolInner>,
}

impl RemoteWorkerPool {
    const DEFAULT_QUEUE_CAPACITY: usize = 1024;

    pub fn new(pool: Arc<PythonWorkerPool>) -> Self {
        Self::with_capacity(
            pool,
            Self::DEFAULT_QUEUE_CAPACITY,
            Self::DEFAULT_QUEUE_CAPACITY,
        )
    }

    pub fn with_capacity(
        pool: Arc<PythonWorkerPool>,
        request_capacity: usize,
        completion_capacity: usize,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel(request_capacity.max(1));
        let (completion_tx, completion_rx) = mpsc::channel(completion_capacity.max(1));
        Self {
            inner: Arc::new(RemoteWorkerPoolInner {
                pool,
                request_tx,
                request_rx: StdMutex::new(Some(request_rx)),
                completion_tx,
                completion_rx: Mutex::new(completion_rx),
                launched: AtomicBool::new(false),
            }),
        }
    }

    pub async fn new_with_config(
        config: PythonWorkerConfig,
        count: usize,
        bind_addr: Option<SocketAddr>,
        max_action_lifecycle: Option<u64>,
        max_concurrent_per_worker: usize,
    ) -> AnyResult<Self> {
        let worker_count = count.max(1);
        let per_worker = max_concurrent_per_worker.max(1);
        let queue_capacity = worker_count
            .saturating_mul(per_worker)
            .saturating_mul(2)
            .max(Self::DEFAULT_QUEUE_CAPACITY);
        let pool = PythonWorkerPool::new_with_bridge_addr(
            config,
            count,
            bind_addr,
            max_action_lifecycle,
            max_concurrent_per_worker,
        )
        .await?;
        Ok(Self::with_capacity(
            Arc::new(pool),
            queue_capacity,
            queue_capacity,
        ))
    }

    pub fn bridge_addr(&self) -> SocketAddr {
        self.inner.pool.bridge_addr()
    }

    pub async fn shutdown(self) -> AnyResult<()> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => match Arc::try_unwrap(inner.pool) {
                Ok(pool) => pool.shutdown().await,
                Err(_) => {
                    warn!("worker pool still referenced during shutdown; skipping shutdown");
                    Ok(())
                }
            },
            Err(_) => {
                warn!("remote worker pool still referenced during shutdown; skipping shutdown");
                Ok(())
            }
        }
    }
}

impl BaseWorkerPool for RemoteWorkerPool {
    fn launch<'a>(&'a self) -> futures::future::BoxFuture<'a, Result<(), WorkerPoolError>> {
        Box::pin(async move {
            if self.inner.launched.swap(true, Ordering::SeqCst) {
                return Ok(());
            }

            let request_rx = {
                let mut guard = self.inner.request_rx.lock().map_err(|_| {
                    WorkerPoolError::new("RemoteWorkerPoolError", "failed to lock request receiver")
                })?;
                guard.take()
            };

            let Some(mut request_rx) = request_rx else {
                return Ok(());
            };

            let pool = Arc::clone(&self.inner.pool);
            let completion_tx = self.inner.completion_tx.clone();

            tokio::spawn(async move {
                while let Some(request) = request_rx.recv().await {
                    let completion_tx = completion_tx.clone();
                    let pool = Arc::clone(&pool);
                    tokio::spawn(async move {
                        let completion = execute_remote_request(pool, request).await;
                        let _ = completion_tx.send(completion).await;
                    });
                }
            });

            Ok(())
        })
    }

    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        self.inner.request_tx.try_send(request).map_err(|err| {
            WorkerPoolError::new(
                "RemoteWorkerPoolError",
                format!("failed to enqueue action request: {err}"),
            )
        })
    }

    fn get_complete<'a>(&'a self) -> futures::future::BoxFuture<'a, Vec<ActionCompletion>> {
        Box::pin(async move {
            let mut receiver = self.inner.completion_rx.lock().await;
            let mut completions = Vec::new();
            match receiver.recv().await {
                Some(first) => completions.push(first),
                None => return completions,
            }
            while let Ok(value) = receiver.try_recv() {
                completions.push(value);
            }
            completions
        })
    }
}

impl WorkerPoolStats for PythonWorkerPool {
    fn stats_snapshot(&self) -> WorkerPoolStatsSnapshot {
        let snapshots = self.throughput_snapshots();
        let active_workers = snapshots.len() as u16;
        let throughput_per_min: f64 = snapshots.iter().map(|s| s.throughput_per_min).sum();
        let total_completed: i64 = snapshots.iter().map(|s| s.total_completed as i64).sum();
        let last_action_at = snapshots.iter().filter_map(|s| s.last_action_at).max();
        let (dispatch_queue_size, total_in_flight) = self.queue_stats();
        let (median_dequeue_ms, median_handling_ms) = self.median_latencies_ms();

        WorkerPoolStatsSnapshot {
            active_workers,
            throughput_per_min,
            total_completed,
            last_action_at,
            dispatch_queue_size,
            total_in_flight,
            median_dequeue_ms,
            median_handling_ms,
        }
    }
}

impl WorkerPoolStats for RemoteWorkerPool {
    fn stats_snapshot(&self) -> WorkerPoolStatsSnapshot {
        self.inner.pool.stats_snapshot()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::process::Stdio;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    };

    use serde_json::json;
    use tokio::process::Child;

    use super::*;
    use crate::workers::BaseWorkerPool;

    #[test]
    fn test_config_builder() {
        let config = PythonWorkerConfig::new()
            .with_user_module("my_module")
            .with_python_paths(vec![PathBuf::from("/extra/path")]);

        assert_eq!(config.user_modules, vec!["my_module".to_string()]);
        assert_eq!(
            config.extra_python_paths,
            vec![PathBuf::from("/extra/path")]
        );
    }

    #[test]
    fn test_config_with_multiple_modules() {
        let config = PythonWorkerConfig::new()
            .with_user_modules(vec!["module1".to_string(), "module2".to_string()]);

        assert_eq!(config.user_modules, vec!["module1", "module2"]);
    }

    #[test]
    fn test_default_runner_detection() {
        // Should return uv as fallback if rappel-worker not in PATH
        let (path, args) = default_runner();
        // Either rappel-worker was found, or we get uv with args
        if args.is_empty() {
            assert!(path.to_string_lossy().contains("rappel-worker"));
        } else {
            assert_eq!(path, PathBuf::from("uv"));
            assert_eq!(args, vec!["run", "python", "-m", "rappel.worker"]);
        }
    }

    fn make_string_kwarg(key: &str, value: &str) -> proto::WorkflowArgument {
        proto::WorkflowArgument {
            key: key.to_string(),
            value: Some(proto::WorkflowArgumentValue {
                kind: Some(proto::workflow_argument_value::Kind::Primitive(
                    proto::PrimitiveWorkflowArgument {
                        kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                            value.to_string(),
                        )),
                    },
                )),
            }),
        }
    }

    fn spawn_stub_child() -> Child {
        #[cfg(windows)]
        {
            Command::new("cmd")
                .args(["/C", "timeout", "/T", "60", "/NOBREAK"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn windows stub child")
        }
        #[cfg(not(windows))]
        {
            Command::new("sleep")
                .arg("60")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn unix stub child")
        }
    }

    async fn test_bridge() -> Option<Arc<WorkerBridgeServer>> {
        match WorkerBridgeServer::start(None).await {
            Ok(server) => Some(server),
            Err(err) => {
                let message = format!("{err:?}");
                if message.contains("Operation not permitted")
                    || message.contains("Permission denied")
                {
                    None
                } else {
                    panic!("start worker bridge: {err}");
                }
            }
        }
    }

    fn make_result_payload(value: Value) -> proto::WorkflowArguments {
        proto::WorkflowArguments {
            arguments: vec![proto::WorkflowArgument {
                key: "result".to_string(),
                value: Some(messages::json_to_workflow_argument_value(&value)),
            }],
        }
    }

    fn make_test_worker(
        worker_id: u64,
    ) -> (
        PythonWorker,
        mpsc::Receiver<proto::Envelope>,
        mpsc::Sender<proto::Envelope>,
    ) {
        let (to_worker, from_runner) = mpsc::channel(16);
        let (to_runner, from_worker) = mpsc::channel(16);
        let shared = Arc::new(Mutex::new(SharedState::new()));
        let reader_shared = Arc::clone(&shared);
        let reader_handle = tokio::spawn(async move {
            let mut incoming = from_worker;
            let _ = PythonWorker::reader_loop(&mut incoming, reader_shared).await;
        });

        let worker = PythonWorker {
            child: spawn_stub_child(),
            sender: to_worker,
            shared,
            next_delivery: AtomicU64::new(1),
            reader_handle: Some(reader_handle),
            worker_id,
        };
        (worker, from_runner, to_runner)
    }

    async fn make_single_worker_pool() -> Option<(
        Arc<PythonWorkerPool>,
        mpsc::Receiver<proto::Envelope>,
        mpsc::Sender<proto::Envelope>,
    )> {
        let bridge = test_bridge().await?;
        let (worker, outgoing, incoming) = make_test_worker(0);
        let pool = PythonWorkerPool {
            workers: RwLock::new(vec![Arc::new(worker)]),
            cursor: AtomicUsize::new(0),
            metrics: StdMutex::new(WorkerPoolMetrics::new(
                vec![0],
                Duration::from_secs(THROUGHPUT_WINDOW_SECS),
                LATENCY_SAMPLE_SIZE,
            )),
            action_counts: vec![AtomicU64::new(0)],
            in_flight_counts: vec![AtomicUsize::new(0)],
            max_concurrent_per_worker: 2,
            max_action_lifecycle: None,
            bridge,
            config: PythonWorkerConfig::new(),
        };
        Some((Arc::new(pool), outgoing, incoming))
    }

    #[tokio::test]
    async fn test_send_action_roundtrip_happy_path() {
        let (worker, mut outgoing, incoming) = make_test_worker(7);
        let dispatch_token = Uuid::new_v4();

        let responder = tokio::spawn(async move {
            let envelope = outgoing.recv().await.expect("dispatch envelope");
            assert_eq!(
                proto::MessageKind::try_from(envelope.kind).ok(),
                Some(proto::MessageKind::ActionDispatch)
            );
            let dispatch = messages::decode_message::<proto::ActionDispatch>(&envelope.payload)
                .expect("decode dispatch");
            assert_eq!(dispatch.action_name, "greet");

            incoming
                .send(proto::Envelope {
                    delivery_id: envelope.delivery_id + 100,
                    partition_id: 0,
                    kind: proto::MessageKind::Ack as i32,
                    payload: messages::encode_message(&proto::Ack {
                        acked_delivery_id: envelope.delivery_id,
                    }),
                })
                .await
                .expect("send ack");
            incoming
                .send(proto::Envelope {
                    delivery_id: envelope.delivery_id,
                    partition_id: 0,
                    kind: proto::MessageKind::ActionResult as i32,
                    payload: messages::encode_message(&proto::ActionResult {
                        action_id: dispatch.action_id,
                        success: true,
                        payload: Some(make_result_payload(json!("hello"))),
                        worker_start_ns: 10,
                        worker_end_ns: 42,
                        dispatch_token: Some(dispatch_token.to_string()),
                        error_type: None,
                        error_message: None,
                    }),
                })
                .await
                .expect("send action result");
        });

        let metrics = worker
            .send_action(ActionDispatchPayload {
                action_id: "action-1".to_string(),
                instance_id: "instance-1".to_string(),
                sequence: 1,
                action_name: "greet".to_string(),
                module_name: "tests.actions".to_string(),
                kwargs: proto::WorkflowArguments {
                    arguments: vec![make_string_kwarg("name", "World")],
                },
                timeout_seconds: 30,
                max_retries: 0,
                attempt_number: 0,
                dispatch_token,
            })
            .await
            .expect("send action");

        responder.await.expect("responder task");
        assert!(metrics.success);
        assert_eq!(metrics.action_id, "action-1");
        assert_eq!(metrics.instance_id, "instance-1");
        assert_eq!(metrics.dispatch_token, Some(dispatch_token));
        assert_eq!(metrics.worker_duration, Duration::from_nanos(32));

        worker.shutdown().await.expect("shutdown worker");
    }

    #[test]
    fn test_concurrency_slot_logic() {
        // Test the atomic slot logic without spawning workers
        let in_flight = AtomicUsize::new(0);
        let max_concurrent = 3;

        // Helper to try acquire
        let try_acquire = || {
            loop {
                let current = in_flight.load(Ordering::Acquire);
                if current >= max_concurrent {
                    return false;
                }
                match in_flight.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return true,
                    Err(_) => continue,
                }
            }
        };

        // Acquire up to max
        assert!(try_acquire());
        assert!(try_acquire());
        assert!(try_acquire());
        // At capacity
        assert!(!try_acquire());
        assert_eq!(in_flight.load(Ordering::Relaxed), 3);

        // Release one
        in_flight.fetch_sub(1, Ordering::Release);
        assert_eq!(in_flight.load(Ordering::Relaxed), 2);

        // Can acquire again
        assert!(try_acquire());
        assert!(!try_acquire());
    }

    #[tokio::test]
    async fn test_execute_remote_request_happy_path() {
        let Some((pool, mut outgoing, incoming)) = make_single_worker_pool().await else {
            return;
        };
        let request = ActionRequest {
            executor_id: Uuid::new_v4(),
            execution_id: Uuid::new_v4(),
            action_name: "double".to_string(),
            module_name: Some("tests.actions".to_string()),
            kwargs: HashMap::from([("value".to_string(), Value::Number(9.into()))]),
        };

        let responder = tokio::spawn(async move {
            let envelope = outgoing.recv().await.expect("dispatch envelope");
            incoming
                .send(proto::Envelope {
                    delivery_id: envelope.delivery_id + 1,
                    partition_id: 0,
                    kind: proto::MessageKind::Ack as i32,
                    payload: messages::encode_message(&proto::Ack {
                        acked_delivery_id: envelope.delivery_id,
                    }),
                })
                .await
                .expect("send ack");
            incoming
                .send(proto::Envelope {
                    delivery_id: envelope.delivery_id,
                    partition_id: 0,
                    kind: proto::MessageKind::ActionResult as i32,
                    payload: messages::encode_message(&proto::ActionResult {
                        action_id: "ignored".to_string(),
                        success: true,
                        payload: Some(make_result_payload(Value::Number(18.into()))),
                        worker_start_ns: 100,
                        worker_end_ns: 125,
                        dispatch_token: None,
                        error_type: None,
                        error_message: None,
                    }),
                })
                .await
                .expect("send result");
        });

        let completion = execute_remote_request(Arc::clone(&pool), request.clone()).await;
        responder.await.expect("responder task");
        assert_eq!(completion.executor_id, request.executor_id);
        assert_eq!(completion.execution_id, request.execution_id);
        assert_eq!(completion.result, Value::Number(18.into()));
        assert_eq!(pool.total_in_flight(), 0);

        if let Ok(pool) = Arc::try_unwrap(pool) {
            pool.shutdown().await.expect("shutdown pool");
        }
    }

    #[tokio::test]
    async fn test_remote_worker_pool_launch_queue_get_complete_happy_path() {
        let Some((pool, mut outgoing, incoming)) = make_single_worker_pool().await else {
            return;
        };
        let remote = RemoteWorkerPool::new(Arc::clone(&pool));
        BaseWorkerPool::launch(&remote)
            .await
            .expect("launch remote pool");
        let request = ActionRequest {
            executor_id: Uuid::new_v4(),
            execution_id: Uuid::new_v4(),
            action_name: "square".to_string(),
            module_name: Some("tests.actions".to_string()),
            kwargs: HashMap::from([("value".to_string(), Value::Number(5.into()))]),
        };
        let execution_id = request.execution_id;

        let responder = tokio::spawn(async move {
            let envelope = outgoing.recv().await.expect("dispatch envelope");
            incoming
                .send(proto::Envelope {
                    delivery_id: envelope.delivery_id + 5,
                    partition_id: 0,
                    kind: proto::MessageKind::Ack as i32,
                    payload: messages::encode_message(&proto::Ack {
                        acked_delivery_id: envelope.delivery_id,
                    }),
                })
                .await
                .expect("send ack");
            incoming
                .send(proto::Envelope {
                    delivery_id: envelope.delivery_id,
                    partition_id: 0,
                    kind: proto::MessageKind::ActionResult as i32,
                    payload: messages::encode_message(&proto::ActionResult {
                        action_id: "ignored".to_string(),
                        success: true,
                        payload: Some(make_result_payload(Value::Number(25.into()))),
                        worker_start_ns: 300,
                        worker_end_ns: 360,
                        dispatch_token: None,
                        error_type: None,
                        error_message: None,
                    }),
                })
                .await
                .expect("send result");
        });

        BaseWorkerPool::queue(&remote, request).expect("queue request");
        let completions = BaseWorkerPool::get_complete(&remote).await;
        responder.await.expect("responder task");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].execution_id, execution_id);
        assert_eq!(completions[0].result, Value::Number(25.into()));

        drop(remote);
        tokio::time::sleep(Duration::from_millis(10)).await;
        if let Ok(pool) = Arc::try_unwrap(pool) {
            pool.shutdown().await.expect("shutdown pool");
        }
    }

    #[test]
    fn test_action_result_success_false_deserialize() {
        use prost::Message;

        // These are the bytes from Python when success=False is set
        // The success field is NOT included because it's the default value in proto3
        let success_false_bytes: &[u8] = &[0x0a, 0x04, 0x74, 0x65, 0x73, 0x74];

        // These are the bytes from Python when success=True is set
        let success_true_bytes: &[u8] = &[0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01];

        // Deserialize success=False case
        let result_false =
            proto::ActionResult::decode(success_false_bytes).expect("decode success=false");
        assert_eq!(result_false.action_id, "test");
        assert!(
            !result_false.success,
            "success should be false when field is omitted (proto3 default)"
        );

        // Deserialize success=True case
        let result_true =
            proto::ActionResult::decode(success_true_bytes).expect("decode success=true");
        assert_eq!(result_true.action_id, "test");
        assert!(
            result_true.success,
            "success should be true when field is 1"
        );
    }
}
