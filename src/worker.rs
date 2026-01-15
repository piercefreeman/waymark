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
    collections::{HashMap, VecDeque},
    env,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use anyhow::{Context, Result as AnyResult, anyhow};
use chrono::{DateTime, Utc};
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

#[derive(Debug, Clone)]
pub(crate) struct WorkerThroughputSnapshot {
    pub worker_id: u64,
    pub throughput_per_min: f64,
    pub total_completed: u64,
    pub last_action_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct WorkerThroughputState {
    worker_id: u64,
    total_completed: u64,
    recent_completions: VecDeque<Instant>,
    last_action_at: Option<DateTime<Utc>>,
}

impl WorkerThroughputState {
    fn new(worker_id: u64) -> Self {
        Self {
            worker_id,
            total_completed: 0,
            recent_completions: VecDeque::new(),
            last_action_at: None,
        }
    }

    fn prune_before(&mut self, cutoff: Instant) {
        while self
            .recent_completions
            .front()
            .is_some_and(|instant| *instant < cutoff)
        {
            self.recent_completions.pop_front();
        }
    }
}

#[derive(Debug)]
struct WorkerThroughputTracker {
    window: Duration,
    workers: Vec<WorkerThroughputState>,
}

impl WorkerThroughputTracker {
    fn new(worker_ids: Vec<u64>, window: Duration) -> Self {
        let workers = worker_ids
            .into_iter()
            .map(WorkerThroughputState::new)
            .collect();
        Self { window, workers }
    }

    fn record_completion(&mut self, worker_idx: usize) {
        let now = Instant::now();
        let wall_time = Utc::now();
        self.record_completion_at(worker_idx, now, wall_time);
    }

    fn record_completion_at(&mut self, worker_idx: usize, when: Instant, wall_time: DateTime<Utc>) {
        let Some(worker) = self.workers.get_mut(worker_idx) else {
            return;
        };
        let cutoff = when.checked_sub(self.window).unwrap_or(when);
        worker.prune_before(cutoff);
        worker.recent_completions.push_back(when);
        worker.total_completed = worker.total_completed.saturating_add(1);
        worker.last_action_at = Some(wall_time);
    }

    fn snapshot(&mut self) -> Vec<WorkerThroughputSnapshot> {
        self.snapshot_at(Instant::now())
    }

    fn snapshot_at(&mut self, now: Instant) -> Vec<WorkerThroughputSnapshot> {
        let window_secs = self.window.as_secs_f64();
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        self.workers
            .iter_mut()
            .map(|worker| {
                worker.prune_before(cutoff);
                let throughput_per_min = if window_secs > 0.0 {
                    (worker.recent_completions.len() as f64 / window_secs) * 60.0
                } else {
                    0.0
                };

                WorkerThroughputSnapshot {
                    worker_id: worker.worker_id,
                    throughput_per_min,
                    total_completed: worker.total_completed,
                    last_action_at: worker.last_action_at,
                }
            })
            .collect()
    }
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
/// let bridge = WorkerBridgeServer::start(None).await?;
/// let config = PythonWorkerConfig::new()
///     .with_user_module("my_app.actions");
/// let pool = PythonWorkerPool::new(config, 4, bridge, None).await?;
///
/// let metrics = pool.get_worker(0).await.send_action(dispatch).await?;
/// ```
pub struct PythonWorkerPool {
    /// The workers in the pool (RwLock for recycling support)
    workers: RwLock<Vec<Arc<PythonWorker>>>,
    /// Cursor for round-robin selection
    cursor: AtomicUsize,
    /// Pool identifier for reporting
    pool_id: Uuid,
    /// Throughput tracker for workers in the pool
    throughput: StdMutex<WorkerThroughputTracker>,
    /// Action counts per worker slot (for lifecycle tracking)
    action_counts: Vec<AtomicU64>,
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
        let worker_count = count.max(1);
        info!(
            count = worker_count,
            max_action_lifecycle = ?max_action_lifecycle,
            "spawning python worker pool"
        );

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

        let worker_ids = workers.iter().map(|worker| worker.worker_id()).collect();
        let action_counts = (0..worker_count).map(|_| AtomicU64::new(0)).collect();
        Ok(Self {
            workers: RwLock::new(workers),
            cursor: AtomicUsize::new(0),
            pool_id: Uuid::new_v4(),
            throughput: StdMutex::new(WorkerThroughputTracker::new(
                worker_ids,
                Duration::from_secs(60),
            )),
            action_counts,
            max_action_lifecycle,
            bridge,
            config,
        })
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

    /// Get a snapshot of all workers in the pool.
    pub async fn workers_snapshot(&self) -> Vec<Arc<PythonWorker>> {
        self.workers.read().await.clone()
    }

    pub(crate) fn pool_id(&self) -> Uuid {
        self.pool_id
    }

    /// Record an action completion for a worker and trigger recycling if needed.
    ///
    /// This increments the action count for the worker at the given index.
    /// If `max_action_lifecycle` is set and the count reaches or exceeds the
    /// threshold, a background task is spawned to recycle the worker.
    pub fn record_completion(&self, worker_idx: usize, pool: Arc<PythonWorkerPool>) {
        // Update throughput tracking
        if let Ok(mut tracker) = self.throughput.lock() {
            tracker.record_completion(worker_idx);
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
        if let Ok(mut tracker) = self.throughput.lock() {
            let idx = worker_idx % tracker.workers.len();
            tracker.workers[idx] = WorkerThroughputState::new(new_worker_id);
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

    pub(crate) fn throughput_snapshot(&self) -> Vec<WorkerThroughputSnapshot> {
        if let Ok(mut tracker) = self.throughput.lock() {
            return tracker.snapshot();
        }
        Vec::new()
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

    // Integration tests that require Python workers
    // Run with: cargo test --features integration
    // Or manually with: cargo test -- --ignored

    /// Helper to create test kwargs
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

    #[tokio::test]
    #[ignore] // Requires Python environment
    async fn test_pool_spawn_and_shutdown() {
        let bridge = WorkerBridgeServer::start(None).await.expect("start bridge");
        let config = PythonWorkerConfig::new();

        let pool = PythonWorkerPool::new(config, 2, Arc::clone(&bridge), None)
            .await
            .expect("create pool");

        assert_eq!(pool.len(), 2);
        assert!(!pool.is_empty());

        // Verify workers have sequential IDs
        let workers = pool.workers_snapshot().await;
        assert_eq!(workers[0].worker_id(), 0);
        assert_eq!(workers[1].worker_id(), 1);

        pool.shutdown().await.expect("shutdown pool");
        bridge.shutdown().await;
    }

    #[tokio::test]
    #[ignore] // Requires Python environment
    async fn test_pool_round_robin_selection() {
        let bridge = WorkerBridgeServer::start(None).await.expect("start bridge");
        let config = PythonWorkerConfig::new();

        let pool = PythonWorkerPool::new(config, 3, Arc::clone(&bridge), None)
            .await
            .expect("create pool");

        // Round-robin should cycle through workers
        let idx1 = pool.next_worker_idx();
        let idx2 = pool.next_worker_idx();
        let idx3 = pool.next_worker_idx();
        let idx4 = pool.next_worker_idx(); // Should wrap to first

        let w1 = pool.get_worker(idx1).await;
        let w2 = pool.get_worker(idx2).await;
        let w3 = pool.get_worker(idx3).await;
        let w4 = pool.get_worker(idx4).await;

        assert_eq!(w1.worker_id(), 0);
        assert_eq!(w2.worker_id(), 1);
        assert_eq!(w3.worker_id(), 2);
        assert_eq!(w4.worker_id(), 0); // Wrapped

        pool.shutdown().await.expect("shutdown pool");
        bridge.shutdown().await;
    }

    #[tokio::test]
    #[ignore] // Requires Python environment with test actions
    async fn test_send_action_roundtrip() {
        let bridge = WorkerBridgeServer::start(None).await.expect("start bridge");
        let config = PythonWorkerConfig::new().with_user_module("tests.fixtures.test_actions");

        let pool = PythonWorkerPool::new(config, 1, Arc::clone(&bridge), None)
            .await
            .expect("create pool");

        let dispatch = ActionDispatchPayload {
            action_id: "test-1".to_string(),
            instance_id: "instance-1".to_string(),
            sequence: 0,
            action_name: "greet".to_string(),
            module_name: "tests.fixtures.test_actions".to_string(),
            kwargs: proto::WorkflowArguments {
                arguments: vec![make_string_kwarg("name", "World")],
            },
            timeout_seconds: 30,
            max_retries: 0,
            attempt_number: 0,
            dispatch_token: Uuid::new_v4(),
        };

        let worker = pool.get_worker(0).await;
        let metrics = worker.send_action(dispatch).await.expect("send action");

        assert!(metrics.success);
        assert!(metrics.ack_latency.as_micros() > 0);
        assert!(metrics.round_trip > metrics.ack_latency);
        assert!(metrics.worker_duration.as_nanos() > 0);

        pool.shutdown().await.expect("shutdown pool");
        bridge.shutdown().await;
    }

    #[tokio::test]
    #[ignore] // Requires Python environment
    async fn test_multiple_concurrent_actions() {
        let bridge = WorkerBridgeServer::start(None).await.expect("start bridge");
        let config = PythonWorkerConfig::new().with_user_module("tests.fixtures.test_actions");

        let pool = Arc::new(
            PythonWorkerPool::new(config, 2, Arc::clone(&bridge), None)
                .await
                .expect("create pool"),
        );

        // Spawn multiple concurrent actions
        let mut handles = Vec::new();
        for i in 0..4 {
            let pool = Arc::clone(&pool);
            handles.push(tokio::spawn(async move {
                let dispatch = ActionDispatchPayload {
                    action_id: format!("test-{}", i),
                    instance_id: "instance-1".to_string(),
                    sequence: i,
                    action_name: "greet".to_string(),
                    module_name: "tests.fixtures.test_actions".to_string(),
                    kwargs: proto::WorkflowArguments {
                        arguments: vec![make_string_kwarg("name", &format!("User-{}", i))],
                    },
                    timeout_seconds: 30,
                    max_retries: 0,
                    attempt_number: 0,
                    dispatch_token: Uuid::new_v4(),
                };

                let idx = pool.next_worker_idx();
                let worker = pool.get_worker(idx).await;
                worker.send_action(dispatch).await
            }));
        }

        // All should succeed
        for handle in handles {
            let result = handle.await.expect("task join");
            let metrics = result.expect("action result");
            assert!(metrics.success);
        }

        // Need to get the pool out of the Arc for shutdown
        // In real code, you'd structure this differently
        bridge.shutdown().await;
    }

    #[test]
    fn test_worker_throughput_tracker_window() {
        let base_wall = Utc::now();
        let mut tracker = WorkerThroughputTracker::new(vec![0, 1], Duration::from_secs(60));
        let base = Instant::now();

        tracker.record_completion_at(0, base, base_wall);
        tracker.record_completion_at(0, base + Duration::from_secs(10), base_wall);
        tracker.record_completion_at(0, base + Duration::from_secs(50), base_wall);
        tracker.record_completion_at(0, base + Duration::from_secs(70), base_wall);

        let snapshots = tracker.snapshot_at(base + Duration::from_secs(70));
        let worker_one = snapshots
            .iter()
            .find(|snapshot| snapshot.worker_id == 0)
            .expect("worker zero snapshot");
        let worker_two = snapshots
            .iter()
            .find(|snapshot| snapshot.worker_id == 1)
            .expect("worker one snapshot");

        assert_eq!(worker_one.total_completed, 4);
        assert!((worker_one.throughput_per_min - 3.0).abs() < 0.001);
        assert!(worker_one.last_action_at.is_some());

        assert_eq!(worker_two.total_completed, 0);
        assert_eq!(worker_two.throughput_per_min, 0.0);
        assert!(worker_two.last_action_at.is_none());
    }

    #[tokio::test]
    #[ignore] // Requires Python environment with test actions
    async fn test_worker_recycling_after_lifecycle_limit() {
        let bridge = WorkerBridgeServer::start(None).await.expect("start bridge");
        let config = PythonWorkerConfig::new().with_user_module("tests.fixtures.test_actions");

        // Create pool with max_action_lifecycle = 2
        let pool = Arc::new(
            PythonWorkerPool::new(config, 1, Arc::clone(&bridge), Some(2))
                .await
                .expect("create pool"),
        );

        // Get initial worker ID
        let initial_worker_id = pool.get_worker(0).await.worker_id();
        assert_eq!(initial_worker_id, 0);

        // Send first action - count becomes 1
        let dispatch1 = ActionDispatchPayload {
            action_id: "test-1".to_string(),
            instance_id: "instance-1".to_string(),
            sequence: 0,
            action_name: "greet".to_string(),
            module_name: "tests.fixtures.test_actions".to_string(),
            kwargs: proto::WorkflowArguments {
                arguments: vec![make_string_kwarg("name", "World1")],
            },
            timeout_seconds: 30,
            max_retries: 0,
            attempt_number: 0,
            dispatch_token: Uuid::new_v4(),
        };

        let worker = pool.get_worker(0).await;
        let metrics = worker.send_action(dispatch1).await.expect("send action 1");
        assert!(metrics.success);
        pool.record_completion(0, Arc::clone(&pool));

        // Worker should still be the same (count = 1, threshold = 2)
        let worker_id_after_1 = pool.get_worker(0).await.worker_id();
        assert_eq!(worker_id_after_1, initial_worker_id);

        // Send second action - count becomes 2, triggers recycle
        let dispatch2 = ActionDispatchPayload {
            action_id: "test-2".to_string(),
            instance_id: "instance-1".to_string(),
            sequence: 1,
            action_name: "greet".to_string(),
            module_name: "tests.fixtures.test_actions".to_string(),
            kwargs: proto::WorkflowArguments {
                arguments: vec![make_string_kwarg("name", "World2")],
            },
            timeout_seconds: 30,
            max_retries: 0,
            attempt_number: 0,
            dispatch_token: Uuid::new_v4(),
        };

        let worker = pool.get_worker(0).await;
        let metrics = worker.send_action(dispatch2).await.expect("send action 2");
        assert!(metrics.success);
        pool.record_completion(0, Arc::clone(&pool));

        // Give time for the recycling background task to complete
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Worker should now have a different ID (recycled)
        let worker_id_after_recycle = pool.get_worker(0).await.worker_id();
        assert_ne!(
            worker_id_after_recycle, initial_worker_id,
            "Worker should have been recycled after reaching lifecycle limit"
        );

        // The new worker should have ID 1 (second worker spawned)
        assert_eq!(worker_id_after_recycle, 1);

        // Verify the new worker works correctly
        let dispatch3 = ActionDispatchPayload {
            action_id: "test-3".to_string(),
            instance_id: "instance-1".to_string(),
            sequence: 2,
            action_name: "greet".to_string(),
            module_name: "tests.fixtures.test_actions".to_string(),
            kwargs: proto::WorkflowArguments {
                arguments: vec![make_string_kwarg("name", "World3")],
            },
            timeout_seconds: 30,
            max_retries: 0,
            attempt_number: 0,
            dispatch_token: Uuid::new_v4(),
        };

        let worker = pool.get_worker(0).await;
        let metrics = worker.send_action(dispatch3).await.expect("send action 3");
        assert!(metrics.success);

        bridge.shutdown().await;
    }

    #[tokio::test]
    #[ignore] // Requires Python environment with test actions
    async fn test_worker_no_recycling_when_lifecycle_none() {
        let bridge = WorkerBridgeServer::start(None).await.expect("start bridge");
        let config = PythonWorkerConfig::new().with_user_module("tests.fixtures.test_actions");

        // Create pool with no lifecycle limit
        let pool = Arc::new(
            PythonWorkerPool::new(config, 1, Arc::clone(&bridge), None)
                .await
                .expect("create pool"),
        );

        let initial_worker_id = pool.get_worker(0).await.worker_id();

        // Send several actions
        for i in 0..5 {
            let dispatch = ActionDispatchPayload {
                action_id: format!("test-{}", i),
                instance_id: "instance-1".to_string(),
                sequence: i,
                action_name: "greet".to_string(),
                module_name: "tests.fixtures.test_actions".to_string(),
                kwargs: proto::WorkflowArguments {
                    arguments: vec![make_string_kwarg("name", &format!("World{}", i))],
                },
                timeout_seconds: 30,
                max_retries: 0,
                attempt_number: 0,
                dispatch_token: Uuid::new_v4(),
            };

            let worker = pool.get_worker(0).await;
            let metrics = worker.send_action(dispatch).await.expect("send action");
            assert!(metrics.success);
            pool.record_completion(0, Arc::clone(&pool));
        }

        // Worker should still be the same (no recycling)
        let final_worker_id = pool.get_worker(0).await.worker_id();
        assert_eq!(
            final_worker_id, initial_worker_id,
            "Worker should not have been recycled when lifecycle limit is None"
        );

        bridge.shutdown().await;
    }
}
