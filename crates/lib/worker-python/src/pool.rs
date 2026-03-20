use std::{
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use anyhow::Result as AnyResult;
use tracing::{error, info, trace, warn};

use waymark_worker_metrics::{WorkerPoolMetrics, WorkerThroughputSnapshot};

use crate::{Config, Process};

const LATENCY_SAMPLE_SIZE: usize = 256;
const THROUGHPUT_WINDOW_SECS: u64 = 1;

/// Pool of Python workers for action execution.
///
/// Provides round-robin load balancing across multiple worker processes.
/// Workers are spawned eagerly on pool creation.
///
/// # Example
///
/// ```ignore
/// let config = Config::new()
///     .with_user_module("my_app.actions");
/// let pool = ProcessPool::new_with_bridge_addr(config, 4, None, None, 10).await?;
///
/// let metrics = pool.get_worker(0).await.send_action(dispatch).await?;
/// ```
pub struct Pool {
    /// The workers in the pool (RwLock for recycling support)
    workers: RwLock<Vec<Arc<crate::Process>>>,

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

    /// The registry of the connecting workers.
    connecting_registry: Arc<waymark_worker_remote_connecting::Registry>,

    /// Worker configuration for spawning replacements
    config: Config,
}

impl Pool {
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
        config: Config,
        count: usize,
        max_action_lifecycle: Option<u64>,
        connecting_registry: Arc<waymark_worker_remote_connecting::Registry>,
    ) -> AnyResult<Self> {
        Self::new_with_concurrency(config, count, max_action_lifecycle, connecting_registry, 10)
            .await
    }

    /// Create a new worker pool with explicit concurrency limit.
    pub async fn new_with_concurrency(
        config: Config,
        count: usize,
        max_action_lifecycle: Option<u64>,
        connecting_registry: Arc<waymark_worker_remote_connecting::Registry>,
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
                let reservation = connecting_registry.reserve_worker();
                tokio::spawn(async move { Process::spawn(cfg, reservation).await })
            })
            .collect();

        let mut workers = Vec::with_capacity(worker_count);
        for (i, handle) in spawn_handles.into_iter().enumerate() {
            let result = handle.await.unwrap(); // propagate panics
            match result {
                Ok(worker) => {
                    workers.push(Arc::new(worker));
                }
                Err(err) => {
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
            connecting_registry,
            config,
        })
    }

    /// Get a worker by index.
    ///
    /// Returns a clone of the Arc for the worker at the given index.
    pub async fn get_worker(&self, idx: usize) -> Arc<Process> {
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
    pub async fn workers_snapshot(&self) -> Vec<Arc<Process>> {
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
    pub fn record_completion(&self, worker_idx: usize, pool: Arc<Pool>) {
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
        let reservation = self.connecting_registry.reserve_worker();
        let new_worker = Process::spawn(self.config.clone(), reservation).await?;
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

        info!("worker pool shutdown complete");
        Ok(())
    }
}
