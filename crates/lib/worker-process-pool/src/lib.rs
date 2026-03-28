use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use nonempty_collections::NEVec;
use tokio::sync::RwLock;

use tracing::{error, info, trace, warn};

use waymark_worker_metrics::{WorkerPoolMetrics, WorkerThroughputSnapshot};

const LATENCY_SAMPLE_SIZE: usize = 256;
const THROUGHPUT_WINDOW_SECS: u64 = 1;

type Registry = waymark_worker_reservation::Registry<waymark_worker_message_protocol::Channels>;

type WorkerId = u64;

pub struct WorkerState {
    pub handle: waymark_worker_process::Handle,
    pub sender: waymark_worker_message_protocol::Sender,
    pub id: WorkerId,
}

pub struct Pool<Spec> {
    /// The spec for the worker processes.
    worker_process_spec: Spec,

    /// The registry of the connecting workers.
    workers_registry: Arc<Registry>,

    // Worker ID sequence.
    worker_id_sequence: AtomicU64,

    /// The workers in the pool (RwLock for recycling support)
    worker_processes: RwLock<Vec<Arc<WorkerState>>>,

    /// Cursor for round-robin selection
    cursor: AtomicUsize,

    /// Shared metrics tracker for throughput + latency.
    metrics: StdMutex<WorkerPoolMetrics>,

    /// Action counts per worker slot (for lifecycle tracking)
    action_counts: NEVec<AtomicU64>,

    /// In-flight action counts per worker slot (for concurrency control)
    in_flight_counts: NEVec<AtomicUsize>,

    /// Maximum concurrent actions per worker
    max_concurrent_per_worker: NonZeroUsize,

    /// Maximum actions per worker before recycling (None = no limit)
    max_action_lifecycle: Option<NonZeroU64>,
}

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("unable to spawn worker with index {worker_index}: {error}")]
    WorkerSpawn {
        error: waymark_reserved_process::SpawnError,
        worker_index: usize,
    },
}

impl<Spec> Pool<Spec>
where
    Spec: waymark_worker_process_spec::Spec,
{
    /// Create a new worker pool with explicit concurrency limit.
    pub async fn new_with_concurrency(
        workers_registry: Arc<Registry>,
        worker_process_spec: Spec,
        worker_count: NonZeroUsize,
        max_action_lifecycle: Option<NonZeroU64>,
        max_concurrent_per_worker: NonZeroUsize,
    ) -> Result<Self, InitError> {
        info!(
            count = worker_count,
            max_action_lifecycle = ?max_action_lifecycle,
            "spawning python worker pool"
        );

        // Spawn all workers in parallel to reduce boot time.
        let spawn_results: Vec<_> = {
            let workers_registry = &workers_registry;
            (0..worker_count.get())
                .map(|_| {
                    let reservation = workers_registry.reserve();
                    let params = worker_process_spec.prepare_spawn_params(reservation.id());
                    tokio::spawn(waymark_worker_process::spawn(reservation, params))
                })
                .collect()
        };

        let mut workers = Vec::with_capacity(worker_count.get());
        let mut worker_id_sequence = 0;
        for (worker_index, handle) in spawn_results.into_iter().enumerate() {
            let result = handle.await.unwrap(); // propagate panics
            match result {
                Ok((handle, sender)) => {
                    workers.push(Arc::new(WorkerState {
                        handle,
                        sender,
                        id: worker_id_sequence,
                    }));
                    worker_id_sequence += 1;
                }
                Err(error) => {
                    warn!(
                        worker_index,
                        ?error,
                        "failed to spawn worker, cleaning up {} already spawned",
                        workers.len()
                    );
                    for worker in workers {
                        if let Ok(worker) = Arc::try_unwrap(worker) {
                            let _ = worker.handle.shutdown().await;
                        }
                    }
                    return Err(InitError::WorkerSpawn {
                        error,
                        worker_index,
                    });
                }
            }
        }

        info!(count = workers.len(), "worker pool ready");

        let worker_id_sequence = AtomicU64::new(worker_id_sequence);
        let worker_ids = workers.iter().map(|worker| worker.id).collect();
        let action_counts = nevec_fn(worker_count, |_| AtomicU64::new(0));
        let in_flight_counts = nevec_fn(worker_count, |_| AtomicUsize::new(0));
        Ok(Self {
            worker_process_spec,
            workers_registry,
            worker_id_sequence,
            worker_processes: RwLock::new(workers),
            cursor: AtomicUsize::new(0),
            metrics: StdMutex::new(WorkerPoolMetrics::new(
                worker_ids,
                Duration::from_secs(THROUGHPUT_WINDOW_SECS),
                LATENCY_SAMPLE_SIZE,
            )),
            action_counts,
            in_flight_counts,
            max_concurrent_per_worker,
            max_action_lifecycle,
        })
    }
}

fn nevec_fn<T>(items: NonZeroUsize, mut f: impl FnMut(usize) -> T) -> NEVec<T> {
    let mut vec = NEVec::with_capacity(items, f(0));
    for index in 1..items.get() {
        vec.push(f(index));
    }
    vec
}

impl<Spec> Pool<Spec> {
    /// Get a worker by index.
    ///
    /// Returns a clone of the Arc for the worker at the given index.
    pub async fn get_worker(&self, idx: usize) -> Arc<WorkerState> {
        let worker_processes = self.worker_processes.read().await;
        Arc::clone(&worker_processes[idx % worker_processes.len()])
    }

    /// Get the next worker index using round-robin selection.
    ///
    /// This is lock-free and O(1). Returns the index that can be used
    /// with `get_worker` to fetch the actual worker.
    pub fn next_worker_idx(&self) -> usize {
        self.cursor.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the number of workers in the pool.
    pub fn len(&self) -> NonZeroUsize {
        self.action_counts.len()
    }

    /// Get the maximum concurrent actions per worker.
    pub fn max_concurrent_per_worker(&self) -> NonZeroUsize {
        self.max_concurrent_per_worker
    }

    /// Get total capacity (worker_count * max_concurrent_per_worker).
    pub fn total_capacity(&self) -> NonZeroUsize {
        self.len().saturating_mul(self.max_concurrent_per_worker)
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
        self.total_capacity()
            .get()
            .saturating_sub(self.total_in_flight())
    }

    /// Try to acquire a slot for the next available worker.
    ///
    /// Returns `Some(worker_idx)` if a slot was acquired, `None` if all workers
    /// are at capacity. Uses round-robin selection among workers with capacity.
    pub fn try_acquire_slot(&self) -> Option<usize> {
        let worker_count = self.len();

        // Try each worker starting from the current cursor position
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for i in 0..worker_count.get() {
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
            if current >= self.max_concurrent_per_worker.get() {
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
    pub async fn workers_snapshot(&self) -> Vec<Arc<WorkerState>> {
        self.worker_processes.read().await.clone()
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
}

impl<Spec> Pool<Spec>
where
    Spec: waymark_worker_process_spec::Spec,
{
    /// Record an action completion for a worker and trigger recycling if needed.
    ///
    /// This decrements the in-flight count and increments the action count for
    /// the worker at the given index. If `max_action_lifecycle` is set and the
    /// count reaches or exceeds the threshold, a background task is spawned to
    /// recycle the worker.
    pub fn record_completion(&self, worker_idx: usize, pool: Arc<Self>)
    where
        Spec: Send + Sync + 'static,
    {
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
                && new_count >= max_lifecycle.get()
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
    async fn recycle_worker(
        &self,
        worker_idx: usize,
    ) -> Result<(), waymark_reserved_process::SpawnError> {
        // Spawn the replacement worker first
        let reservation = self.workers_registry.reserve();
        let params = self
            .worker_process_spec
            .prepare_spawn_params(reservation.id());
        let (handle, sender) = waymark_worker_process::spawn(reservation, params).await?;
        let new_worker_id = self.worker_id_sequence.fetch_add(1, Ordering::Relaxed);
        let new_worker = WorkerState {
            handle,
            sender,
            id: new_worker_id,
        };

        // Replace the worker in the pool
        let old_worker = {
            let mut worker_processes = self.worker_processes.write().await;
            let idx = worker_idx % worker_processes.len();
            std::mem::replace(&mut worker_processes[idx], Arc::new(new_worker))
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
            old_worker_id = old_worker.id,
            new_worker_id,
            "recycled worker"
        );

        // The old worker will be cleaned up when its Arc drops
        // (once all in-flight actions complete)

        Ok(())
    }
}

impl<Spec> Pool<Spec> {
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
    pub(crate) fn max_lifecycle(&self) -> Option<NonZeroU64> {
        self.max_action_lifecycle
    }

    /// Gracefully shut down all workers in the pool.
    ///
    /// Workers are shut down in order. Any workers still in use
    /// (shared references exist) are skipped with a warning.
    pub async fn shutdown(self) -> Result<(), waymark_managed_process::ShutdownError> {
        let workers = self.worker_processes.into_inner();
        info!(count = workers.len(), "shutting down worker pool");

        for worker in workers {
            match Arc::try_unwrap(worker) {
                Ok(worker) => {
                    worker.handle.shutdown().await?;
                }
                Err(arc) => {
                    warn!(
                        worker_id = arc.id,
                        "worker still in use during shutdown; skipping"
                    );
                }
            }
        }

        info!("worker pool shutdown complete");
        Ok(())
    }

    /// Unwrap an [`Arc`] and gracefully shut down all workers in the pool.
    ///
    /// See [`Pool::shutdown`].
    pub async fn shutdown_arc(
        self: Arc<Self>,
    ) -> Result<(), waymark_managed_process::ShutdownError> {
        let Some(pool) = Arc::into_inner(self) else {
            warn!("worker pool still referenced during shutdown; skipping shutdown");
            return Ok(());
        };
        pool.shutdown().await
    }
}

impl<Spec> waymark_worker_status_core::WorkerPoolStats for Pool<Spec> {
    fn stats_snapshot(&self) -> waymark_worker_status_core::WorkerPoolStatsSnapshot {
        let snapshots = self.throughput_snapshots();
        let active_workers = snapshots.len() as u16;
        let throughput_per_min: f64 = snapshots.iter().map(|s| s.throughput_per_min).sum();
        let total_completed: i64 = snapshots.iter().map(|s| s.total_completed as i64).sum();
        let last_action_at = snapshots.iter().filter_map(|s| s.last_action_at).max();
        let (dispatch_queue_size, total_in_flight) = self.queue_stats();
        let (median_dequeue_ms, median_handling_ms) = self.median_latencies_ms();

        waymark_worker_status_core::WorkerPoolStatsSnapshot {
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
