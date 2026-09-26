use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use nonempty_collections::NEVec;
use tokio::sync::RwLock;

use tracing::{info, warn};

type Registry = waymark_worker_reservation::Registry<waymark_worker_message_protocol::Channels>;

/// A worker's generation: a fresh number per spawn into the pool.
type WorkerGeneration = u64;

pub struct WorkerState {
    pub handle: waymark_worker_process::Handle,
    pub sender: Arc<waymark_worker_message_protocol::Sender>,
    pub generation: WorkerGeneration,
}

/// Reported by [`Pool::record_completion`] on every completion at or past
/// the lifecycle limit, until the slot is recycled: the worker is due for
/// [`Pool::recycle_worker`], which acts on the first report for a worker
/// and ignores the rest.
#[derive(Debug)]
pub struct RecycleDue {
    /// The generation of the worker the completion was recorded against.
    pub generation: WorkerGeneration,
}

/// The claim a recycle holds on its slot, released when the recycle is
/// over, one way or another.
struct RecycleClaim<'a> {
    slot: &'a AtomicBool,
}

impl<'a> RecycleClaim<'a> {
    /// Claim `slot` for a recycle; `None` when a recycle of the slot is
    /// already in progress.
    fn try_acquire(slot: &'a AtomicBool) -> Option<Self> {
        let acquired = slot
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();

        acquired.then(|| Self { slot })
    }
}

impl Drop for RecycleClaim<'_> {
    fn drop(&mut self) {
        self.slot.store(false, Ordering::Release);
    }
}

pub struct Pool<Spec> {
    /// The spec for the worker processes.
    worker_process_spec: Spec,

    /// The registry of the connecting workers.
    workers_registry: Arc<Registry>,

    /// The next worker generation.
    generation_sequence: AtomicU64,

    /// The workers in the pool (RwLock for recycling support)
    worker_processes: RwLock<Vec<WorkerState>>,

    /// Cursor for round-robin selection
    cursor: AtomicUsize,

    /// Action counts per worker slot (for lifecycle tracking)
    action_counts: NEVec<AtomicU64>,

    /// The generation of the worker each slot holds, kept in step with
    /// `worker_processes` for the lock-free completion path.
    slot_generations: NEVec<AtomicU64>,

    /// Whether a recycle of the slot is in progress, per slot.
    recycle_claims: NEVec<AtomicBool>,

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
        error: waymark_worker_process::SpawnError,
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
        let mut generation_sequence = 0;
        for (worker_index, handle) in spawn_results.into_iter().enumerate() {
            let result = handle.await.unwrap(); // propagate panics
            match result {
                Ok((handle, sender)) => {
                    workers.push(WorkerState {
                        handle,
                        sender: Arc::new(sender),
                        generation: generation_sequence,
                    });
                    generation_sequence += 1;
                }
                Err(error) => {
                    warn!(
                        worker_index,
                        ?error,
                        "failed to spawn worker, cleaning up {} already spawned",
                        workers.len()
                    );
                    for worker in workers {
                        let _ = worker.handle.shutdown().await;
                    }
                    return Err(InitError::WorkerSpawn {
                        error,
                        worker_index,
                    });
                }
            }
        }

        info!(count = workers.len(), "worker pool ready");

        metrics::gauge!("waymark_worker_process_pool_workers").set(worker_count.get() as f64);
        metrics::gauge!("waymark_worker_process_pool_action_capacity").set(
            worker_count
                .get()
                .saturating_mul(max_concurrent_per_worker.get()) as f64,
        );

        let generation_sequence = AtomicU64::new(generation_sequence);
        let action_counts = nevec_fn(worker_count, |_| AtomicU64::new(0));
        let slot_generations = nevec_fn(worker_count, |index| {
            AtomicU64::new(workers[index].generation)
        });
        let recycle_claims = nevec_fn(worker_count, |_| AtomicBool::new(false));
        let in_flight_counts = nevec_fn(worker_count, |_| AtomicUsize::new(0));
        Ok(Self {
            worker_process_spec,
            workers_registry,
            generation_sequence,
            worker_processes: RwLock::new(workers),
            cursor: AtomicUsize::new(0),
            action_counts,
            slot_generations,
            recycle_claims,
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
    /// Get a worker sender by index.
    ///
    /// Returns a clone of the [`Arc`] for the sender at the given index.
    pub async fn get_worker_sender(
        &self,
        idx: usize,
    ) -> Arc<waymark_worker_message_protocol::Sender> {
        let worker_processes = self.worker_processes.read().await;
        Arc::clone(&worker_processes[idx % worker_processes.len()].sender)
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
                Ok(_) => {
                    metrics::counter!("waymark_worker_process_pool_actions_acquired_total")
                        .increment(1);
                    return true;
                }
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
            metrics::counter!("waymark_worker_process_pool_actions_released_total").increment(1);
        }
    }

    /// Get in-flight count for a specific worker.
    pub fn in_flight_for_worker(&self, worker_idx: usize) -> usize {
        self.in_flight_counts
            .get(worker_idx % self.len())
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

impl<Spec> Pool<Spec>
where
    Spec: waymark_worker_process_spec::Spec,
{
    /// Record an action completion for a worker.
    ///
    /// Decrements the in-flight count and increments the action count for
    /// the worker at the given index. When `max_action_lifecycle` is set and
    /// the count is at or past it, reports that a recycle is due for the
    /// worker the slot holds. Every completion past the limit reports it
    /// again until [`recycle_worker`](Self::recycle_worker) resets the
    /// count; that is where the reports are told apart.
    pub fn record_completion(&self, worker_idx: usize) -> Option<RecycleDue> {
        // Release the in-flight slot
        self.release_slot(worker_idx);
        metrics::counter!("waymark_worker_process_pool_actions_completed_total").increment(1);
        let unix_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        metrics::gauge!("waymark_worker_process_pool_last_action_completed_timestamp_seconds")
            .set(unix_time.as_secs_f64());

        // The generation is read before the count: a recycle resets the
        // count before it publishes the replacement's generation, so a
        // count past the limit is never reported against the replacement.
        let generation = self
            .slot_generations
            .get(worker_idx)?
            .load(Ordering::SeqCst);

        // Increment action count
        let counter = self.action_counts.get(worker_idx)?;
        let new_count = counter.fetch_add(1, Ordering::SeqCst) + 1;

        // Check if recycling is needed
        let max_lifecycle = self.max_action_lifecycle?;
        if new_count < max_lifecycle.get() {
            return None;
        }

        info!(
            worker_idx,
            generation,
            action_count = new_count,
            max_lifecycle,
            "worker reached action lifecycle limit, recycle due"
        );

        Some(RecycleDue { generation })
    }

    /// Recycle the worker at the given index, on the report `due`.
    ///
    /// Acts once per worker: the report is ignored when the slot no longer
    /// holds the worker it was reported for, or while a recycle of that
    /// worker is in progress. Otherwise a replacement is spawned and
    /// swapped in and the slot's action count is reset. The old worker
    /// will be shut down once all in-flight actions complete (when its
    /// Arc reference count drops to zero). A failed spawn releases the
    /// slot, so the next report retries.
    pub async fn recycle_worker(
        &self,
        worker_idx: usize,
        due: RecycleDue,
    ) -> Result<(), waymark_worker_process::SpawnError> {
        let slot = worker_idx % self.len();
        let RecycleDue { generation } = due;

        // Claim the slot for this worker's recycle, against the worker the
        // slot holds right now; the read lock keeps a swap from happening
        // in between.
        let claim = {
            let worker_processes = self.worker_processes.read().await;
            if worker_processes[slot].generation != generation {
                tracing::debug!(
                    worker_idx,
                    generation,
                    "recycle reported for a worker already replaced; ignored"
                );
                return Ok(());
            }

            let claim_slot = self
                .recycle_claims
                .get(slot)
                .expect("the slot index is within the pool");
            let Some(claim) = RecycleClaim::try_acquire(claim_slot) else {
                tracing::debug!(
                    worker_idx,
                    generation,
                    "recycle reported while one is in progress; ignored"
                );
                return Ok(());
            };

            claim
        };

        // Spawn the replacement worker first
        let reservation = self.workers_registry.reserve();
        let params = self
            .worker_process_spec
            .prepare_spawn_params(reservation.id());
        let (handle, sender) = waymark_worker_process::spawn(reservation, params).await?;
        let new_generation = self.generation_sequence.fetch_add(1, Ordering::Relaxed);
        let new_worker = WorkerState {
            handle,
            sender: Arc::new(sender),
            generation: new_generation,
        };

        // Replace the worker in the pool, reset the slot's action count and
        // only then publish the replacement's generation; see
        // `record_completion`.
        let old_worker = {
            let mut worker_processes = self.worker_processes.write().await;
            let old_worker = std::mem::replace(&mut worker_processes[slot], new_worker);

            if let Some(counter) = self.action_counts.get(slot) {
                counter.store(0, Ordering::SeqCst);
            }
            if let Some(slot_generation) = self.slot_generations.get(slot) {
                slot_generation.store(new_generation, Ordering::SeqCst);
            }

            old_worker
        };

        drop(claim);

        info!(
            worker_idx,
            old_generation = old_worker.generation,
            new_generation,
            "recycled worker"
        );

        // The old worker will be cleaned up when its Arc drops
        // (once all in-flight actions complete)

        Ok(())
    }
}

impl<Spec> Pool<Spec> {
    /// Gracefully shut down all workers in the pool.
    ///
    /// Workers are shut down in order.
    pub async fn shutdown(self) -> Result<(), waymark_managed_process::ShutdownError> {
        let workers = self.worker_processes.into_inner();
        info!(count = workers.len(), "shutting down worker pool");

        for worker in workers {
            worker.handle.shutdown().await?;
        }

        info!("worker pool shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[derive(Debug, Default)]
    struct DummySpec;

    impl waymark_worker_process_spec::Spec for DummySpec {
        fn prepare_spawn_params(
            &self,
            _reservation_id: waymark_worker_reservation::Id,
        ) -> waymark_worker_process::SpawnParams {
            waymark_worker_process::SpawnParams {
                command: tokio::process::Command::new("false"),
                wait_for_playload_timeout: Duration::from_millis(1),
                shutdown_params: waymark_worker_process::ShutdownParams {
                    tasks_graceful_shutdown_timeout: Duration::from_millis(1),
                    process_graceful_shutdown_timeout: Duration::from_millis(1),
                    process_kill_timeout: Duration::from_millis(1),
                },
            }
        }
    }

    fn make_pool(
        worker_count: usize,
        max_concurrent_per_worker: usize,
        max_action_lifecycle: Option<u64>,
    ) -> Pool<DummySpec> {
        let worker_count = NonZeroUsize::new(worker_count).expect("worker count must be non-zero");
        let max_concurrent_per_worker = NonZeroUsize::new(max_concurrent_per_worker)
            .expect("max concurrent per worker must be non-zero");
        let max_action_lifecycle = max_action_lifecycle
            .map(|limit| NonZeroU64::new(limit).expect("max action lifecycle must be non-zero"));

        Pool {
            worker_process_spec: DummySpec,
            workers_registry: Arc::new(Registry::default()),
            generation_sequence: AtomicU64::new(worker_count.get() as u64),
            worker_processes: RwLock::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            action_counts: nevec_fn(worker_count, |_| AtomicU64::new(0)),
            slot_generations: nevec_fn(worker_count, |index| {
                AtomicU64::new(u64::try_from(index).expect("the slot index fits a generation"))
            }),
            recycle_claims: nevec_fn(worker_count, |_| AtomicBool::new(false)),
            in_flight_counts: nevec_fn(worker_count, |_| AtomicUsize::new(0)),
            max_concurrent_per_worker,
            max_action_lifecycle,
        }
    }

    #[test]
    fn nevec_fn_populates_each_index() {
        let values = nevec_fn(NonZeroUsize::new(4).expect("non-zero"), |index| index * 2);

        assert_eq!(values.iter().copied().collect::<Vec<_>>(), vec![0, 2, 4, 6]);
    }

    #[test]
    fn slot_acquire_release_and_completion_count_on_the_recorder() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let pool = make_pool(1, 1, None);

            assert!(pool.try_acquire_slot_for_worker(0));
            assert!(!pool.try_acquire_slot_for_worker(0), "at capacity");
            let recycle_due = pool.record_completion(0);
            assert!(recycle_due.is_none(), "no lifecycle limit");
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let counter = |name: &str| -> u64 {
            snapshot
                .iter()
                .find_map(|(key, _, _, value)| match value {
                    metrics_util::debugging::DebugValue::Counter(value)
                        if key.key().name() == name =>
                    {
                        Some(*value)
                    }
                    _ => None,
                })
                .unwrap_or_else(|| panic!("counter {name} not recorded"))
        };

        assert_eq!(
            counter("waymark_worker_process_pool_actions_acquired_total"),
            1
        );
        assert_eq!(
            counter("waymark_worker_process_pool_actions_released_total"),
            1
        );
        assert_eq!(
            counter("waymark_worker_process_pool_actions_completed_total"),
            1
        );
    }

    #[test]
    fn record_completion_increments_internal_action_count() {
        let pool = make_pool(2, 2, None);

        assert!(pool.try_acquire_slot_for_worker(1));
        let recycle_due = pool.record_completion(1);
        assert!(recycle_due.is_none(), "no lifecycle limit");

        let get_action_count = |worker_idx: usize| -> u64 {
            pool.action_counts
                .get(worker_idx)
                .map(|c| c.load(Ordering::SeqCst))
                .unwrap_or(0)
        };

        assert_eq!(get_action_count(0), 0);
        assert_eq!(get_action_count(1), 1);
    }

    #[test]
    fn record_completion_reports_due_on_every_completion_at_or_past_the_limit() {
        let pool = make_pool(1, 2, Some(2));

        assert!(pool.try_acquire_slot_for_worker(0));
        assert!(pool.try_acquire_slot_for_worker(0));
        assert!(pool.record_completion(0).is_none(), "below the limit");

        let due = pool.record_completion(0).expect("at the limit");
        assert_eq!(due.generation, 0);

        assert!(pool.try_acquire_slot_for_worker(0));
        let due = pool
            .record_completion(0)
            .expect("past the limit, reported again");
        assert_eq!(due.generation, 0);
    }
}
