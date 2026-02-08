//! Worker pool interface for executing actions.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use serde_json::Value;
use uuid::Uuid;

/// Action execution request routed through the worker pool.
#[derive(Clone, Debug)]
pub struct ActionRequest {
    pub executor_id: Uuid,
    pub execution_id: Uuid,
    pub action_name: String,
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, Value>,
    pub timeout_seconds: u32,
    pub attempt_number: u32,
    pub dispatch_token: Uuid,
}

/// Completed action result emitted by the worker pool.
#[derive(Clone, Debug)]
pub struct ActionCompletion {
    pub executor_id: Uuid,
    pub execution_id: Uuid,
    pub attempt_number: u32,
    pub dispatch_token: Uuid,
    pub result: Value,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct WorkerPoolError {
    pub kind: String,
    pub message: String,
}

impl WorkerPoolError {
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }
}

/// Abstract worker pool with queue and batch completion polling.
pub trait BaseWorkerPool: Send + Sync {
    /// Start any background tasks required by the pool.
    ///
    /// Default implementation is a no-op for pools that don't need launch work.
    fn launch<'a>(&'a self) -> BoxFuture<'a, Result<(), WorkerPoolError>> {
        Box::pin(async { Ok(()) })
    }

    /// Submit an action request for execution.
    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError>;

    /// Await and return a batch of completed actions.
    fn get_complete<'a>(&'a self) -> BoxFuture<'a, Vec<ActionCompletion>>;
}

pub fn error_to_value(error: &WorkerPoolError) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("type".to_string(), Value::String(error.kind.clone()));
    map.insert("message".to_string(), Value::String(error.message.clone()));
    Value::Object(map)
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

/// Throughput snapshot for a single worker.
#[derive(Debug, Clone)]
pub struct WorkerThroughputSnapshot {
    /// Worker ID
    pub worker_id: u64,
    /// Throughput in actions per minute
    pub throughput_per_min: f64,
    /// Total actions completed since pool start
    pub total_completed: u64,
    /// Timestamp of last completed action
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

    fn reset_worker(&mut self, worker_idx: usize, worker_id: u64) {
        if self.workers.is_empty() {
            return;
        }
        let idx = worker_idx % self.workers.len();
        self.workers[idx] = WorkerThroughputState::new(worker_id);
    }
}

#[derive(Debug)]
struct LatencyTracker {
    max_samples: usize,
    dequeue_ms: VecDeque<i64>,
    handling_ms: VecDeque<i64>,
}

impl LatencyTracker {
    fn new(max_samples: usize) -> Self {
        Self {
            max_samples: max_samples.max(1),
            dequeue_ms: VecDeque::new(),
            handling_ms: VecDeque::new(),
        }
    }

    fn record(&mut self, ack_latency: Duration, worker_duration: Duration) {
        let dequeue = ack_latency.as_millis() as i64;
        let handling = worker_duration.as_millis() as i64;
        self.dequeue_ms.push_back(dequeue);
        self.handling_ms.push_back(handling);

        while self.dequeue_ms.len() > self.max_samples {
            self.dequeue_ms.pop_front();
        }
        while self.handling_ms.len() > self.max_samples {
            self.handling_ms.pop_front();
        }
    }

    fn median(values: &VecDeque<i64>) -> Option<i64> {
        if values.is_empty() {
            return None;
        }
        let mut ordered: Vec<i64> = values.iter().copied().collect();
        ordered.sort_unstable();
        Some(ordered[ordered.len() / 2])
    }

    fn snapshot_medians(&self) -> (Option<i64>, Option<i64>) {
        (
            Self::median(&self.dequeue_ms),
            Self::median(&self.handling_ms),
        )
    }
}

#[derive(Debug)]
pub(crate) struct WorkerPoolMetrics {
    throughput: WorkerThroughputTracker,
    latency: LatencyTracker,
}

impl WorkerPoolMetrics {
    pub(crate) fn new(
        worker_ids: Vec<u64>,
        throughput_window: Duration,
        latency_samples: usize,
    ) -> Self {
        Self {
            throughput: WorkerThroughputTracker::new(worker_ids, throughput_window),
            latency: LatencyTracker::new(latency_samples),
        }
    }

    pub(crate) fn record_completion(&mut self, worker_idx: usize) {
        self.throughput.record_completion(worker_idx);
    }

    #[cfg(test)]
    pub(crate) fn record_completion_at(
        &mut self,
        worker_idx: usize,
        when: Instant,
        wall_time: DateTime<Utc>,
    ) {
        self.throughput
            .record_completion_at(worker_idx, when, wall_time);
    }

    pub(crate) fn reset_worker(&mut self, worker_idx: usize, worker_id: u64) {
        self.throughput.reset_worker(worker_idx, worker_id);
    }

    pub(crate) fn throughput_snapshots(&mut self, now: Instant) -> Vec<WorkerThroughputSnapshot> {
        self.throughput.snapshot_at(now)
    }

    pub(crate) fn record_latency(&mut self, ack_latency: Duration, worker_duration: Duration) {
        self.latency.record(ack_latency, worker_duration);
    }

    pub(crate) fn median_latencies_ms(&self) -> (Option<i64>, Option<i64>) {
        self.latency.snapshot_medians()
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerPoolMetrics;
    use chrono::Utc;
    use std::time::{Duration, Instant};

    #[test]
    fn test_worker_throughput_tracker_window() {
        let base_wall = Utc::now();
        let mut metrics = WorkerPoolMetrics::new(vec![0, 1], Duration::from_secs(60), 16);
        let base = Instant::now();

        metrics.record_completion_at(0, base, base_wall);
        metrics.record_completion_at(0, base + Duration::from_secs(10), base_wall);
        metrics.record_completion_at(0, base + Duration::from_secs(50), base_wall);
        metrics.record_completion_at(0, base + Duration::from_secs(70), base_wall);

        let snapshots = metrics.throughput_snapshots(base + Duration::from_secs(70));
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
}
