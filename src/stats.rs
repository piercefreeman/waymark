//! In-memory lifecycle statistics tracking for diagnosing performance issues.
//!
//! Tracks timing metrics across the dequeue and completion handler lifecycle
//! with rolling window statistics (configurable, default 60 seconds).

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A single timing sample with timestamp.
#[derive(Debug, Clone, Copy)]
struct Sample {
    /// When this sample was recorded.
    timestamp: Instant,
    /// The duration in microseconds.
    value_us: u64,
}

/// A metric that tracks samples within a rolling time window.
#[derive(Debug)]
struct RollingMetric {
    samples: VecDeque<Sample>,
    window: Duration,
}

impl RollingMetric {
    fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::with_capacity(10000),
            window,
        }
    }

    fn record(&mut self, value_us: u64) {
        let now = Instant::now();
        self.samples.push_back(Sample {
            timestamp: now,
            value_us,
        });
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        let cutoff = now - self.window;
        while let Some(front) = self.samples.front() {
            if front.timestamp < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn stats(&mut self) -> MetricStats {
        let now = Instant::now();
        self.prune(now);

        if self.samples.is_empty() {
            return MetricStats::default();
        }

        let mut values: Vec<u64> = self.samples.iter().map(|s| s.value_us).collect();
        values.sort_unstable();

        let count = values.len();
        let sum: u64 = values.iter().sum();
        let mean = sum / count as u64;
        let min = values[0];
        let max = values[count - 1];
        let p50 = values[count / 2];
        let p95 = values[(count as f64 * 0.95) as usize];
        let p99 = values[(count as f64 * 0.99) as usize];

        MetricStats {
            count: count as u64,
            mean_us: mean,
            min_us: min,
            max_us: max,
            p50_us: p50,
            p95_us: p95,
            p99_us: p99,
        }
    }
}

/// Statistics for a single metric.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricStats {
    pub count: u64,
    pub mean_us: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
}

impl MetricStats {
    /// Format as a concise string for logging.
    pub fn format(&self) -> String {
        if self.count == 0 {
            return "n=0".to_string();
        }
        format!(
            "n={} mean={:.1}ms p50={:.1}ms p95={:.1}ms p99={:.1}ms min={:.1}ms max={:.1}ms",
            self.count,
            self.mean_us as f64 / 1000.0,
            self.p50_us as f64 / 1000.0,
            self.p95_us as f64 / 1000.0,
            self.p99_us as f64 / 1000.0,
            self.min_us as f64 / 1000.0,
            self.max_us as f64 / 1000.0,
        )
    }
}

/// All lifecycle statistics snapshot.
#[derive(Debug, Clone, Default)]
pub struct LifecycleStatsSnapshot {
    // Dequeue phase
    pub db_fetch: MetricStats,
    pub dispatch_to_worker: MetricStats,

    // Worker phase
    pub worker_roundtrip: MetricStats,
    pub worker_execution: MetricStats,

    // Completion phase
    pub channel_wait: MetricStats,
    pub batcher_wait: MetricStats,
    pub completion_db_write: MetricStats,
    pub inline_execution: MetricStats,
    pub subgraph_analysis: MetricStats,

    // Totals
    pub total_dequeue: MetricStats,
    pub total_completion: MetricStats,
}

impl LifecycleStatsSnapshot {
    /// Format all stats as a multi-line string for logging.
    pub fn format(&self) -> String {
        format!(
            "\n  dequeue:     db_fetch={} dispatch={}\n  worker:      roundtrip={} execution={}\n  completion:  channel={} batcher={} db_write={} inline={} subgraph={}\n  totals:      dequeue={} completion={}",
            self.db_fetch.format(),
            self.dispatch_to_worker.format(),
            self.worker_roundtrip.format(),
            self.worker_execution.format(),
            self.channel_wait.format(),
            self.batcher_wait.format(),
            self.completion_db_write.format(),
            self.inline_execution.format(),
            self.subgraph_analysis.format(),
            self.total_dequeue.format(),
            self.total_completion.format(),
        )
    }
}

/// Thread-safe lifecycle statistics collector.
///
/// Tracks timing metrics across the dequeue and completion handler lifecycle
/// with rolling window statistics.
pub struct LifecycleStats {
    #[allow(dead_code)]
    window: Duration,

    // Dequeue phase
    db_fetch: Mutex<RollingMetric>,
    dispatch_to_worker: Mutex<RollingMetric>,

    // Worker phase
    worker_roundtrip: Mutex<RollingMetric>,
    worker_execution: Mutex<RollingMetric>,

    // Completion phase
    channel_wait: Mutex<RollingMetric>,
    batcher_wait: Mutex<RollingMetric>,
    completion_db_write: Mutex<RollingMetric>,
    inline_execution: Mutex<RollingMetric>,
    subgraph_analysis: Mutex<RollingMetric>,

    // Totals
    total_dequeue: Mutex<RollingMetric>,
    total_completion: Mutex<RollingMetric>,

    // Counters for events
    completions_processed: AtomicU64,
    completions_stale: AtomicU64,
    actions_dispatched: AtomicU64,
}

impl LifecycleStats {
    /// Create a new stats collector with the given rolling window duration.
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            db_fetch: Mutex::new(RollingMetric::new(window)),
            dispatch_to_worker: Mutex::new(RollingMetric::new(window)),
            worker_roundtrip: Mutex::new(RollingMetric::new(window)),
            worker_execution: Mutex::new(RollingMetric::new(window)),
            channel_wait: Mutex::new(RollingMetric::new(window)),
            batcher_wait: Mutex::new(RollingMetric::new(window)),
            completion_db_write: Mutex::new(RollingMetric::new(window)),
            inline_execution: Mutex::new(RollingMetric::new(window)),
            subgraph_analysis: Mutex::new(RollingMetric::new(window)),
            total_dequeue: Mutex::new(RollingMetric::new(window)),
            total_completion: Mutex::new(RollingMetric::new(window)),
            completions_processed: AtomicU64::new(0),
            completions_stale: AtomicU64::new(0),
            actions_dispatched: AtomicU64::new(0),
        }
    }

    /// Create with default 60-second window.
    pub fn default_window() -> Self {
        Self::new(Duration::from_secs(60))
    }

    // Dequeue phase recording

    pub fn record_db_fetch(&self, duration: Duration) {
        if let Ok(mut metric) = self.db_fetch.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    pub fn record_dispatch_to_worker(&self, duration: Duration) {
        if let Ok(mut metric) = self.dispatch_to_worker.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    pub fn record_total_dequeue(&self, duration: Duration) {
        if let Ok(mut metric) = self.total_dequeue.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    // Worker phase recording

    pub fn record_worker_roundtrip(&self, duration: Duration) {
        if let Ok(mut metric) = self.worker_roundtrip.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    pub fn record_worker_execution(&self, duration: Duration) {
        if let Ok(mut metric) = self.worker_execution.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    // Completion phase recording

    pub fn record_channel_wait(&self, duration: Duration) {
        if let Ok(mut metric) = self.channel_wait.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    pub fn record_batcher_wait(&self, duration: Duration) {
        if let Ok(mut metric) = self.batcher_wait.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    pub fn record_completion_db_write(&self, duration: Duration) {
        if let Ok(mut metric) = self.completion_db_write.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    pub fn record_inline_execution(&self, duration: Duration) {
        if let Ok(mut metric) = self.inline_execution.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    pub fn record_subgraph_analysis(&self, duration: Duration) {
        if let Ok(mut metric) = self.subgraph_analysis.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    pub fn record_total_completion(&self, duration: Duration) {
        if let Ok(mut metric) = self.total_completion.lock() {
            metric.record(duration.as_micros() as u64);
        }
    }

    // Event counters

    pub fn increment_completions_processed(&self) {
        self.completions_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_completions_stale(&self) {
        self.completions_stale.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_actions_dispatched(&self, count: u64) {
        self.actions_dispatched.fetch_add(count, Ordering::Relaxed);
    }

    /// Get a snapshot of all current statistics.
    pub fn snapshot(&self) -> LifecycleStatsSnapshot {
        LifecycleStatsSnapshot {
            db_fetch: self
                .db_fetch
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            dispatch_to_worker: self
                .dispatch_to_worker
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            worker_roundtrip: self
                .worker_roundtrip
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            worker_execution: self
                .worker_execution
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            channel_wait: self
                .channel_wait
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            batcher_wait: self
                .batcher_wait
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            completion_db_write: self
                .completion_db_write
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            inline_execution: self
                .inline_execution
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            subgraph_analysis: self
                .subgraph_analysis
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            total_dequeue: self
                .total_dequeue
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
            total_completion: self
                .total_completion
                .lock()
                .map(|mut m| m.stats())
                .unwrap_or_default(),
        }
    }

    /// Get and reset event counters, returning (completions_processed, completions_stale, actions_dispatched).
    pub fn take_counters(&self) -> (u64, u64, u64) {
        (
            self.completions_processed.swap(0, Ordering::Relaxed),
            self.completions_stale.swap(0, Ordering::Relaxed),
            self.actions_dispatched.swap(0, Ordering::Relaxed),
        )
    }

    /// Log current statistics and reset counters.
    pub fn log_and_reset_counters(&self) {
        let snapshot = self.snapshot();
        let (processed, stale, dispatched) = self.take_counters();

        tracing::info!(
            "lifecycle_stats: processed={} stale={} dispatched={}{}",
            processed,
            stale,
            dispatched,
            snapshot.format()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rolling_metric_basic() {
        let mut metric = RollingMetric::new(Duration::from_secs(60));
        metric.record(1000);
        metric.record(2000);
        metric.record(3000);

        let stats = metric.stats();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.mean_us, 2000);
        assert_eq!(stats.min_us, 1000);
        assert_eq!(stats.max_us, 3000);
    }

    #[test]
    fn test_lifecycle_stats_snapshot() {
        let stats = LifecycleStats::default_window();
        stats.record_db_fetch(Duration::from_millis(10));
        stats.record_db_fetch(Duration::from_millis(20));
        stats.record_worker_roundtrip(Duration::from_millis(100));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.db_fetch.count, 2);
        assert_eq!(snapshot.worker_roundtrip.count, 1);
    }

    #[test]
    fn test_counters() {
        let stats = LifecycleStats::default_window();
        stats.increment_completions_processed();
        stats.increment_completions_processed();
        stats.increment_actions_dispatched(5);

        let (processed, stale, dispatched) = stats.take_counters();
        assert_eq!(processed, 2);
        assert_eq!(stale, 0);
        assert_eq!(dispatched, 5);

        // Should be reset
        let (processed, _, _) = stats.take_counters();
        assert_eq!(processed, 0);
    }
}
