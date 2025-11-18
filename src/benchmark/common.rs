use std::time::Duration;

use crate::db::{CompletionRecord, Database};
use tokio::{sync::mpsc, task::JoinHandle, time};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub sequence: u32,
    pub ack_latency: Duration,
    pub round_trip: Duration,
    pub worker_duration: Duration,
}

impl From<crate::worker::RoundTripMetrics> for BenchmarkResult {
    fn from(value: crate::worker::RoundTripMetrics) -> Self {
        Self {
            sequence: value.sequence,
            ack_latency: value.ack_latency,
            round_trip: value.round_trip,
            worker_duration: value.worker_duration,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkSummary {
    pub total_messages: usize,
    pub elapsed: Duration,
    pub throughput_per_sec: f64,
    pub avg_ack_ms: f64,
    pub avg_round_trip_ms: f64,
    pub worker_avg_ms: f64,
    pub p95_round_trip_ms: f64,
}

impl BenchmarkSummary {
    pub fn from_results(results: Vec<BenchmarkResult>, elapsed: Duration) -> Self {
        let total_messages = results.len();
        let throughput_per_sec = total_messages as f64 / elapsed.as_secs_f64().max(1e-9);
        let denom = total_messages as f64;
        let ack_sum: f64 = results.iter().map(|r| r.ack_latency.as_secs_f64()).sum();
        let round_sum: f64 = results.iter().map(|r| r.round_trip.as_secs_f64()).sum();
        let worker_sum: f64 = results
            .iter()
            .map(|r| r.worker_duration.as_secs_f64())
            .sum();
        let mut round_sorted: Vec<_> = results.iter().map(|r| r.round_trip).collect();
        round_sorted.sort_unstable();
        let p95_index = if round_sorted.is_empty() {
            0
        } else {
            let idx = ((round_sorted.len() - 1) as f64 * 0.95).round() as usize;
            idx.min(round_sorted.len() - 1)
        };
        let p95_round_trip_ms = round_sorted
            .get(p95_index)
            .copied()
            .unwrap_or_default()
            .as_secs_f64()
            * 1000.0;

        Self {
            total_messages,
            elapsed,
            throughput_per_sec,
            avg_ack_ms: (ack_sum / denom) * 1000.0,
            avg_round_trip_ms: (round_sum / denom) * 1000.0,
            worker_avg_ms: (worker_sum / denom) * 1000.0,
            p95_round_trip_ms,
        }
    }

    pub fn log(&self) {
        info!(
            total = self.total_messages,
            elapsed = %format!("{:.2}s", self.elapsed.as_secs_f64()),
            throughput = %format!("{:.0} msg/s", self.throughput_per_sec),
            avg_ack_ms = %format!("{:.3} ms", self.avg_ack_ms),
            avg_round_trip_ms = %format!("{:.3} ms", self.avg_round_trip_ms),
            worker_avg_ms = %format!("{:.3} ms", self.worker_avg_ms),
            p95_round_trip_ms = %format!("{:.3} ms", self.p95_round_trip_ms),
            "benchmark summary",
        );
    }
}

pub fn spawn_completion_worker(
    database: Database,
) -> (mpsc::Sender<CompletionRecord>, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel(1024);
    let handle = tokio::spawn(async move {
        const BATCH_SIZE: usize = 128;
        let mut buffer = Vec::with_capacity(BATCH_SIZE);
        let mut ticker = time::interval(Duration::from_millis(5));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(record) => {
                            buffer.push(record);
                            if buffer.len() >= BATCH_SIZE {
                                if let Err(err) = database.mark_actions_batch(&buffer).await {
                                    warn!(?err, "failed to flush completion batch");
                                }
                                buffer.clear();
                            }
                        }
                        None => break,
                    }
                }
                _ = ticker.tick() => {
                    if buffer.is_empty() {
                        if rx.is_closed() {
                            break;
                        }
                        continue;
                    }
                    if let Err(err) = database.mark_actions_batch(&buffer).await {
                        warn!(?err, "failed to flush completion batch");
                    }
                    buffer.clear();
                }
            }
        }
        if !buffer.is_empty()
            && let Err(err) = database.mark_actions_batch(&buffer).await
        {
            warn!(?err, "failed to flush completion batch");
        }
    });
    (tx, handle)
}
