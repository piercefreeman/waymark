use std::time::{Duration, Instant};

use anyhow::Result;
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tracing::info;

use crate::{
    messages::MessageError,
    python_worker::{PythonWorker, PythonWorkerConfig, RoundTripMetrics},
};

#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub total_messages: usize,
    pub in_flight: usize,
    pub payload_size: usize,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            total_messages: 10_000,
            in_flight: 32,
            payload_size: 4096,
        }
    }
}

pub struct BenchmarkHarness {
    worker: PythonWorker,
}

impl BenchmarkHarness {
    pub async fn new(config: PythonWorkerConfig) -> Result<Self> {
        let worker = PythonWorker::spawn(config).await?;
        Ok(Self { worker })
    }

    pub async fn run(&self, config: &HarnessConfig) -> Result<BenchmarkSummary> {
        let total = config.total_messages;
        let mut completed = Vec::with_capacity(total);
        let mut inflight: FuturesUnordered<BoxFuture<'_, Result<RoundTripMetrics, MessageError>>> =
            FuturesUnordered::new();
        let mut issued = 0usize;
        let start = Instant::now();
        let worker = &self.worker;
        let max_inflight = config.in_flight.max(1);

        while issued < total || !inflight.is_empty() {
            while inflight.len() < max_inflight && issued < total {
                let sequence = issued as u32;
                let payload = config.payload_size;
                let fut: BoxFuture<'_, Result<RoundTripMetrics, MessageError>> =
                    Box::pin(async move { worker.send_benchmark_command(sequence, payload).await });
                inflight.push(fut);
                issued += 1;
            }

            if let Some(result) = inflight.next().await {
                let metrics = result?;
                completed.push(BenchmarkResult::from(metrics));
            }
        }

        let elapsed = start.elapsed();
        let summary = BenchmarkSummary::from_results(completed, elapsed);
        Ok(summary)
    }

    pub async fn shutdown(self) -> Result<()> {
        self.worker.shutdown().await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub sequence: u32,
    pub ack_latency: Duration,
    pub round_trip: Duration,
    pub worker_duration: Duration,
}

impl From<RoundTripMetrics> for BenchmarkResult {
    fn from(value: RoundTripMetrics) -> Self {
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
    fn from_results(results: Vec<BenchmarkResult>, elapsed: Duration) -> Self {
        let total_messages = results.len();
        let throughput_per_sec = total_messages as f64 / elapsed.as_secs_f64().max(1e-9);

        let mut ack_sum = 0f64;
        let mut round_sum = 0f64;
        let mut worker_sum = 0f64;
        let mut round_sorted = Vec::with_capacity(total_messages);

        for result in &results {
            ack_sum += result.ack_latency.as_secs_f64();
            round_sum += result.round_trip.as_secs_f64();
            worker_sum += result.worker_duration.as_secs_f64();
            round_sorted.push(result.round_trip);
        }

        round_sorted.sort();
        let denom = total_messages.max(1) as f64;
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
