use std::time::{Duration, Instant};

use anyhow::Result;
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use serde_json::json;
use tokio::{sync::mpsc, task::JoinHandle, time};
use tracing::{Instrument, info, warn};

use crate::{
    db::{CompletionRecord, Database},
    messages::MessageError,
    python_worker::{
        ActionDispatchPayload, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics,
    },
};

#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub total_messages: usize,
    pub in_flight: usize,
    pub payload_size: usize,
    pub partition_id: i32,
    pub progress_interval: Option<Duration>,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            total_messages: 10_000,
            in_flight: 32,
            payload_size: 4096,
            partition_id: 0,
            progress_interval: None,
        }
    }
}

pub struct BenchmarkHarness {
    workers: PythonWorkerPool,
    database: Database,
    completion_tx: mpsc::Sender<CompletionRecord>,
    completion_handle: JoinHandle<()>,
}

const BENCHMARK_USER_MODULE: &str = "carabiner_worker.fixtures.benchmark_actions";
const BENCHMARK_ACTION: &str = "benchmark.echo_payload";
const BENCHMARK_REQUEST_MODEL: &str = "PayloadRequest";

impl BenchmarkHarness {
    pub async fn new(
        config: PythonWorkerConfig,
        worker_count: usize,
        database: Database,
    ) -> Result<Self> {
        let workers = PythonWorkerPool::new(config, worker_count).await?;
        let (tx, rx) = mpsc::channel(1024);
        let db_clone = database.clone();
        let handle = tokio::spawn(async move {
            run_completion_worker(db_clone, rx).await;
        });
        Ok(Self {
            workers,
            database,
            completion_tx: tx,
            completion_handle: handle,
        })
    }

    pub async fn run(&self, config: &HarnessConfig) -> Result<BenchmarkSummary> {
        self.database.reset_partition(config.partition_id).await?;
        let encoded_payload = build_benchmark_payload(config.payload_size);
        self.database
            .seed_actions(
                config.partition_id,
                config.total_messages,
                &encoded_payload,
            )
            .await?;

        let total = config.total_messages;
        let mut completed = Vec::with_capacity(total);
        let mut inflight: FuturesUnordered<BoxFuture<'_, Result<RoundTripMetrics, MessageError>>> =
            FuturesUnordered::new();
        let mut dispatched = 0usize;
        let start = Instant::now();
        let worker_count = self.workers.len().max(1);
        let max_inflight = config.in_flight.max(1) * worker_count;
        let mut last_report = start;

        while completed.len() < total {
            while inflight.len() < max_inflight && dispatched < total {
                let needed = (max_inflight - inflight.len()).min(total - dispatched);
                let actions = self
                    .database
                    .dispatch_actions(config.partition_id, needed as i64)
                    .await?;
                if actions.is_empty() {
                    break;
                }

                for action in actions {
                    let payload = ActionDispatchPayload {
                        action_id: action.id,
                        instance_id: action.instance_id,
                        sequence: action.action_seq,
                        payload: action.payload,
                    };
                    let worker = self.workers.next_worker();
                    let span = tracing::debug_span!(
                        "dispatch",
                        action_id = payload.action_id,
                        instance_id = payload.instance_id,
                        sequence = payload.sequence
                    );
                    let fut: BoxFuture<'_, Result<RoundTripMetrics, MessageError>> =
                        Box::pin(async move { worker.send_action(payload).await }.instrument(span));
                    inflight.push(fut);
                    dispatched += 1;
                }
            }

            match inflight.next().await {
                Some(Ok(metrics)) => {
                    let record = CompletionRecord {
                        action_id: metrics.action_id,
                        success: metrics.success,
                        delivery_id: metrics.delivery_id,
                        result_payload: metrics.response_payload.clone(),
                    };
                    if let Err(err) = self.completion_tx.send(record).await {
                        warn!(?err, "completion channel closed");
                    }
                    tracing::debug!(
                        action_id = metrics.action_id,
                        round_trip_ms = %format!("{:.3}", metrics.round_trip.as_secs_f64() * 1000.0),
                        ack_ms = %format!("{:.3}", metrics.ack_latency.as_secs_f64() * 1000.0),
                        worker_ms = %format!("{:.3}", metrics.worker_duration.as_secs_f64() * 1000.0),
                        "action completed"
                    );
                    completed.push(BenchmarkResult::from(metrics));
                    if let Some(interval) = config.progress_interval {
                        let now = Instant::now();
                        if now.duration_since(last_report) >= interval {
                            let elapsed = now.duration_since(start);
                            let throughput =
                                completed.len() as f64 / elapsed.as_secs_f64().max(1e-9);
                            info!(
                                processed = completed.len(),
                                total,
                                elapsed = %format!("{:.1}s", elapsed.as_secs_f64()),
                                throughput = %format!("{:.0} msg/s", throughput),
                                in_flight = inflight.len(),
                                worker_count,
                                db_queue = dispatched - completed.len(),
                                dispatched,
                                "benchmark progress",
                            );
                            last_report = now;
                        }
                    }
                }
                Some(Err(err)) => return Err(err.into()),
                None => {
                    if dispatched >= total {
                        break;
                    }
                }
            }
        }

        let elapsed = start.elapsed();
        let summary = BenchmarkSummary::from_results(completed, elapsed);
        Ok(summary)
    }

    pub async fn shutdown(self) -> Result<()> {
        let BenchmarkHarness {
            workers,
            completion_tx,
            completion_handle,
            ..
        } = self;
        drop(completion_tx);
        workers.shutdown().await?;
        if let Err(err) = completion_handle.await {
            warn!(?err, "completion worker failed");
        }
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

async fn run_completion_worker(db: Database, mut rx: mpsc::Receiver<CompletionRecord>) {
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
                            if let Err(err) = db.mark_actions_batch(&buffer).await {
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
                if let Err(err) = db.mark_actions_batch(&buffer).await {
                    warn!(?err, "failed to flush completion batch");
                }
                buffer.clear();
            }
        }
    }
    if !buffer.is_empty()
        && let Err(err) = db.mark_actions_batch(&buffer).await
    {
        warn!(?err, "failed to flush completion batch");
    }
}

fn build_benchmark_payload(payload_size: usize) -> Vec<u8> {
    let payload_data = "x".repeat(payload_size);
    let invocation = json!({
        "action": BENCHMARK_ACTION,
        "kwargs": {
            "request": {
                "kind": "basemodel",
                "model": {
                    "module": BENCHMARK_USER_MODULE,
                    "name": BENCHMARK_REQUEST_MODEL,
                },
                "data": {
                    "payload": payload_data,
                }
            }
        }
    });
    serde_json::to_vec(&invocation).expect("serialize benchmark payload")
}
