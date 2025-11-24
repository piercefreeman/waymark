use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use prost::Message;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{Instrument, info, warn};

use crate::{
    benchmark::common::{BenchmarkResult, BenchmarkSummary, spawn_completion_worker},
    db::{CompletionRecord, Database},
    messages::{MessageError, proto},
    server_worker::WorkerBridgeServer,
    worker::{ActionDispatchPayload, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics},
};

#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub total_messages: usize,
    pub in_flight: usize,
    pub payload_size: usize,
    pub progress_interval: Option<Duration>,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            total_messages: 10_000,
            in_flight: 32,
            payload_size: 4096,
            progress_interval: None,
        }
    }
}

pub struct BenchmarkHarness {
    worker_server: Arc<WorkerBridgeServer>,
    workers: PythonWorkerPool,
    database: Database,
    completion_tx: mpsc::Sender<CompletionRecord>,
    completion_handle: JoinHandle<()>,
}

const BENCHMARK_USER_MODULE: &str = "benchmark.fixtures.benchmark_actions";
const BENCHMARK_ACTION: &str = "benchmark.echo_payload";
const BENCHMARK_REQUEST_MODEL: &str = "PayloadRequest";

impl BenchmarkHarness {
    pub async fn new(
        config: PythonWorkerConfig,
        worker_count: usize,
        database: Database,
    ) -> Result<Self> {
        let worker_server = WorkerBridgeServer::start(None).await?;
        let workers =
            PythonWorkerPool::new(config, worker_count, Arc::clone(&worker_server)).await?;
        let (tx, handle) = spawn_completion_worker(database.clone());
        Ok(Self {
            worker_server,
            workers,
            database,
            completion_tx: tx,
            completion_handle: handle,
        })
    }

    pub async fn run(&self, config: &HarnessConfig) -> Result<BenchmarkSummary> {
        self.database.reset_workflow_state().await?;
        let encoded_payload = build_benchmark_dispatch(config.payload_size)?;
        self.database
            .seed_actions(
                config.total_messages,
                BENCHMARK_USER_MODULE,
                BENCHMARK_ACTION,
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
                let actions = self.database.dispatch_actions(needed as i64).await?;
                if actions.is_empty() {
                    break;
                }

                for action in actions {
                    let dispatch =
                        proto::WorkflowNodeDispatch::decode(action.dispatch_payload.as_slice())
                            .map_err(|err| anyhow!("failed to decode benchmark dispatch: {err}"))?;
                    let payload = ActionDispatchPayload {
                        action_id: action.id,
                        instance_id: action.instance_id,
                        sequence: action.action_seq,
                        dispatch,
                        timeout_seconds: action.timeout_seconds,
                        max_retries: action.max_retries,
                        attempt_number: action.attempt_number,
                        dispatch_token: action.delivery_token,
                    };
                    let worker = self.workers.next_worker();
                    let span = tracing::debug_span!(
                        "dispatch",
                        action_id = %payload.action_id,
                        instance_id = %payload.instance_id,
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
                        dispatch_token: metrics.dispatch_token,
                        control: metrics.control.clone(),
                    };
                    if let Err(err) = self.completion_tx.send(record).await {
                        warn!(?err, "completion channel closed");
                    }
                    tracing::debug!(
                        action_id = %metrics.action_id,
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
            worker_server,
            workers,
            completion_tx,
            completion_handle,
            ..
        } = self;
        drop(completion_tx);
        workers.shutdown().await?;
        worker_server.shutdown().await;
        if let Err(err) = completion_handle.await {
            warn!(?err, "completion worker failed");
        }
        Ok(())
    }
}

fn build_benchmark_dispatch(payload_size: usize) -> Result<Vec<u8>> {
    let payload_data = "x".repeat(payload_size);

    // Build string primitive for payload value
    let payload_primitive = proto::PrimitiveWorkflowArgument {
        kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
            payload_data,
        )),
    };
    let payload_value = proto::WorkflowArgumentValue {
        kind: Some(proto::workflow_argument_value::Kind::Primitive(
            payload_primitive,
        )),
    };

    // Build dict for BaseModel data
    let dict_entries = vec![proto::WorkflowArgument {
        key: "payload".to_string(),
        value: Some(payload_value),
    }];
    let basemodel = proto::BaseModelWorkflowArgument {
        module: BENCHMARK_USER_MODULE.to_string(),
        name: BENCHMARK_REQUEST_MODEL.to_string(),
        data: Some(proto::WorkflowDictArgument {
            entries: dict_entries,
        }),
    };
    let argument_value = proto::WorkflowArgumentValue {
        kind: Some(proto::workflow_argument_value::Kind::Basemodel(basemodel)),
    };
    let arguments = proto::WorkflowArguments {
        arguments: vec![proto::WorkflowArgument {
            key: "request".to_string(),
            value: Some(argument_value),
        }],
    };
    let mut kwargs = HashMap::new();
    kwargs.insert("request".to_string(), "request".to_string());
    let node = proto::WorkflowDagNode {
        id: "benchmark".to_string(),
        action: BENCHMARK_ACTION.to_string(),
        module: BENCHMARK_USER_MODULE.to_string(),
        produces: vec!["result".to_string()],
        kwargs,
        ..Default::default()
    };
    let dispatch = proto::WorkflowNodeDispatch {
        node: Some(node),
        workflow_input: Some(arguments),
        context: Vec::new(),
        resolved_kwargs: None,
    };
    Ok(dispatch.encode_to_vec())
}
