use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use prost::Message;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    task::JoinHandle,
    time::{MissedTickBehavior, interval, sleep},
};
use tracing::{debug, error, info, warn};

use crate::{
    Database, LedgerAction, PythonWorkerPool, db::CompletionRecord, messages::proto,
    worker::ActionDispatchPayload,
};

const COMPLETION_BUFFER_TARGET: usize = 64;
const COMPLETION_FLUSH_INTERVAL_MS: u64 = 50;

#[derive(Clone, Debug)]
pub struct PollingConfig {
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub max_concurrent: usize,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
            max_concurrent: num_cpus::get().max(1) * 2,
        }
    }
}

pub struct PollingDispatcher {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<Result<()>>,
}

impl PollingDispatcher {
    pub fn start(
        config: PollingConfig,
        database: Arc<Database>,
        worker_pool: Arc<PythonWorkerPool>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let task = DispatcherTask {
                config,
                database,
                worker_pool,
                shutdown_rx,
            };
            if let Err(err) = task.run().await {
                error!(?err, "polling dispatcher terminated with error");
                Err(err)
            } else {
                Ok(())
            }
        });
        Self {
            shutdown_tx,
            handle,
        }
    }

    pub fn trigger_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub async fn shutdown(self) -> Result<()> {
        self.trigger_shutdown();
        match self.handle.await {
            Ok(result) => result,
            Err(err) => Err(anyhow!("polling dispatcher task panicked: {err}")),
        }
    }
}

struct DispatcherTask {
    config: PollingConfig,
    database: Arc<Database>,
    worker_pool: Arc<PythonWorkerPool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl DispatcherTask {
    async fn run(mut self) -> Result<()> {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            batch_size = self.config.batch_size,
            max_concurrent = self.config.max_concurrent,
            "starting polling dispatcher",
        );

        let mut ticker = interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut timeout_ticker = interval(Duration::from_secs(5));
        timeout_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent.max(1)));
        let (completion_tx, completion_rx) = self.completion_channel();
        let db = Arc::clone(&self.database);
        let completion_handle = tokio::spawn(async move {
            Self::completion_loop(db, completion_rx).await;
        });

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(err) = self.poll_and_dispatch(&semaphore, &completion_tx).await {
                        metrics::counter!("rappel_dispatch_errors_total").increment(1);
                        error!(?err, "polling cycle failed");
                    }
                }
                _ = timeout_ticker.tick() => {
                    if let Err(err) = self.check_timeouts().await {
                        metrics::counter!("rappel_dispatch_errors_total").increment(1);
                        warn!(?err, "timeout check failed");
                    }
                }
                changed = self.shutdown_rx.changed() => {
                    if changed.is_ok() && *self.shutdown_rx.borrow() {
                        info!("polling dispatcher shutting down");
                        break;
                    }
                }
            }
        }

        self.wait_for_inflight(&semaphore).await;
        drop(completion_tx);
        let _ = completion_handle.await;
        Ok(())
    }

    fn completion_channel(
        &self,
    ) -> (
        mpsc::Sender<CompletionRecord>,
        mpsc::Receiver<CompletionRecord>,
    ) {
        let capacity = (self.config.batch_size as usize).max(COMPLETION_BUFFER_TARGET) * 2;
        mpsc::channel(capacity)
    }

    async fn poll_and_dispatch(
        &self,
        semaphore: &Arc<Semaphore>,
        completion_tx: &mpsc::Sender<CompletionRecord>,
    ) -> Result<()> {
        let available = semaphore.available_permits();
        if available == 0 {
            return Ok(());
        }
        let limit = available.min(self.config.batch_size.max(1) as usize).max(1) as i64;

        let actions = self.database.dispatch_actions(limit).await?;

        if actions.is_empty() {
            return Ok(());
        }

        debug!(count = actions.len(), "dispatching actions");

        for action in actions {
            let permit = semaphore.clone().acquire_owned().await?;
            let db = Arc::clone(&self.database);
            let pool = Arc::clone(&self.worker_pool);
            let tx = completion_tx.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::dispatch_action(db, pool, tx, action, permit).await {
                    metrics::counter!("rappel_dispatch_errors_total").increment(1);
                    error!(?err, "dispatch task failed");
                }
            });
        }

        Ok(())
    }

    async fn dispatch_action(
        database: Arc<Database>,
        worker_pool: Arc<PythonWorkerPool>,
        completion_tx: mpsc::Sender<CompletionRecord>,
        action: LedgerAction,
        _permit: OwnedSemaphorePermit,
    ) -> Result<()> {
        // Sleep actions are auto-completed by the scheduler - they were queued with a future
        // scheduled_at time and are picked up once that time has passed
        if action.function_name == "sleep" {
            debug!(
                action_id = %action.id,
                instance_id = %action.instance_id,
                "auto-completing sleep action"
            );
            // Create a proper result payload with a null value for the "result" key
            let result_payload = proto::WorkflowArguments {
                arguments: vec![proto::WorkflowArgument {
                    key: "result".to_string(),
                    value: Some(proto::WorkflowArgumentValue {
                        kind: Some(proto::workflow_argument_value::Kind::Primitive(
                            proto::PrimitiveWorkflowArgument {
                                kind: Some(proto::primitive_workflow_argument::Kind::NullValue(
                                    prost_types::NullValue::NullValue as i32,
                                )),
                            },
                        )),
                    }),
                }],
            };
            let record = CompletionRecord {
                action_id: action.id,
                success: true,
                delivery_id: 0,
                result_payload: result_payload.encode_to_vec(),
                dispatch_token: Some(action.delivery_token),
                control: None,
            };
            if let Err(err) = completion_tx.send(record).await {
                warn!(?err, "completion channel closed, dropping sleep completion");
            }
            return Ok(());
        }

        let dispatch = match proto::WorkflowNodeDispatch::decode(action.dispatch_payload.as_slice())
        {
            Ok(dispatch) => dispatch,
            Err(err) => {
                warn!(
                    action_id = %action.id,
                    instance_id = %action.instance_id,
                    ?err,
                    "failed to decode action dispatch payload; requeueing",
                );
                database.requeue_action(action.id).await?;
                return Ok(());
            }
        };
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
        let worker = worker_pool.next_worker();
        match worker.send_action(payload).await {
            Ok(metrics) => {
                let record = CompletionRecord {
                    action_id: metrics.action_id,
                    success: metrics.success,
                    delivery_id: metrics.delivery_id,
                    result_payload: metrics.response_payload,
                    dispatch_token: metrics.dispatch_token,
                    control: metrics.control,
                };
                if let Err(err) = completion_tx.send(record).await {
                    warn!(?err, "completion channel closed, dropping record");
                }
            }
            Err(err) => {
                warn!(
                    action_id = %action.id,
                    instance_id = %action.instance_id,
                    ?err,
                    "worker rejected action",
                );
                database.requeue_action(action.id).await?;
            }
        }
        Ok(())
    }

    async fn completion_loop(database: Arc<Database>, mut rx: mpsc::Receiver<CompletionRecord>) {
        let mut buffer = Vec::with_capacity(COMPLETION_BUFFER_TARGET);
        let mut ticker = interval(Duration::from_millis(COMPLETION_FLUSH_INTERVAL_MS));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                Some(record) = rx.recv() => {
                    buffer.push(record);
                    if buffer.len() >= COMPLETION_BUFFER_TARGET {
                        Self::flush_buffer(&database, &mut buffer).await;
                    }
                }
                _ = ticker.tick() => {
                    if !buffer.is_empty() {
                        Self::flush_buffer(&database, &mut buffer).await;
                    }
                }
                else => break,
            }
        }
        if !buffer.is_empty() {
            Self::flush_buffer(&database, &mut buffer).await;
        }
    }

    async fn flush_buffer(database: &Database, buffer: &mut Vec<CompletionRecord>) {
        if buffer.is_empty() {
            return;
        }
        let mut pending = Vec::new();
        std::mem::swap(buffer, &mut pending);
        if let Err(err) = database.mark_actions_batch(&pending).await {
            metrics::counter!("rappel_dispatch_errors_total").increment(1);
            error!(?err, "failed to mark action batch, retrying");
            buffer.extend(pending);
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn wait_for_inflight(&mut self, semaphore: &Arc<Semaphore>) {
        let expected = self.config.max_concurrent.max(1);
        loop {
            if semaphore.available_permits() == expected {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn check_timeouts(&self) -> Result<()> {
        let limit = self.config.batch_size.max(1);
        let count = self.database.mark_timed_out_actions(limit).await?;
        if count > 0 {
            info!(count, "marked timed out actions");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PollingConfig;

    #[test]
    fn default_config_values() {
        let config = PollingConfig::default();
        assert_eq!(config.poll_interval, std::time::Duration::from_millis(100));
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.max_concurrent, num_cpus::get().max(1) * 2);
    }
}
