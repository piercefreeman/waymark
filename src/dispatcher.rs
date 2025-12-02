//! Action dispatcher for workflow execution.
//!
//! The dispatcher dequeues actions from the store and sends them to Python workers
//! for execution, then processes completions back through the store.

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
    PythonWorkerPool, Store,
    messages::proto,
    store::{ActionCompletion, QueuedAction},
    worker::ActionDispatchPayload,
};
use uuid::Uuid;

const COMPLETION_BUFFER_TARGET: usize = 48;
const COMPLETION_FLUSH_INTERVAL_MS: u64 = 2;

#[derive(Clone, Debug)]
pub struct DispatcherConfig {
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub max_concurrent: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
            max_concurrent: num_cpus::get().max(1) * 2,
        }
    }
}

/// Completion record for batching
#[derive(Debug, Clone)]
pub struct CompletionRecord {
    pub action_id: Uuid,
    pub node_id: String,
    pub instance_id: Uuid,
    pub success: bool,
    pub result_payload: Vec<u8>,
    pub dispatch_token: Option<Uuid>,
    pub control: Option<proto::WorkflowNodeControl>,
}

pub struct Dispatcher {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<Result<()>>,
}

impl Dispatcher {
    pub fn start(
        config: DispatcherConfig,
        store: Arc<Store>,
        worker_pool: Arc<PythonWorkerPool>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let task = DispatcherTask {
                config,
                store,
                worker_pool,
                shutdown_rx,
            };
            if let Err(err) = task.run().await {
                error!(?err, "dispatcher terminated with error");
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
            Err(err) => Err(anyhow!("dispatcher task panicked: {err}")),
        }
    }
}

struct DispatcherTask {
    config: DispatcherConfig,
    store: Arc<Store>,
    worker_pool: Arc<PythonWorkerPool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl DispatcherTask {
    async fn run(mut self) -> Result<()> {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            batch_size = self.config.batch_size,
            max_concurrent = self.config.max_concurrent,
            "starting dispatcher",
        );

        let mut ticker = interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent.max(1)));
        let (completion_tx, completion_rx) = self.completion_channel();
        let store = Arc::clone(&self.store);
        let completion_handle = tokio::spawn(async move {
            Self::completion_loop(store, completion_rx).await;
        });

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(err) = self.poll_and_dispatch(&semaphore, &completion_tx).await {
                        metrics::counter!("rappel_dispatch_errors_total").increment(1);
                        error!(?err, "dispatch cycle failed");
                    }
                }
                changed = self.shutdown_rx.changed() => {
                    if changed.is_ok() && *self.shutdown_rx.borrow() {
                        info!("dispatcher shutting down");
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
        let limit = available.min(self.config.batch_size.max(1) as usize).max(1);

        // Dequeue actions up to limit
        let mut actions = Vec::with_capacity(limit);
        for _ in 0..limit {
            match self.store.dequeue_action().await? {
                Some(action) => actions.push(action),
                None => break,
            }
        }

        if actions.is_empty() {
            return Ok(());
        }

        debug!(count = actions.len(), "dispatching actions");

        for action in actions {
            let permit = semaphore.clone().acquire_owned().await?;
            let store = Arc::clone(&self.store);
            let pool = Arc::clone(&self.worker_pool);
            let tx = completion_tx.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::dispatch_action(store, pool, tx, action, permit).await {
                    metrics::counter!("rappel_dispatch_errors_total").increment(1);
                    error!(?err, "dispatch task failed");
                }
            });
        }

        Ok(())
    }

    async fn dispatch_action(
        _store: Arc<Store>,
        worker_pool: Arc<PythonWorkerPool>,
        completion_tx: mpsc::Sender<CompletionRecord>,
        action: QueuedAction,
        _permit: OwnedSemaphorePermit,
    ) -> Result<()> {
        // Decode the dispatch payload
        let dispatch: proto::NodeDispatch = serde_json::from_str(&action.dispatch_json)?;

        let payload = ActionDispatchPayload {
            action_id: action.id,
            instance_id: action.instance_id,
            sequence: action.attempt,
            dispatch,
            timeout_seconds: action.timeout_seconds,
            max_retries: action.max_retries,
            attempt_number: action.attempt,
            dispatch_token: action.id, // Use action_id as dispatch token
        };

        let worker = worker_pool.next_worker();
        match worker.send_action(payload).await {
            Ok(metrics) => {
                let record = CompletionRecord {
                    action_id: metrics.action_id,
                    node_id: action.node_id,
                    instance_id: action.instance_id,
                    success: metrics.success,
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
                // TODO: requeue the action
            }
        }
        Ok(())
    }

    async fn completion_loop(store: Arc<Store>, mut rx: mpsc::Receiver<CompletionRecord>) {
        let mut buffer = Vec::with_capacity(COMPLETION_BUFFER_TARGET);
        let mut ticker = interval(Duration::from_millis(COMPLETION_FLUSH_INTERVAL_MS));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                Some(record) = rx.recv() => {
                    buffer.push(record);
                    if buffer.len() >= COMPLETION_BUFFER_TARGET {
                        Self::flush_buffer(&store, &mut buffer).await;
                    }
                }
                _ = ticker.tick() => {
                    if !buffer.is_empty() {
                        Self::flush_buffer(&store, &mut buffer).await;
                    }
                }
                else => break,
            }
        }
        if !buffer.is_empty() {
            Self::flush_buffer(&store, &mut buffer).await;
        }
    }

    async fn flush_buffer(store: &Store, buffer: &mut Vec<CompletionRecord>) {
        if buffer.is_empty() {
            return;
        }
        let mut pending = Vec::new();
        std::mem::swap(buffer, &mut pending);

        for record in pending {
            // Decode the result payload to get the value
            let result = if !record.result_payload.is_empty() {
                match proto::WorkflowArguments::decode(record.result_payload.as_slice()) {
                    Ok(args) => {
                        // Extract the "result" key from arguments
                        args.arguments
                            .iter()
                            .find(|arg| arg.key == "result")
                            .and_then(|arg| arg.value.as_ref())
                            .map(|v| decode_arg_value(v))
                    }
                    Err(_) => None,
                }
            } else {
                None
            };

            let completion = ActionCompletion {
                action_id: record.action_id,
                node_id: record.node_id,
                instance_id: record.instance_id,
                success: record.success,
                result,
            };

            if let Err(err) = store.complete_action(completion).await {
                metrics::counter!("rappel_dispatch_errors_total").increment(1);
                error!(?err, "failed to complete action");
            }
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
}

/// Decode a WorkflowArgumentValue to serde_json::Value
fn decode_arg_value(value: &proto::WorkflowArgumentValue) -> serde_json::Value {
    use proto::workflow_argument_value::Kind;
    use proto::primitive_workflow_argument::Kind as PrimitiveKind;

    match &value.kind {
        Some(Kind::Primitive(p)) => match &p.kind {
            Some(PrimitiveKind::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(PrimitiveKind::IntValue(i)) => serde_json::Value::Number((*i).into()),
            Some(PrimitiveKind::DoubleValue(d)) => {
                serde_json::Number::from_f64(*d)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
            Some(PrimitiveKind::BoolValue(b)) => serde_json::Value::Bool(*b),
            Some(PrimitiveKind::NullValue(_)) => serde_json::Value::Null,
            None => serde_json::Value::Null,
        },
        Some(Kind::ListValue(list)) => {
            serde_json::Value::Array(list.items.iter().map(decode_arg_value).collect())
        }
        Some(Kind::TupleValue(tuple)) => {
            serde_json::Value::Array(tuple.items.iter().map(decode_arg_value).collect())
        }
        Some(Kind::DictValue(dict)) => {
            let map: serde_json::Map<String, serde_json::Value> = dict.entries.iter()
                .filter_map(|entry| {
                    entry.value.as_ref().map(|v| (entry.key.clone(), decode_arg_value(v)))
                })
                .collect();
            serde_json::Value::Object(map)
        }
        Some(Kind::Basemodel(bm)) => {
            let mut map = serde_json::Map::new();
            map.insert("__type__".to_string(), serde_json::Value::String(format!("{}.{}", bm.module, bm.name)));
            if let Some(data) = &bm.data {
                for entry in &data.entries {
                    if let Some(v) = &entry.value {
                        map.insert(entry.key.clone(), decode_arg_value(v));
                    }
                }
            }
            serde_json::Value::Object(map)
        }
        Some(Kind::Exception(e)) => {
            let mut map = serde_json::Map::new();
            map.insert("__error__".to_string(), serde_json::Value::Bool(true));
            map.insert("type".to_string(), serde_json::Value::String(e.r#type.clone()));
            map.insert("module".to_string(), serde_json::Value::String(e.module.clone()));
            map.insert("message".to_string(), serde_json::Value::String(e.message.clone()));
            serde_json::Value::Object(map)
        }
        None => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::DispatcherConfig;

    #[test]
    fn default_config_values() {
        let config = DispatcherConfig::default();
        assert_eq!(config.poll_interval, std::time::Duration::from_millis(100));
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.max_concurrent, num_cpus::get().max(1) * 2);
    }
}
