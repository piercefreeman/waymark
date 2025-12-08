//! ActionWorkerBridge gRPC service.
//!
//! Handles bidirectional streaming with action workers.
//! Tracks capacity per worker and only dequeues what we can handle.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::Stream;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use crate::db::Database;
use crate::messages::{
    action_worker_bridge_server::ActionWorkerBridge, Ack, ActionDispatch, ActionResult, Envelope,
    MessageKind, WorkerHello, WorkerType, WorkflowArguments, WorkflowArgumentValue,
    workflow_argument_value::Kind as ValueKind,
    primitive_workflow_argument::Kind as PrimitiveKind,
};

/// Convert a WorkflowArgumentValue proto to a JSON value.
fn proto_value_to_json(value: &WorkflowArgumentValue) -> serde_json::Value {
    match &value.kind {
        Some(ValueKind::Primitive(prim)) => match &prim.kind {
            Some(PrimitiveKind::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(PrimitiveKind::DoubleValue(d)) => serde_json::json!(*d),
            Some(PrimitiveKind::IntValue(i)) => serde_json::json!(*i),
            Some(PrimitiveKind::BoolValue(b)) => serde_json::Value::Bool(*b),
            Some(PrimitiveKind::NullValue(_)) => serde_json::Value::Null,
            None => serde_json::Value::Null,
        },
        Some(ValueKind::ListValue(list)) => {
            let items: Vec<serde_json::Value> = list.items.iter().map(proto_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        Some(ValueKind::TupleValue(tuple)) => {
            let items: Vec<serde_json::Value> = tuple.items.iter().map(proto_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        Some(ValueKind::DictValue(dict)) => {
            let map: serde_json::Map<String, serde_json::Value> = dict
                .entries
                .iter()
                .filter_map(|entry| {
                    entry.value.as_ref().map(|v| (entry.key.clone(), proto_value_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(map)
        }
        Some(ValueKind::Basemodel(bm)) => {
            // Convert BaseModel to a dict with type info
            let mut map = serde_json::Map::new();
            map.insert("__type__".to_string(), serde_json::json!({
                "module": bm.module.clone(),
                "name": bm.name.clone(),
            }));
            if let Some(data) = &bm.data {
                for entry in &data.entries {
                    if let Some(v) = &entry.value {
                        map.insert(entry.key.clone(), proto_value_to_json(v));
                    }
                }
            }
            serde_json::Value::Object(map)
        }
        Some(ValueKind::Exception(exc)) => {
            serde_json::json!({
                "__exception__": true,
                "type": exc.r#type,
                "module": exc.module,
                "message": exc.message,
                "traceback": exc.traceback,
            })
        }
        None => serde_json::Value::Null,
    }
}

/// Convert WorkflowArguments proto to a JSON object.
fn proto_args_to_json(args: &Option<WorkflowArguments>) -> serde_json::Value {
    match args {
        Some(workflow_args) => {
            let map: serde_json::Map<String, serde_json::Value> = workflow_args
                .arguments
                .iter()
                .filter_map(|arg| {
                    arg.value.as_ref().map(|v| (arg.key.clone(), proto_value_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(map)
        }
        None => serde_json::json!({}),
    }
}

/// State for a connected action worker.
struct WorkerConnection {
    worker_id: u64,
    tx: mpsc::Sender<Envelope>,
    /// Number of actions currently in-flight for this worker
    in_flight: AtomicUsize,
}

/// Shared state for the action worker bridge.
pub struct ActionWorkerBridgeState {
    /// Connected workers by ID
    workers: RwLock<HashMap<u64, Arc<WorkerConnection>>>,
    /// Next delivery ID
    next_delivery_id: std::sync::atomic::AtomicU64,
    /// Max concurrent actions per worker (from config)
    max_concurrent_per_worker: usize,
}

impl ActionWorkerBridgeState {
    pub fn new(max_concurrent_per_worker: usize) -> Self {
        ActionWorkerBridgeState {
            workers: RwLock::new(HashMap::new()),
            next_delivery_id: std::sync::atomic::AtomicU64::new(1),
            max_concurrent_per_worker,
        }
    }

    /// Get total available slots across all connected workers.
    pub async fn available_slots(&self) -> usize {
        let workers = self.workers.read().await;
        workers
            .values()
            .map(|w| {
                let in_flight = w.in_flight.load(Ordering::SeqCst);
                self.max_concurrent_per_worker.saturating_sub(in_flight)
            })
            .sum()
    }

    /// Get the next delivery ID.
    pub fn next_delivery_id(&self) -> u64 {
        self.next_delivery_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Send an action dispatch to an available worker.
    /// Picks the worker with the most available capacity.
    /// Increments in_flight counter on dispatch.
    pub async fn dispatch_action(&self, dispatch: ActionDispatch) -> Result<(), Status> {
        let workers = self.workers.read().await;

        // Find worker with most available capacity
        let best_worker = workers
            .values()
            .filter(|w| {
                let in_flight = w.in_flight.load(Ordering::SeqCst);
                in_flight < self.max_concurrent_per_worker
            })
            .min_by_key(|w| w.in_flight.load(Ordering::SeqCst));

        if let Some(conn) = best_worker {
            // Increment in_flight BEFORE sending
            conn.in_flight.fetch_add(1, Ordering::SeqCst);

            let envelope = Envelope {
                delivery_id: self.next_delivery_id(),
                partition_id: 0,
                kind: MessageKind::ActionDispatch as i32,
                payload: prost::Message::encode_to_vec(&dispatch),
            };

            if let Err(e) = conn.tx.send(envelope).await {
                // Failed to send, decrement in_flight
                conn.in_flight.fetch_sub(1, Ordering::SeqCst);
                return Err(Status::unavailable(format!("Worker disconnected: {}", e)));
            }

            Ok(())
        } else {
            Err(Status::resource_exhausted("No workers with available capacity"))
        }
    }

    /// Decrement in_flight counter for a worker when action completes.
    pub async fn action_completed(&self, worker_id: u64) {
        let workers = self.workers.read().await;
        if let Some(conn) = workers.get(&worker_id) {
            conn.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Get the number of connected workers.
    pub async fn worker_count(&self) -> usize {
        self.workers.read().await.len()
    }
}

/// ActionWorkerBridge gRPC service implementation.
pub struct ActionWorkerBridgeService {
    state: Arc<ActionWorkerBridgeState>,
    db: Arc<Database>,
}

impl ActionWorkerBridgeService {
    pub fn new(state: Arc<ActionWorkerBridgeState>, db: Arc<Database>) -> Self {
        ActionWorkerBridgeService { state, db }
    }
}

#[tonic::async_trait]
impl ActionWorkerBridge for ActionWorkerBridgeService {
    type AttachStream = Pin<Box<dyn Stream<Item = Result<Envelope, Status>> + Send>>;

    async fn attach(
        &self,
        request: Request<Streaming<Envelope>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let mut inbound = request.into_inner();

        // Create outbound channel
        let (tx, rx) = mpsc::channel::<Envelope>(32);
        let outbound = ReceiverStream::new(rx);

        let state = self.state.clone();
        let db = self.db.clone();

        // Spawn handler task
        tokio::spawn(async move {
            let mut worker_id: Option<u64> = None;

            while let Some(result) = inbound.message().await.transpose() {
                match result {
                    Ok(envelope) => {
                        let kind = MessageKind::try_from(envelope.kind)
                            .unwrap_or(MessageKind::Unspecified);

                        match kind {
                            MessageKind::WorkerHello => {
                                let hello: WorkerHello =
                                    prost::Message::decode(envelope.payload.as_slice())
                                        .unwrap_or_default();

                                if hello.worker_type() != WorkerType::Action {
                                    warn!(
                                        worker_id = hello.worker_id,
                                        "Worker connected with wrong type"
                                    );
                                    break;
                                }

                                worker_id = Some(hello.worker_id);

                                // Register worker with capacity tracking
                                let mut workers = state.workers.write().await;
                                workers.insert(
                                    hello.worker_id,
                                    Arc::new(WorkerConnection {
                                        worker_id: hello.worker_id,
                                        tx: tx.clone(),
                                        in_flight: AtomicUsize::new(0),
                                    }),
                                );

                                info!(
                                    worker_id = hello.worker_id,
                                    max_concurrent = state.max_concurrent_per_worker,
                                    "Action worker connected"
                                );
                            }

                            MessageKind::Ack => {
                                let ack: Ack =
                                    prost::Message::decode(envelope.payload.as_slice())
                                        .unwrap_or_default();
                                debug!(
                                    delivery_id = ack.acked_delivery_id,
                                    "Received ack from action worker"
                                );
                            }

                            MessageKind::ActionResult => {
                                let action_result: ActionResult =
                                    prost::Message::decode(envelope.payload.as_slice())
                                        .unwrap_or_default();

                                info!(
                                    action_id = %action_result.action_id,
                                    success = action_result.success,
                                    "Received action result"
                                );

                                // Decrement in_flight counter for this worker
                                if let Some(wid) = worker_id {
                                    let workers = state.workers.read().await;
                                    if let Some(conn) = workers.get(&wid) {
                                        conn.in_flight.fetch_sub(1, Ordering::SeqCst);
                                    }
                                }

                                // Parse IDs
                                let action_id = match uuid::Uuid::parse_str(&action_result.action_id) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        error!(error = %e, "Invalid action_id");
                                        continue;
                                    }
                                };

                                let dispatch_token = match &action_result.dispatch_token {
                                    Some(t) => match uuid::Uuid::parse_str(t) {
                                        Ok(id) => id,
                                        Err(e) => {
                                            error!(error = %e, "Invalid dispatch_token");
                                            continue;
                                        }
                                    },
                                    None => {
                                        error!("Missing dispatch_token in action result");
                                        continue;
                                    }
                                };

                                // Update database with result
                                if action_result.success {
                                    // Convert payload proto to JSON
                                    let result_json = proto_args_to_json(&action_result.payload);

                                    match db.complete_action_atomic(action_id, dispatch_token, result_json).await {
                                        Ok((action_updated, instance_rescheduled)) => {
                                            if action_updated {
                                                debug!(
                                                    action_id = %action_id,
                                                    instance_rescheduled = instance_rescheduled,
                                                    "Action completed in database"
                                                );
                                            } else {
                                                warn!(action_id = %action_id, "Action completion ignored (stale token)");
                                            }
                                        }
                                        Err(e) => {
                                            error!(error = %e, "Failed to complete action");
                                        }
                                    }
                                } else {
                                    let error_msg = action_result.error_message.as_deref().unwrap_or("Unknown error");
                                    match db.fail_action_atomic(action_id, dispatch_token, error_msg).await {
                                        Ok(updated) => {
                                            if updated {
                                                debug!(action_id = %action_id, "Action failed in database");
                                            } else {
                                                warn!(action_id = %action_id, "Action failure ignored (stale token)");
                                            }
                                        }
                                        Err(e) => {
                                            error!(error = %e, "Failed to mark action as failed");
                                        }
                                    }
                                }
                            }

                            _ => {
                                warn!(kind = ?kind, "Unexpected message from action worker");
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Error receiving from action worker");
                        break;
                    }
                }
            }

            // Cleanup on disconnect
            if let Some(id) = worker_id {
                let mut workers = state.workers.write().await;
                workers.remove(&id);
                info!(worker_id = id, "Action worker disconnected");
            }
        });

        Ok(Response::new(Box::pin(outbound.map(Ok))))
    }
}

use futures::StreamExt;
