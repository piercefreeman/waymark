//! InstanceWorkerBridge gRPC service.
//!
//! Handles bidirectional streaming with instance workers.
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
    instance_worker_bridge_server::InstanceWorkerBridge, Ack, Envelope, InstanceActions,
    InstanceComplete, InstanceDispatch, MessageKind, WorkerHello, WorkerType,
    WorkflowArguments, WorkflowArgumentValue,
};

/// Convert protobuf WorkflowArgumentValue to serde_json::Value.
fn proto_value_to_json(value: &WorkflowArgumentValue) -> serde_json::Value {
    use crate::messages::workflow_argument_value::Kind;
    use crate::messages::primitive_workflow_argument::Kind as PrimKind;

    match &value.kind {
        Some(Kind::Primitive(prim)) => match &prim.kind {
            Some(PrimKind::StringValue(s)) => serde_json::Value::String(s.clone()),
            Some(PrimKind::IntValue(i)) => serde_json::json!(*i),
            Some(PrimKind::DoubleValue(d)) => serde_json::json!(*d),
            Some(PrimKind::BoolValue(b)) => serde_json::Value::Bool(*b),
            Some(PrimKind::NullValue(_)) => serde_json::Value::Null,
            None => serde_json::Value::Null,
        },
        Some(Kind::ListValue(list)) => {
            let items: Vec<serde_json::Value> = list.items.iter().map(proto_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        Some(Kind::DictValue(dict)) => {
            let mut map = serde_json::Map::new();
            for entry in &dict.entries {
                if let Some(v) = &entry.value {
                    map.insert(entry.key.clone(), proto_value_to_json(v));
                }
            }
            serde_json::Value::Object(map)
        }
        Some(Kind::TupleValue(tuple)) => {
            let items: Vec<serde_json::Value> = tuple.items.iter().map(proto_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        _ => serde_json::Value::Null,
    }
}

/// Convert protobuf WorkflowArguments to serde_json::Value (as an object).
fn proto_args_to_json(args: &WorkflowArguments) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for arg in &args.arguments {
        if let Some(v) = &arg.value {
            map.insert(arg.key.clone(), proto_value_to_json(v));
        }
    }
    serde_json::Value::Object(map)
}

/// State for a connected instance worker.
struct WorkerConnection {
    worker_id: u64,
    tx: mpsc::Sender<Envelope>,
    /// Number of instances currently in-flight for this worker
    in_flight: AtomicUsize,
}

/// Shared state for the instance worker bridge.
pub struct InstanceWorkerBridgeState {
    /// Connected workers by ID
    workers: RwLock<HashMap<u64, Arc<WorkerConnection>>>,
    /// Next delivery ID
    next_delivery_id: std::sync::atomic::AtomicU64,
    /// Max concurrent instances per worker (from config)
    max_concurrent_per_worker: usize,
}

impl InstanceWorkerBridgeState {
    pub fn new(max_concurrent_per_worker: usize) -> Self {
        InstanceWorkerBridgeState {
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

    /// Send an instance dispatch to an available worker.
    /// Picks the worker with the most available capacity.
    /// Increments in_flight counter on dispatch.
    pub async fn dispatch_instance(&self, dispatch: InstanceDispatch) -> Result<(), Status> {
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
                kind: MessageKind::InstanceDispatch as i32,
                payload: prost::Message::encode_to_vec(&dispatch),
            };

            if let Err(e) = conn.tx.send(envelope).await {
                // Failed to send, decrement in_flight
                conn.in_flight.fetch_sub(1, Ordering::SeqCst);
                return Err(Status::unavailable(format!("Worker disconnected: {}", e)));
            }

            Ok(())
        } else {
            Err(Status::resource_exhausted(
                "No workers with available capacity",
            ))
        }
    }

    /// Get the number of connected workers.
    pub async fn worker_count(&self) -> usize {
        self.workers.read().await.len()
    }
}

/// InstanceWorkerBridge gRPC service implementation.
pub struct InstanceWorkerBridgeService {
    state: Arc<InstanceWorkerBridgeState>,
    db: Arc<Database>,
}

impl InstanceWorkerBridgeService {
    pub fn new(state: Arc<InstanceWorkerBridgeState>, db: Arc<Database>) -> Self {
        InstanceWorkerBridgeService { state, db }
    }
}

#[tonic::async_trait]
impl InstanceWorkerBridge for InstanceWorkerBridgeService {
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

                                if hello.worker_type() != WorkerType::Instance {
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
                                    "Instance worker connected"
                                );
                            }

                            MessageKind::Ack => {
                                let ack: Ack =
                                    prost::Message::decode(envelope.payload.as_slice())
                                        .unwrap_or_default();
                                debug!(
                                    delivery_id = ack.acked_delivery_id,
                                    "Received ack from instance worker"
                                );
                            }

                            MessageKind::InstanceActions => {
                                let actions_msg: InstanceActions =
                                    prost::Message::decode(envelope.payload.as_slice())
                                        .unwrap_or_default();

                                info!(
                                    instance_id = %actions_msg.instance_id,
                                    pending_count = actions_msg.pending_actions.len(),
                                    "Instance reported pending actions"
                                );

                                // Decrement in_flight - instance yielded with pending actions
                                if let Some(wid) = worker_id {
                                    let workers = state.workers.read().await;
                                    if let Some(conn) = workers.get(&wid) {
                                        conn.in_flight.fetch_sub(1, Ordering::SeqCst);
                                    }
                                }

                                // Parse IDs
                                let instance_id = match uuid::Uuid::parse_str(&actions_msg.instance_id) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        error!(error = %e, "Invalid instance_id");
                                        continue;
                                    }
                                };

                                let dispatch_token = match &actions_msg.dispatch_token {
                                    Some(t) => match uuid::Uuid::parse_str(t) {
                                        Ok(id) => id,
                                        Err(e) => {
                                            error!(error = %e, "Invalid dispatch_token");
                                            continue;
                                        }
                                    },
                                    None => {
                                        error!("Missing dispatch_token in instance actions");
                                        continue;
                                    }
                                };

                                // Add pending actions to database
                                let actions_to_add: Vec<(String, String, serde_json::Value, Option<i32>)> =
                                    actions_msg.pending_actions.iter().map(|a| {
                                        let kwargs = a.kwargs.as_ref()
                                            .map(proto_args_to_json)
                                            .unwrap_or_else(|| serde_json::json!({}));
                                        (
                                            a.action_name.clone(),
                                            a.module_name.clone(),
                                            kwargs,
                                            a.timeout_seconds.map(|t| t as i32),
                                        )
                                    }).collect();

                                let start_sequence = actions_msg.replayed_count as i32;
                                let new_actions_until = start_sequence + actions_to_add.len() as i32;

                                // Add actions to DB
                                if !actions_to_add.is_empty() {
                                    if let Err(e) = db.add_actions(instance_id, actions_to_add, start_sequence).await {
                                        error!(error = %e, "Failed to add actions to database");
                                        continue;
                                    }
                                }

                                // Update instance to waiting_for_actions
                                match db.set_instance_waiting(instance_id, dispatch_token, new_actions_until).await {
                                    Ok(updated) => {
                                        if updated {
                                            debug!(
                                                instance_id = %instance_id,
                                                actions_until = new_actions_until,
                                                "Instance set to waiting_for_actions"
                                            );
                                        } else {
                                            warn!(instance_id = %instance_id, "Instance update ignored (stale token)");
                                        }
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Failed to update instance status");
                                    }
                                }
                            }

                            MessageKind::InstanceComplete => {
                                let complete: InstanceComplete =
                                    prost::Message::decode(envelope.payload.as_slice())
                                        .unwrap_or_default();

                                info!(
                                    instance_id = %complete.instance_id,
                                    total_actions = complete.total_actions,
                                    "Instance completed"
                                );

                                // Decrement in_flight - instance completed
                                if let Some(wid) = worker_id {
                                    let workers = state.workers.read().await;
                                    if let Some(conn) = workers.get(&wid) {
                                        conn.in_flight.fetch_sub(1, Ordering::SeqCst);
                                    }
                                }

                                // Parse IDs
                                let instance_id = match uuid::Uuid::parse_str(&complete.instance_id) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        error!(error = %e, "Invalid instance_id");
                                        continue;
                                    }
                                };

                                let dispatch_token = match &complete.dispatch_token {
                                    Some(t) => match uuid::Uuid::parse_str(t) {
                                        Ok(id) => id,
                                        Err(e) => {
                                            error!(error = %e, "Invalid dispatch_token");
                                            continue;
                                        }
                                    },
                                    None => {
                                        error!("Missing dispatch_token in instance complete");
                                        continue;
                                    }
                                };

                                // Mark instance as completed
                                let result_json = serde_json::json!({
                                    "completed": true,
                                    "total_actions": complete.total_actions,
                                    // TODO: Properly deserialize result payload
                                });

                                match db.complete_instance(instance_id, dispatch_token, result_json).await {
                                    Ok(updated) => {
                                        if updated {
                                            info!(
                                                instance_id = %instance_id,
                                                total_actions = complete.total_actions,
                                                "Instance marked completed in database"
                                            );
                                        } else {
                                            warn!(instance_id = %instance_id, "Instance completion ignored (stale token)");
                                        }
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Failed to complete instance");
                                    }
                                }
                            }

                            _ => {
                                warn!(kind = ?kind, "Unexpected message from instance worker");
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Error receiving from instance worker");
                        break;
                    }
                }
            }

            // Cleanup on disconnect
            if let Some(id) = worker_id {
                let mut workers = state.workers.write().await;
                workers.remove(&id);
                info!(worker_id = id, "Instance worker disconnected");
            }
        });

        Ok(Response::new(Box::pin(outbound.map(Ok))))
    }
}

use futures::StreamExt;
