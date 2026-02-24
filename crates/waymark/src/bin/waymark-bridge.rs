//! Waymark Bridge - gRPC server for workflow registration and singleton discovery.
//!
//! This binary starts the Waymark bridge server with:
//! - gRPC WorkflowService for workflow registration
//! - gRPC health check for singleton discovery
//!
//! Configuration is via environment variables:
//! - WAYMARK_DATABASE_URL: PostgreSQL connection string (required unless in-memory)
//! - WAYMARK_BRIDGE_GRPC_ADDR: gRPC server bind address (default: 127.0.0.1:24117)
//! - WAYMARK_BRIDGE_IN_MEMORY: enable in-memory execution mode for streaming workflows

use std::collections::{HashMap, VecDeque};
use std::env;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::{Stream, future::BoxFuture};
use prost::Message;
use serde_json::Value;
use sqlx::{PgPool, Row};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, async_trait};
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

use waymark::backends::{
    ActionDone, BackendError, BackendResult, CoreBackend, GraphUpdate, InstanceDone,
    InstanceLockStatus, LockClaim, PostgresBackend, QueuedInstance, QueuedInstanceBatch,
    SchedulerBackend, WorkflowRegistration, WorkflowRegistryBackend, WorkflowVersion,
};
use waymark::db;
use waymark::messages::{self, ast as ir, proto};
use waymark::scheduler::{CreateScheduleParams, ScheduleId, ScheduleStatus, ScheduleType};
use waymark::waymark_core::runloop::{RunLoop, RunLoopSupervisorConfig};
use waymark::waymark_core::runner::RunnerState;
use waymark::workers::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};
use waymark_dag::convert_to_dag;

const DEFAULT_GRPC_ADDR: &str = "127.0.0.1:24117";

#[derive(Clone)]
struct WorkflowStore {
    backend: PostgresBackend,
}

impl WorkflowStore {
    async fn connect(dsn: &str) -> Result<Self> {
        let pool = PgPool::connect(dsn).await?;
        db::run_migrations(&pool).await?;
        let backend = PostgresBackend::new(pool);
        Ok(Self { backend })
    }

    fn pool(&self) -> &sqlx::PgPool {
        self.backend.pool()
    }

    async fn upsert_workflow_version(
        &self,
        registration: &proto::WorkflowRegistration,
    ) -> Result<Uuid> {
        let workflow_version = if registration.workflow_version.is_empty() {
            registration.ir_hash.clone()
        } else {
            registration.workflow_version.clone()
        };
        let backend_registration = WorkflowRegistration {
            workflow_name: registration.workflow_name.clone(),
            workflow_version,
            ir_hash: registration.ir_hash.clone(),
            program_proto: registration.ir.clone(),
            concurrent: registration.concurrent,
        };
        self.backend
            .upsert_workflow_version(&backend_registration)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> Result<()> {
        self.backend
            .queue_instances(instances)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }

    async fn wait_for_instance(
        &self,
        instance_id: Uuid,
        poll_interval: Duration,
    ) -> Result<Option<proto::WorkflowArguments>> {
        loop {
            let row = sqlx::query(
                r#"
                SELECT result, error
                FROM runner_instances
                WHERE instance_id = $1
                "#,
            )
            .bind(instance_id)
            .fetch_optional(self.pool())
            .await?;

            let Some(row) = row else {
                return Ok(None);
            };

            let result_bytes: Option<Vec<u8>> = row.get("result");
            let error_bytes: Option<Vec<u8>> = row.get("error");

            if result_bytes.is_some() || error_bytes.is_some() {
                let result_value = result_bytes
                    .as_deref()
                    .map(rmp_serde::from_slice::<Value>)
                    .transpose()
                    .map_err(|err| anyhow::anyhow!(err))?;
                let error_value = error_bytes
                    .as_deref()
                    .map(rmp_serde::from_slice::<Value>)
                    .transpose()
                    .map_err(|err| anyhow::anyhow!(err))?;

                return Ok(Some(build_workflow_arguments(result_value, error_value)));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

type InstanceLockStore = HashMap<Uuid, (Option<Uuid>, Option<DateTime<Utc>>)>;
type WorkflowVersionStore = HashMap<(String, String), (Uuid, WorkflowRegistration)>;

#[derive(Clone)]
struct InMemoryBackend {
    queue: Arc<Mutex<VecDeque<QueuedInstance>>>,
    instances_done: Arc<Mutex<HashMap<Uuid, InstanceDone>>>,
    instance_locks: Arc<Mutex<InstanceLockStore>>,
    workflow_versions: Arc<Mutex<WorkflowVersionStore>>,
}

impl InMemoryBackend {
    fn new(
        queue: Arc<Mutex<VecDeque<QueuedInstance>>>,
        instances_done: Arc<Mutex<HashMap<Uuid, InstanceDone>>>,
    ) -> Self {
        Self {
            queue,
            instances_done,
            instance_locks: Arc::new(Mutex::new(HashMap::new())),
            workflow_versions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl CoreBackend for InMemoryBackend {
    fn clone_box(&self) -> Box<dyn CoreBackend> {
        Box::new(self.clone())
    }

    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let mut guard = self
            .instance_locks
            .lock()
            .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
        let mut locks = Vec::with_capacity(graphs.len());
        for graph in graphs {
            if let Some((Some(lock_uuid), lock_expires_at)) = guard.get_mut(&graph.instance_id)
                && *lock_uuid == claim.lock_uuid
                && lock_expires_at.is_none_or(|expires_at| expires_at < claim.lock_expires_at)
            {
                *lock_expires_at = Some(claim.lock_expires_at);
            }
            let (lock_uuid, lock_expires_at) = guard
                .get(&graph.instance_id)
                .cloned()
                .unwrap_or((None, None));
            locks.push(InstanceLockStatus {
                instance_id: graph.instance_id,
                lock_uuid,
                lock_expires_at,
            });
        }
        Ok(locks)
    }

    async fn save_actions_done(&self, _actions: &[ActionDone]) -> BackendResult<()> {
        Ok(())
    }

    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        if size == 0 {
            return Ok(QueuedInstanceBatch {
                instances: Vec::new(),
            });
        }
        let mut guard = self
            .queue
            .lock()
            .map_err(|_| BackendError::Message("in-memory queue lock poisoned".to_string()))?;
        let now = Utc::now();
        let mut instances = Vec::new();
        while instances.len() < size {
            let Some(instance) = guard.front() else {
                break;
            };
            if let Some(scheduled_at) = instance.scheduled_at
                && scheduled_at > now
            {
                break;
            }
            let instance = guard.pop_front().expect("in-memory queue reported empty");
            instances.push(instance);
        }
        if !instances.is_empty() {
            let mut locks = self
                .instance_locks
                .lock()
                .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
            for instance in &instances {
                locks.insert(
                    instance.instance_id,
                    (Some(claim.lock_uuid), Some(claim.lock_expires_at)),
                );
            }
        }
        Ok(QueuedInstanceBatch { instances })
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        let mut guard = self
            .instances_done
            .lock()
            .map_err(|_| BackendError::Message("in-memory results lock poisoned".to_string()))?;
        for instance in instances {
            guard.insert(instance.executor_id, instance.clone());
        }
        if !instances.is_empty() {
            let mut locks = self
                .instance_locks
                .lock()
                .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
            for instance in instances {
                locks.remove(&instance.executor_id);
            }
        }
        Ok(())
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        let mut guard = self
            .queue
            .lock()
            .map_err(|_| BackendError::Message("in-memory queue lock poisoned".to_string()))?;
        for instance in instances {
            guard.push_back(instance.clone());
        }
        Ok(())
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let mut guard = self
            .instance_locks
            .lock()
            .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
        let mut locks = Vec::new();
        for instance_id in instance_ids {
            let entry = guard
                .entry(*instance_id)
                .or_insert((Some(claim.lock_uuid), Some(claim.lock_expires_at)));
            if entry.0 == Some(claim.lock_uuid) {
                entry.1 = Some(claim.lock_expires_at);
            }
            locks.push(InstanceLockStatus {
                instance_id: *instance_id,
                lock_uuid: entry.0,
                lock_expires_at: entry.1,
            });
        }
        Ok(locks)
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        let mut guard = self
            .instance_locks
            .lock()
            .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
        for instance_id in instance_ids {
            if let Some((current_lock, _)) = guard.get(instance_id)
                && *current_lock == Some(lock_uuid)
            {
                guard.remove(instance_id);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl WorkflowRegistryBackend for InMemoryBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        let mut guard = self.workflow_versions.lock().map_err(|_| {
            BackendError::Message("in-memory workflow versions poisoned".to_string())
        })?;
        let key = (
            registration.workflow_name.clone(),
            registration.workflow_version.clone(),
        );
        if let Some((id, existing)) = guard.get(&key) {
            if existing.ir_hash != registration.ir_hash {
                return Err(BackendError::Message(format!(
                    "workflow version already exists with different IR hash: {}@{}",
                    registration.workflow_name, registration.workflow_version
                )));
            }
            return Ok(*id);
        }
        let id = Uuid::new_v4();
        guard.insert(key, (id, registration.clone()));
        Ok(id)
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let guard = self.workflow_versions.lock().map_err(|_| {
            BackendError::Message("in-memory workflow versions poisoned".to_string())
        })?;
        let mut versions = Vec::new();
        for (id, registration) in guard.values() {
            if ids.contains(id) {
                versions.push(WorkflowVersion {
                    id: *id,
                    workflow_name: registration.workflow_name.clone(),
                    workflow_version: registration.workflow_version.clone(),
                    ir_hash: registration.ir_hash.clone(),
                    program_proto: registration.program_proto.clone(),
                    concurrent: registration.concurrent,
                });
            }
        }
        Ok(versions)
    }
}

struct StreamWorkerPool {
    dispatch_tx: mpsc::Sender<proto::WorkflowStreamResponse>,
    completion_rx: Arc<AsyncMutex<mpsc::Receiver<ActionCompletion>>>,
    inflight: Arc<Mutex<HashMap<String, StreamInflightAction>>>,
}

#[derive(Clone, Copy)]
struct StreamInflightAction {
    executor_id: Uuid,
    attempt_number: u32,
    dispatch_token: Uuid,
}

impl StreamWorkerPool {
    fn new(
        dispatch_tx: mpsc::Sender<proto::WorkflowStreamResponse>,
        completion_rx: mpsc::Receiver<ActionCompletion>,
    ) -> Self {
        Self {
            dispatch_tx,
            completion_rx: Arc::new(AsyncMutex::new(completion_rx)),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn inflight(&self) -> Arc<Mutex<HashMap<String, StreamInflightAction>>> {
        Arc::clone(&self.inflight)
    }
}

impl BaseWorkerPool for StreamWorkerPool {
    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        let module_name = request
            .module_name
            .clone()
            .ok_or_else(|| WorkerPoolError::new("StreamWorkerPoolError", "missing module name"))?;
        let action_id = request.execution_id.to_string();

        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.insert(
                action_id.clone(),
                StreamInflightAction {
                    executor_id: request.executor_id,
                    attempt_number: request.attempt_number,
                    dispatch_token: request.dispatch_token,
                },
            );
        }

        let dispatch = proto::ActionDispatch {
            action_id: action_id.clone(),
            instance_id: request.executor_id.to_string(),
            sequence: 0,
            action_name: request.action_name,
            module_name,
            kwargs: Some(kwargs_to_workflow_arguments(&request.kwargs)),
            timeout_seconds: Some(request.timeout_seconds),
            max_retries: None,
            attempt_number: Some(request.attempt_number),
            dispatch_token: Some(request.dispatch_token.to_string()),
        };

        let response = proto::WorkflowStreamResponse {
            kind: Some(proto::workflow_stream_response::Kind::ActionDispatch(
                dispatch,
            )),
        };

        self.dispatch_tx.try_send(response).map_err(|err| {
            WorkerPoolError::new(
                "StreamWorkerPoolError",
                format!("failed to dispatch action: {err}"),
            )
        })?;

        Ok(())
    }

    fn get_complete<'a>(&'a self) -> BoxFuture<'a, Vec<ActionCompletion>> {
        Box::pin(async move {
            let mut receiver = self.completion_rx.lock().await;
            let mut completions = Vec::new();
            match receiver.recv().await {
                Some(first) => completions.push(first),
                None => return completions,
            }
            while let Ok(item) = receiver.try_recv() {
                completions.push(item);
            }
            completions
        })
    }
}

struct BridgeService {
    store: Option<Arc<WorkflowStore>>,
}

#[tonic::async_trait]
impl proto::workflow_service_server::WorkflowService for BridgeService {
    type ExecuteWorkflowStream =
        Pin<Box<dyn Stream<Item = Result<proto::WorkflowStreamResponse, Status>> + Send + 'static>>;

    async fn register_workflow(
        &self,
        request: Request<proto::RegisterWorkflowRequest>,
    ) -> Result<Response<proto::RegisterWorkflowResponse>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;

        let registration = request
            .into_inner()
            .registration
            .ok_or_else(|| Status::invalid_argument("registration missing"))?;

        let program = ir::Program::decode(&registration.ir[..])
            .map_err(|err| Status::invalid_argument(format!("invalid IR: {err}")))?;
        let dag = Arc::new(
            convert_to_dag(&program)
                .map_err(|err| Status::invalid_argument(format!("invalid DAG: {err}")))?,
        );

        let version_id = store
            .upsert_workflow_version(&registration)
            .await
            .map_err(|err| Status::internal(format!("database error: {err}")))?;

        let instance_id = Uuid::new_v4();
        let queued = build_queued_instance(
            instance_id,
            version_id,
            Arc::clone(&dag),
            registration.initial_context,
        )
        .map_err(Status::invalid_argument)?;

        store
            .queue_instances(&[queued])
            .await
            .map_err(|err| Status::internal(format!("queue failed: {err}")))?;

        Ok(Response::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_id: instance_id.to_string(),
        }))
    }

    async fn register_workflow_batch(
        &self,
        request: Request<proto::RegisterWorkflowBatchRequest>,
    ) -> Result<Response<proto::RegisterWorkflowBatchResponse>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;
        let request = request.into_inner();
        let registration = request
            .registration
            .ok_or_else(|| Status::invalid_argument("registration missing"))?;

        let program = ir::Program::decode(&registration.ir[..])
            .map_err(|err| Status::invalid_argument(format!("invalid IR: {err}")))?;
        let dag = Arc::new(
            convert_to_dag(&program)
                .map_err(|err| Status::invalid_argument(format!("invalid DAG: {err}")))?,
        );

        let version_id = store
            .upsert_workflow_version(&registration)
            .await
            .map_err(|err| Status::internal(format!("database error: {err}")))?;

        let mut inputs_list = request.inputs_list;
        let mut target_count = request.count as usize;
        if !inputs_list.is_empty() {
            target_count = inputs_list.len();
        }
        if target_count == 0 {
            return Err(Status::invalid_argument("count must be >= 1"));
        }

        let mut instances = Vec::new();
        let mut instance_ids = Vec::new();
        let default_inputs = request.inputs;
        let batch_size = request.batch_size.max(1) as usize;
        let include_ids = request.include_instance_ids;

        for _ in 0..target_count {
            let instance_id = Uuid::new_v4();
            let inputs = if !inputs_list.is_empty() {
                inputs_list.remove(0)
            } else {
                default_inputs.clone().unwrap_or_default()
            };

            let queued =
                build_queued_instance(instance_id, version_id, Arc::clone(&dag), Some(inputs))
                    .map_err(Status::invalid_argument)?;
            instances.push(queued);
            if include_ids {
                instance_ids.push(instance_id.to_string());
            }

            if instances.len() >= batch_size {
                store
                    .queue_instances(&instances)
                    .await
                    .map_err(|err| Status::internal(format!("queue failed: {err}")))?;
                instances.clear();
            }
        }

        if !instances.is_empty() {
            store
                .queue_instances(&instances)
                .await
                .map_err(|err| Status::internal(format!("queue failed: {err}")))?;
        }

        Ok(Response::new(proto::RegisterWorkflowBatchResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_ids: instance_ids,
            queued: target_count as u32,
        }))
    }

    async fn wait_for_instance(
        &self,
        request: Request<proto::WaitForInstanceRequest>,
    ) -> Result<Response<proto::WaitForInstanceResponse>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;
        let request = request.into_inner();
        let instance_id = Uuid::parse_str(&request.instance_id)
            .map_err(|_| Status::invalid_argument("invalid instance_id"))?;
        let poll_interval = Duration::from_secs_f64(request.poll_interval_secs.max(0.1));

        let payload = store
            .wait_for_instance(instance_id, poll_interval)
            .await
            .map_err(|err| Status::internal(format!("wait failed: {err}")))?
            .ok_or_else(|| Status::not_found("instance not found"))?;

        Ok(Response::new(proto::WaitForInstanceResponse {
            payload: payload.encode_to_vec(),
        }))
    }

    async fn execute_workflow(
        &self,
        request: Request<tonic::Streaming<proto::WorkflowStreamRequest>>,
    ) -> Result<Response<Self::ExecuteWorkflowStream>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await
            .map_err(|err| Status::internal(format!("stream error: {err}")))?;
        let Some(first) = first else {
            return Err(Status::invalid_argument("missing registration"));
        };

        let skip_sleep = first.skip_sleep;
        let registration = match first.kind {
            Some(proto::workflow_stream_request::Kind::Registration(registration)) => registration,
            _ => {
                return Err(Status::invalid_argument(
                    "first stream message must include registration",
                ));
            }
        };

        let program = ir::Program::decode(&registration.ir[..])
            .map_err(|err| Status::invalid_argument(format!("invalid IR: {err}")))?;
        let dag = Arc::new(
            convert_to_dag(&program)
                .map_err(|err| Status::invalid_argument(format!("invalid DAG: {err}")))?,
        );

        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let results = Arc::new(Mutex::new(HashMap::new()));
        let backend = InMemoryBackend::new(Arc::clone(&queue), Arc::clone(&results));

        let workflow_version = if registration.workflow_version.is_empty() {
            registration.ir_hash.clone()
        } else {
            registration.workflow_version.clone()
        };
        let version_id = backend
            .upsert_workflow_version(&WorkflowRegistration {
                workflow_name: registration.workflow_name.clone(),
                workflow_version,
                ir_hash: registration.ir_hash.clone(),
                program_proto: registration.ir.clone(),
                concurrent: registration.concurrent,
            })
            .await
            .map_err(|err| Status::internal(format!("workflow upsert failed: {err}")))?;

        let instance_id = Uuid::new_v4();
        let queued = build_queued_instance(
            instance_id,
            version_id,
            Arc::clone(&dag),
            registration.initial_context,
        )
        .map_err(Status::invalid_argument)?;
        queue
            .lock()
            .expect("in-memory queue lock")
            .push_back(queued);

        let (response_tx, response_rx) = mpsc::channel::<proto::WorkflowStreamResponse>(64);
        let (completion_tx, completion_rx) = mpsc::channel::<ActionCompletion>(64);

        let worker_pool = StreamWorkerPool::new(response_tx.clone(), completion_rx);
        let inflight = worker_pool.inflight();

        let response_tx_for_run = response_tx.clone();
        let results_for_run = Arc::clone(&results);
        tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();

            let Ok(runtime) = runtime else {
                let payload = build_workflow_arguments(
                    None,
                    Some(error_value("RunLoopError", "failed to start runtime")),
                );
                let response = proto::WorkflowStreamResponse {
                    kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
                        proto::WorkflowExecutionResult {
                            payload: payload.encode_to_vec(),
                        },
                    )),
                };
                let _ = response_tx_for_run.blocking_send(response);
                return;
            };

            runtime.block_on(async move {
                let mut runloop = RunLoop::new(
                    worker_pool,
                    backend,
                    RunLoopSupervisorConfig {
                        max_concurrent_instances: 25,
                        executor_shards: 1,
                        instance_done_batch_size: None,
                        poll_interval: Duration::from_secs_f64(0.0),
                        persistence_interval: Duration::from_secs_f64(0.1),
                        lock_uuid: Uuid::new_v4(),
                        lock_ttl: Duration::from_secs(15),
                        lock_heartbeat: Duration::from_secs(5),
                        evict_sleep_threshold: Duration::from_secs(10),
                        skip_sleep,
                        active_instance_gauge: None,
                    },
                );
                let run_result = runloop.run().await;

                let payload = match run_result {
                    Ok(_) => {
                        let done = results_for_run
                            .lock()
                            .ok()
                            .and_then(|map| map.get(&instance_id).cloned());
                        if let Some(done) = done {
                            build_workflow_arguments(done.result, done.error)
                        } else {
                            build_workflow_arguments(
                                None,
                                Some(error_value("RunLoopError", "no result")),
                            )
                        }
                    }
                    Err(err) => build_workflow_arguments(
                        None,
                        Some(error_value("RunLoopError", &err.to_string())),
                    ),
                };

                let response = proto::WorkflowStreamResponse {
                    kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
                        proto::WorkflowExecutionResult {
                            payload: payload.encode_to_vec(),
                        },
                    )),
                };
                let _ = response_tx_for_run.send(response).await;
            });
        });

        tokio::spawn(async move {
            loop {
                let message = stream.message().await;
                match message {
                    Ok(Some(request)) => {
                        if let Some(proto::workflow_stream_request::Kind::ActionResult(result)) =
                            request.kind
                            && let Some(completion) = action_result_to_completion(result, &inflight)
                        {
                            let _ = completion_tx.send(completion).await;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        debug!(error = %err, "workflow stream closed");
                        break;
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(response_rx).map(Ok);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ExecuteWorkflowStream
        ))
    }

    async fn register_schedule(
        &self,
        request: Request<proto::RegisterScheduleRequest>,
    ) -> Result<Response<proto::RegisterScheduleResponse>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;
        let request = request.into_inner();

        if let Some(registration) = request.registration {
            store
                .upsert_workflow_version(&registration)
                .await
                .map_err(|err| Status::internal(format!("workflow upsert failed: {err}")))?;
        }

        let schedule = request
            .schedule
            .ok_or_else(|| Status::invalid_argument("schedule missing"))?;

        let schedule_type = match proto_schedule_type(schedule.r#type) {
            Some(value) => value,
            None => return Err(Status::invalid_argument("invalid schedule type")),
        };

        let schedule_name = if request.schedule_name.is_empty() {
            "default".to_string()
        } else {
            request.schedule_name
        };

        let cron_expression = if schedule.cron_expression.is_empty() {
            None
        } else {
            Some(schedule.cron_expression.clone())
        };

        let params = CreateScheduleParams {
            workflow_name: request.workflow_name.clone(),
            schedule_name,
            schedule_type,
            cron_expression,
            interval_seconds: if schedule.interval_seconds == 0 {
                None
            } else {
                Some(schedule.interval_seconds)
            },
            jitter_seconds: schedule.jitter_seconds,
            input_payload: request.inputs.map(|args| args.encode_to_vec()),
            priority: request.priority.unwrap_or(0),
            allow_duplicate: request.allow_duplicate.unwrap_or(false),
        };

        let schedule_id = store
            .backend
            .upsert_schedule(&params)
            .await
            .map_err(|err| Status::internal(format!("schedule upsert failed: {err}")))?;

        let schedule = store
            .backend
            .get_schedule_by_name(&request.workflow_name, &params.schedule_name)
            .await
            .map_err(|err| Status::internal(format!("schedule fetch failed: {err}")))?;

        let next_run_at = schedule
            .and_then(|item| item.next_run_at)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();

        Ok(Response::new(proto::RegisterScheduleResponse {
            schedule_id: schedule_id.to_string(),
            next_run_at,
        }))
    }

    async fn update_schedule_status(
        &self,
        request: Request<proto::UpdateScheduleStatusRequest>,
    ) -> Result<Response<proto::UpdateScheduleStatusResponse>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;
        let request = request.into_inner();
        let status = proto_schedule_status(request.status)
            .ok_or_else(|| Status::invalid_argument("invalid schedule status"))?;

        let schedule = store
            .backend
            .get_schedule_by_name(&request.workflow_name, &request.schedule_name)
            .await
            .map_err(|err| Status::internal(format!("schedule fetch failed: {err}")))?;

        let Some(schedule) = schedule else {
            return Ok(Response::new(proto::UpdateScheduleStatusResponse {
                success: false,
            }));
        };

        let updated = store
            .backend
            .update_schedule_status(ScheduleId(schedule.id), status.as_str())
            .await
            .map_err(|err| Status::internal(format!("schedule update failed: {err}")))?;

        Ok(Response::new(proto::UpdateScheduleStatusResponse {
            success: updated,
        }))
    }

    async fn delete_schedule(
        &self,
        request: Request<proto::DeleteScheduleRequest>,
    ) -> Result<Response<proto::DeleteScheduleResponse>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;
        let request = request.into_inner();

        let schedule = store
            .backend
            .get_schedule_by_name(&request.workflow_name, &request.schedule_name)
            .await
            .map_err(|err| Status::internal(format!("schedule fetch failed: {err}")))?;

        let Some(schedule) = schedule else {
            return Ok(Response::new(proto::DeleteScheduleResponse {
                success: false,
            }));
        };

        let deleted = store
            .backend
            .delete_schedule(ScheduleId(schedule.id))
            .await
            .map_err(|err| Status::internal(format!("schedule delete failed: {err}")))?;

        Ok(Response::new(proto::DeleteScheduleResponse {
            success: deleted,
        }))
    }

    async fn list_schedules(
        &self,
        request: Request<proto::ListSchedulesRequest>,
    ) -> Result<Response<proto::ListSchedulesResponse>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;
        let request = request.into_inner();

        let schedules = store
            .backend
            .list_schedules(1000, 0)
            .await
            .map_err(|err| Status::internal(format!("schedule list failed: {err}")))?;

        let status_filter = request.status_filter;
        let schedules = schedules
            .into_iter()
            .filter(|schedule| {
                if let Some(filter) = &status_filter {
                    schedule.status == *filter
                } else {
                    true
                }
            })
            .map(|schedule| proto::ScheduleInfo {
                id: schedule.id.to_string(),
                workflow_name: schedule.workflow_name,
                schedule_type: proto_schedule_type_from_str(&schedule.schedule_type)
                    .unwrap_or(proto::ScheduleType::Unspecified)
                    as i32,
                cron_expression: schedule.cron_expression.unwrap_or_default(),
                interval_seconds: schedule.interval_seconds.unwrap_or_default(),
                status: proto_schedule_status_from_str(&schedule.status)
                    .unwrap_or(proto::ScheduleStatus::Unspecified) as i32,
                next_run_at: schedule
                    .next_run_at
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default(),
                last_run_at: schedule
                    .last_run_at
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default(),
                last_instance_id: schedule
                    .last_instance_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                created_at: schedule.created_at.to_rfc3339(),
                updated_at: schedule.updated_at.to_rfc3339(),
                schedule_name: schedule.schedule_name,
                jitter_seconds: schedule.jitter_seconds,
                allow_duplicate: schedule.allow_duplicate,
            })
            .collect();

        Ok(Response::new(proto::ListSchedulesResponse { schedules }))
    }
}

fn grpc_addr_from_env() -> SocketAddr {
    env::var("WAYMARK_BRIDGE_GRPC_ADDR")
        .unwrap_or_else(|_| DEFAULT_GRPC_ADDR.to_string())
        .parse()
        .expect("invalid WAYMARK_BRIDGE_GRPC_ADDR")
}

fn in_memory_mode() -> bool {
    env::var("WAYMARK_BRIDGE_IN_MEMORY")
        .ok()
        .map(|value| {
            let lowered = value.trim().to_ascii_lowercase();
            !lowered.is_empty() && lowered != "0" && lowered != "false" && lowered != "no"
        })
        .unwrap_or(false)
}

fn error_value(kind: &str, message: &str) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("type".to_string(), Value::String(kind.to_string()));
    map.insert("message".to_string(), Value::String(message.to_string()));
    Value::Object(map)
}

fn build_workflow_arguments(
    result: Option<Value>,
    error: Option<Value>,
) -> proto::WorkflowArguments {
    let mut arguments = Vec::new();
    if let Some(value) = result {
        arguments.push(proto::WorkflowArgument {
            key: "result".to_string(),
            value: Some(workflow_node_result_value(&value)),
        });
    }
    if let Some(value) = error {
        arguments.push(proto::WorkflowArgument {
            key: "error".to_string(),
            value: Some(messages::json_to_workflow_argument_value(&value)),
        });
    }
    proto::WorkflowArguments { arguments }
}

fn workflow_node_result_value(value: &Value) -> proto::WorkflowArgumentValue {
    let variables_value = match value {
        Value::Object(_) => value.clone(),
        other => {
            let mut map = serde_json::Map::new();
            map.insert("result".to_string(), other.clone());
            Value::Object(map)
        }
    };

    let variables_arg = messages::json_to_workflow_argument_value(&variables_value);
    let dict = proto::WorkflowDictArgument {
        entries: vec![proto::WorkflowArgument {
            key: "variables".to_string(),
            value: Some(variables_arg),
        }],
    };

    proto::WorkflowArgumentValue {
        kind: Some(proto::workflow_argument_value::Kind::Basemodel(
            proto::BaseModelWorkflowArgument {
                module: "waymark.workflow_runtime".to_string(),
                name: "WorkflowNodeResult".to_string(),
                data: Some(dict),
            },
        )),
    }
}

fn kwargs_to_workflow_arguments(kwargs: &HashMap<String, Value>) -> proto::WorkflowArguments {
    let mut arguments = Vec::with_capacity(kwargs.len());
    for (key, value) in kwargs {
        let arg_value = messages::json_to_workflow_argument_value(value);
        arguments.push(proto::WorkflowArgument {
            key: key.clone(),
            value: Some(arg_value),
        });
    }
    proto::WorkflowArguments { arguments }
}

fn action_result_to_completion(
    result: proto::ActionResult,
    inflight: &Arc<Mutex<HashMap<String, StreamInflightAction>>>,
) -> Option<ActionCompletion> {
    let action_id = result.action_id.clone();
    let inflight_action = inflight
        .lock()
        .ok()
        .and_then(|mut map| map.remove(&action_id))?;
    let execution_id = Uuid::parse_str(&action_id).ok()?;

    let payload = result
        .payload
        .as_ref()
        .map(|payload| payload.encode_to_vec())
        .and_then(|bytes| messages::workflow_arguments_to_json(&bytes))
        .unwrap_or(Value::Null);

    let value = if result.success {
        if let Value::Object(mut map) = payload {
            map.remove("result").unwrap_or(Value::Object(map))
        } else {
            payload
        }
    } else if let Value::Object(mut map) = payload {
        let error = map.remove("error").unwrap_or(Value::Object(map));
        normalize_error_value(error)
    } else {
        normalize_error_value(payload)
    };

    Some(ActionCompletion {
        executor_id: inflight_action.executor_id,
        execution_id,
        attempt_number: inflight_action.attempt_number,
        dispatch_token: inflight_action.dispatch_token,
        result: value,
    })
}

fn normalize_error_value(error: Value) -> Value {
    let Value::Object(mut map) = error else {
        return error;
    };

    if let Some(Value::Object(exception)) = map.remove("__exception__") {
        return ensure_error_fields(exception);
    }

    ensure_error_fields(map)
}

fn ensure_error_fields(mut map: serde_json::Map<String, Value>) -> Value {
    let error_type = map
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("RemoteWorkerError")
        .to_string();
    let error_message = map
        .get("message")
        .and_then(|value| value.as_str())
        .unwrap_or("remote worker error")
        .to_string();
    if !map.contains_key("type") {
        map.insert("type".to_string(), Value::String(error_type));
    }
    if !map.contains_key("message") {
        map.insert("message".to_string(), Value::String(error_message));
    }
    Value::Object(map)
}

fn build_queued_instance(
    instance_id: Uuid,
    workflow_version_id: Uuid,
    dag: Arc<waymark_dag::DAG>,
    initial_context: Option<proto::WorkflowArguments>,
) -> Result<QueuedInstance, String> {
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);

    if let Some(context) = initial_context {
        let inputs = workflow_arguments_to_json_map(&context);
        for (name, value) in inputs {
            let expr = literal_from_value(&value);
            let label = format!("input {name} = {value}");
            let _ = state
                .record_assignment(vec![name.clone()], &expr, None, Some(label))
                .map_err(|err| err.0)?;
        }
    }

    let entry_node = dag
        .entry_node
        .as_ref()
        .ok_or_else(|| "DAG entry node not found".to_string())?;

    let entry_exec = state
        .queue_template_node(entry_node, None)
        .map_err(|err| err.0)?;

    Ok(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    })
}

fn workflow_arguments_to_json_map(args: &proto::WorkflowArguments) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    for arg in &args.arguments {
        if let Some(value) = &arg.value {
            map.insert(
                arg.key.clone(),
                messages::workflow_argument_value_to_json(value),
            );
        }
    }
    map
}

fn literal_from_value(value: &Value) -> ir::Expr {
    match value {
        Value::Bool(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::BoolValue(*value)),
            })),
            span: None,
        },
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(value)),
                    })),
                    span: None,
                }
            } else {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::FloatValue(
                            number.as_f64().unwrap_or(0.0),
                        )),
                    })),
                    span: None,
                }
            }
        }
        Value::String(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::StringValue(value.clone())),
            })),
            span: None,
        },
        Value::Array(items) => ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: items.iter().map(literal_from_value).collect(),
            })),
            span: None,
        },
        Value::Object(map) => {
            let entries = map
                .iter()
                .map(|(key, value)| ir::DictEntry {
                    key: Some(literal_from_value(&Value::String(key.clone()))),
                    value: Some(literal_from_value(value)),
                })
                .collect();
            ir::Expr {
                kind: Some(ir::expr::Kind::Dict(ir::DictExpr { entries })),
                span: None,
            }
        }
        Value::Null => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IsNone(true)),
            })),
            span: None,
        },
    }
}

fn proto_schedule_type(value: i32) -> Option<ScheduleType> {
    match proto::ScheduleType::try_from(value).ok()? {
        proto::ScheduleType::Cron => Some(ScheduleType::Cron),
        proto::ScheduleType::Interval => Some(ScheduleType::Interval),
        _ => None,
    }
}

fn proto_schedule_type_from_str(value: &str) -> Option<proto::ScheduleType> {
    match value {
        "cron" => Some(proto::ScheduleType::Cron),
        "interval" => Some(proto::ScheduleType::Interval),
        _ => None,
    }
}

fn proto_schedule_status(value: i32) -> Option<ScheduleStatus> {
    match proto::ScheduleStatus::try_from(value).ok()? {
        proto::ScheduleStatus::Active => Some(ScheduleStatus::Active),
        proto::ScheduleStatus::Paused => Some(ScheduleStatus::Paused),
        _ => None,
    }
}

fn proto_schedule_status_from_str(value: &str) -> Option<proto::ScheduleStatus> {
    match value {
        "active" => Some(proto::ScheduleStatus::Active),
        "paused" => Some(proto::ScheduleStatus::Paused),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "waymark=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let grpc_addr = grpc_addr_from_env();
    let in_memory = in_memory_mode();

    let store = if in_memory {
        None
    } else {
        let dsn = env::var("WAYMARK_DATABASE_URL").context("WAYMARK_DATABASE_URL must be set")?;
        Some(Arc::new(WorkflowStore::connect(&dsn).await?))
    };

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

    let service = BridgeService { store };
    health_reporter
        .set_serving::<proto::workflow_service_server::WorkflowServiceServer<BridgeService>>()
        .await;

    info!(%grpc_addr, in_memory, "waymark bridge starting");

    tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(proto::workflow_service_server::WorkflowServiceServer::new(
            service,
        ))
        .serve(grpc_addr)
        .await
        .context("bridge server exited")?;

    Ok(())
}
