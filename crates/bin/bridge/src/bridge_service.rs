use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use futures_core::Stream;
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use tracing::debug;

use waymark_backend_memory::MemoryBackend;
use waymark_core_backend::{InstanceDone, QueuedInstance};
use waymark_dag_builder::convert_to_dag;
use waymark_ids::{InstanceId, LockId, WorkflowVersionId};
use waymark_ir_conversions::literal_from_json_value;
use waymark_proto::{ast as ir, messages as proto};
use waymark_runloop::{RunLoop, RunLoopConfig};
use waymark_runner_executor_core::ExecutionException;
use waymark_runner_state::RunnerState;
use waymark_scheduler_backend::SchedulerBackend as _;
use waymark_scheduler_core::{CreateScheduleParams, ScheduleStatus, ScheduleType};
use waymark_worker_core::ActionCompletion;
use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowRegistryBackend as _};

use crate::StreamWorkerPool;
use crate::WorkflowStore;

#[cfg(test)]
mod tests;

pub struct BridgeService {
    pub store: Option<Arc<WorkflowStore>>,
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

        let instance_id = InstanceId::new_uuid_v4();
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
            let instance_id = InstanceId::new_uuid_v4();
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
        let instance_id: InstanceId = request
            .instance_id
            .parse()
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
        let backend = MemoryBackend::with_queue(Arc::clone(&queue));

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

        let instance_id = InstanceId::new_uuid_v4();
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
        let backend_for_run = backend.clone();
        tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();

            let Ok(runtime) = runtime else {
                let payload = crate::utils::build_workflow_arguments(
                    None,
                    Some(ExecutionException(error_value(
                        "RunLoopError",
                        "failed to start runtime",
                    ))),
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
                let runloop = RunLoop::new(
                    worker_pool,
                    backend,
                    RunLoopConfig {
                        max_concurrent_instances: 25.try_into().unwrap(),
                        executor_shards: 1.try_into().unwrap(),
                        instance_done_batch_size: None,
                        // TODO: do we really want no interval here?
                        poll_interval: None,
                        persistence_interval: Some(
                            Duration::from_secs_f64(0.1).try_into().unwrap(),
                        ),
                        lock_uuid: LockId::new_uuid_v4(),
                        lock_ttl: Duration::from_secs(15).try_into().unwrap(),
                        lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
                        evict_sleep_threshold: Duration::from_secs(10).try_into().unwrap(),
                        skip_sleep,
                        active_instance_gauge: None,
                    },
                );
                let run_result = runloop.run().await;

                let payload = match run_result {
                    Ok(_) => {
                        let done = find_latest_instance_done(&backend_for_run, instance_id);
                        if let Some(done) = done {
                            crate::utils::build_workflow_arguments(done.result, done.error)
                        } else {
                            crate::utils::build_workflow_arguments(
                                None,
                                Some(ExecutionException(error_value("RunLoopError", "no result"))),
                            )
                        }
                    }
                    Err(err) => crate::utils::build_workflow_arguments(
                        None,
                        Some(ExecutionException(error_value(
                            "RunLoopError",
                            &err.to_string(),
                        ))),
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
                            && let Some(completion) =
                                crate::stream_worker_pool::action_result_to_completion(
                                    result, &inflight,
                                )
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
            .update_schedule_status(schedule.id, status.as_str())
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
            .delete_schedule(schedule.id)
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

fn error_value(kind: &str, message: &str) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert(
        "type".to_string(),
        serde_json::Value::String(kind.to_string()),
    );
    map.insert(
        "message".to_string(),
        serde_json::Value::String(message.to_string()),
    );
    serde_json::Value::Object(map)
}

fn find_latest_instance_done(
    backend: &MemoryBackend,
    instance_id: InstanceId,
) -> Option<InstanceDone> {
    backend
        .instances_done()
        .into_iter()
        .rev()
        .find(|instance| instance.executor_id == instance_id)
}

fn build_queued_instance(
    instance_id: InstanceId,
    workflow_version_id: WorkflowVersionId,
    dag: Arc<waymark_dag::DAG>,
    initial_context: Option<proto::WorkflowArguments>,
) -> Result<QueuedInstance, String> {
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);

    if let Some(context) = initial_context {
        let inputs = workflow_arguments_to_json_map(&context);
        for (name, value) in inputs {
            let expr = literal_from_json_value(&value);
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
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    })
}

fn workflow_arguments_to_json_map(
    args: &proto::WorkflowArguments,
) -> HashMap<String, serde_json::Value> {
    let mut map = HashMap::new();
    for arg in &args.arguments {
        if let Some(value) = &arg.value {
            map.insert(
                arg.key.clone(),
                waymark_message_conversions::workflow_argument_value_to_json(value),
            );
        }
    }
    map
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
