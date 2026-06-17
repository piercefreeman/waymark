use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use waymark_convert_core::TryConvert as _;
use waymark_ids::InstanceId;
use waymark_proto::messages as proto;
use waymark_scheduler_backend::SchedulerBackend as _;
use waymark_scheduler_core::{CreateScheduleParams, ScheduleStatus, ScheduleType};

use crate::WorkflowStore;

pub struct BridgeService {
    pub store: Option<Arc<WorkflowStore>>,
}

#[tonic::async_trait]
impl proto::workflow_service_server::WorkflowService for BridgeService {
    type ExecuteWorkflowStream = std::pin::Pin<
        Box<
            dyn futures_core::Stream<Item = Result<proto::WorkflowStreamResponse, Status>>
                + Send
                + 'static,
        >,
    >;

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

        let (executable_id, entry_input_names) = store
            .compile_and_store(&registration)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        let call_spec =
            waymark_workflow_initialization_convert_proto::InitialContextConverter::try_convert((
                registration.initial_context.as_ref(),
                &entry_input_names[..],
            ))
            .map_err(|err| Status::internal(format!("build entry call spec: {err}")))?;

        let vm_id = InstanceId::new_uuid_v4();
        store
            .register_vm_runtime(vm_id, executable_id, call_spec)
            .await
            .map_err(|err| Status::internal(format!("register vm runtime: {err}")))?;

        Ok(Response::new(proto::RegisterWorkflowResponse {
            workflow_version_id: executable_id.to_string(),
            workflow_instance_id: vm_id.to_string(),
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

        let (executable_id, entry_input_names) = store
            .compile_and_store(&registration)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        let target_count = request.count as usize;
        if target_count == 0 {
            return Err(Status::invalid_argument("count must be >= 1"));
        }

        let call_spec: waymark_system_vm::CallSpec =
            waymark_workflow_initialization_convert_proto::InitialContextConverter::try_convert((
                registration.initial_context.as_ref(),
                &entry_input_names[..],
            ))
            .map_err(|err| Status::internal(format!("build entry call spec: {err}")))?;

        let mut vm_ids = Vec::new();
        let include_ids = request.include_instance_ids;

        for _ in 0..target_count {
            let vm_id = InstanceId::new_uuid_v4();
            if include_ids {
                vm_ids.push(vm_id.to_string());
            }

            store
                .register_vm_runtime(vm_id, executable_id, call_spec.clone())
                .await
                .map_err(|err| Status::internal(format!("register vm runtime: {err}")))?;
        }

        Ok(Response::new(proto::RegisterWorkflowBatchResponse {
            workflow_version_id: executable_id.to_string(),
            workflow_instance_ids: vm_ids,
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
        let mut in_stream = request.into_inner();

        // The first message must carry a WorkflowRegistration.
        let first_msg = in_stream
            .message()
            .await
            .map_err(|err| Status::internal(format!("stream error: {err}")))?
            .ok_or_else(|| Status::invalid_argument("stream closed before registration"))?;

        let registration = match first_msg.kind {
            Some(proto::workflow_stream_request::Kind::Registration(reg)) => reg,
            _ => {
                return Err(Status::invalid_argument(
                    "first message must be a WorkflowRegistration",
                ));
            }
        };
        let _skip_sleep = first_msg.skip_sleep;

        let runtime = waymark_transient_execution_bringup::setup_runtime(&registration)
            .await
            .map_err(|err| Status::internal(format!("setup runtime: {err}")))?;

        let waymark_transient_execution_bringup::ExecuteChannels {
            out_rx,
            action_result_tx,
        } = waymark_transient_execution_bringup::execute(runtime);

        // Feed ActionResult messages from the gRPC input stream into the
        // execution's action-result channel.
        tokio::spawn(async move {
            loop {
                match in_stream.message().await {
                    Ok(Some(msg)) => {
                        if let Some(proto::workflow_stream_request::Kind::ActionResult(result)) =
                            msg.kind
                            && action_result_tx.send(result).await.is_err()
                        {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        tracing::warn!(?err, "gRPC input stream error");
                        break;
                    }
                }
            }
        });

        let out_stream = ReceiverStream::new(out_rx);
        Ok(Response::new(Box::pin(out_stream)))
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
                .compile_and_store(&registration)
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
