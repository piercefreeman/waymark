use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use waymark_convert_core::{Convert as _, TryConvert as _};
use waymark_ids::InstanceId;
use waymark_proto::messages as proto;

use crate::WorkflowStore;

/// The server-side cap (and default) for how many VM registrations go into a
/// single backend statement.
const REGISTRATION_BATCH_MAX_CAP: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

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

        let (executable_id, executable, entry_input_names) = store
            .compile_and_store(&registration)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        let call_spec =
            waymark_workflow_initialization_convert_proto::Converter::<
                waymark_vm_value_python_convert_proto::Converter,
            >::try_convert((&registration.arguments[..], &entry_input_names[..]))
            .map_err(|err| Status::internal(format!("build entry call spec: {err}")))?;

        let vm_id = InstanceId::new_uuid_v4();
        store
            .register_vm_runtime(vm_id, executable_id, executable, call_spec)
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

        let (executable_id, executable, entry_input_names) = store
            .compile_and_store(&registration)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        // Resolve the per-instance entry call specs. A non-empty `arguments_list`
        // provides one arguments payload per instance and overrides
        // `count`/`arguments`; otherwise `count` instances each share
        // `arguments`, falling back to the registration's own when unset.
        #[allow(clippy::result_large_err, reason = "tonic forces this")]
        let build_call_spec = |arguments: &[u8]| {
            waymark_workflow_initialization_convert_proto::Converter::<
                waymark_vm_value_python_convert_proto::Converter,
            >::try_convert((arguments, &entry_input_names[..]))
            .map_err(|err| Status::internal(format!("build entry call spec: {err}")))
        };

        let call_specs: NEVec<waymark_system_vm::CallSpec> =
            match NEVec::try_from_vec(request.arguments_list) {
                #[allow(clippy::result_large_err, reason = "tonic forces this")]
                Some(arguments_list) => arguments_list
                    .into_nonempty_iter()
                    .map(|arguments| build_call_spec(&arguments))
                    .collect::<Result<_, _>>()?,
                None => {
                    let Some(target_count) = NonZeroUsize::new(request.count as usize) else {
                        return Err(Status::invalid_argument(
                            "count must be >= 1 when arguments_list is empty",
                        ));
                    };

                    let base = request
                        .arguments
                        .as_deref()
                        .unwrap_or(&registration.arguments);
                    NEVec::from_elem(build_call_spec(base)?, target_count)
                }
            };

        let queued = call_specs.len().get() as u32;

        let vms: NEVec<_> = call_specs
            .into_nonempty_iter()
            .map(|call_spec| (InstanceId::new_uuid_v4(), call_spec))
            .collect();

        let vm_ids = if request.include_instance_ids {
            vms.iter().map(|(vm_id, _)| vm_id.to_string()).collect()
        } else {
            Vec::new()
        };

        // Chunk database inserts by the client-requested batch size, clamped
        // to the server cap; 0 (unset) means the cap itself.
        let batch_max = NonZeroUsize::new(request.batch_size as usize)
            .map_or(REGISTRATION_BATCH_MAX_CAP, |requested| {
                requested.min(REGISTRATION_BATCH_MAX_CAP)
            });

        store
            .register_vm_runtimes(executable_id, executable, batch_max, vms)
            .await
            .map_err(|err| Status::internal(format!("register vm runtimes: {err}")))?;

        Ok(Response::new(proto::RegisterWorkflowBatchResponse {
            workflow_version_id: executable_id.to_string(),
            workflow_instance_ids: vm_ids,
            queued,
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

        Ok(Response::new(proto::WaitForInstanceResponse { payload }))
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

        let runtime =
            waymark_transient_execution_worker_stream_bringup::setup_runtime(&registration)
                .map_err(|err| Status::internal(format!("setup runtime: {err}")))?;

        let waymark_transient_execution_worker_stream_bringup::ExecuteChannels {
            out_rx,
            action_result_tx,
        } = waymark_transient_execution_worker_stream_bringup::execute(
            runtime,
            first_msg.skip_sleep,
        );

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

        let registration = request
            .registration
            .ok_or_else(|| Status::invalid_argument("registration missing"))?;
        let schedule = request
            .schedule
            .ok_or_else(|| Status::invalid_argument("schedule missing"))?;
        if request.schedule_name.is_empty() {
            return Err(Status::invalid_argument("schedule_name missing"));
        }

        // The conversion is the validation point: unset oneof, bad cron,
        // and non-positive numbers all die here.
        let definition: waymark_scheduler_core::ScheduleDefinition =
            waymark_scheduler_convert_proto::Converter::try_convert(&schedule)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;

        let next_run_at = store
            .register_schedule(&registration, &request.schedule_name, definition)
            .await
            .map_err(|err| match err {
                crate::workflow_store::RegisterScheduleError::NoOccurrences
                | crate::workflow_store::RegisterScheduleError::NextRun(_) => {
                    Status::invalid_argument(err.to_string())
                }
                crate::workflow_store::RegisterScheduleError::Internal(report) => {
                    Status::internal(format!("register schedule: {report}"))
                }
            })?;

        Ok(Response::new(proto::RegisterScheduleResponse {
            next_run_at: Some(waymark_scheduler_convert_proto::Converter::convert(
                next_run_at,
            )),
        }))
    }

    async fn update_schedule_status(
        &self,
        request: Request<proto::UpdateScheduleStatusRequest>,
    ) -> Result<Response<waymark_proto::prost_wkt_types::Empty>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;
        let request = request.into_inner();

        let status: waymark_scheduler_core::ScheduleStatus =
            waymark_scheduler_convert_proto::Converter::try_convert(request.status())
                .map_err(|err| Status::invalid_argument(err.to_string()))?;

        let updated = store
            .update_schedule_status(&request.schedule_name, status)
            .await
            .map_err(|err| Status::internal(format!("update schedule status: {err}")))?;
        if !updated {
            return Err(Status::not_found("no such schedule"));
        }
        Ok(Response::new(waymark_proto::prost_wkt_types::Empty {}))
    }

    async fn delete_schedule(
        &self,
        request: Request<proto::DeleteScheduleRequest>,
    ) -> Result<Response<waymark_proto::prost_wkt_types::Empty>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("bridge running in memory mode"))?;
        let request = request.into_inner();

        let deleted = store
            .delete_schedule(&request.schedule_name)
            .await
            .map_err(|err| Status::internal(format!("delete schedule: {err}")))?;
        if !deleted {
            return Err(Status::not_found("no such schedule"));
        }
        Ok(Response::new(waymark_proto::prost_wkt_types::Empty {}))
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

        // The zero value is the no-filter default, not an error.
        let status_filter = match request.status_filter() {
            proto::ScheduleStatus::Unspecified => None,
            proto::ScheduleStatus::Active => Some(waymark_scheduler_core::ScheduleStatus::Active),
            proto::ScheduleStatus::Paused => Some(waymark_scheduler_core::ScheduleStatus::Paused),
        };

        let records = store
            .list_schedules(status_filter)
            .await
            .map_err(|err| Status::internal(format!("list schedules: {err}")))?;

        let schedules = records
            .into_iter()
            .map(|record| {
                let wire_definition: proto::ScheduleDefinition =
                    waymark_scheduler_convert_proto::Converter::try_convert(&record.definition)
                        .map_err(|err| Status::internal(err.to_string()))?;
                let status: proto::ScheduleStatus =
                    waymark_scheduler_convert_proto::Converter::convert(record.status);

                Ok(proto::ScheduleInfo {
                    workflow_name: record.workflow_name,
                    status: status.into(),
                    next_run_at: Some(waymark_scheduler_convert_proto::Converter::convert(
                        record.next_run_at,
                    )),
                    last_instance_id: record
                        .last_instance_id
                        .map(|last_instance_id| last_instance_id.to_string())
                        .unwrap_or_default(),
                    schedule_name: record.schedule_name,
                    definition: Some(wire_definition),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(proto::ListSchedulesResponse { schedules }))
    }
}
