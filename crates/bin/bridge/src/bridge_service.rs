use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use prost::Message;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use waymark_convert_core::TryConvert as _;
use waymark_ids::InstanceId;
use waymark_proto::messages as proto;

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

        let (executable_id, executable, entry_input_names) = store
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

        // Resolve the per-instance entry call specs. A non-empty `inputs_list`
        // provides one initial context per instance and overrides
        // `count`/`inputs`; otherwise `count` instances each share `inputs`,
        // falling back to the registration's `initial_context` when `inputs`
        // is unset.
        #[allow(clippy::result_large_err, reason = "tonic forces this")]
        let build_call_spec = |initial_context: Option<&proto::WorkflowArguments>| {
            waymark_workflow_initialization_convert_proto::InitialContextConverter::try_convert((
                initial_context,
                &entry_input_names[..],
            ))
            .map_err(|err| Status::internal(format!("build entry call spec: {err}")))
        };

        let call_specs: NEVec<waymark_system_vm::CallSpec> =
            match NEVec::try_from_vec(request.inputs_list) {
                #[allow(clippy::result_large_err, reason = "tonic forces this")]
                Some(inputs_list) => inputs_list
                    .into_nonempty_iter()
                    .map(|inputs| build_call_spec(Some(&inputs)))
                    .collect::<Result<_, _>>()?,
                None => {
                    let Some(target_count) = NonZeroUsize::new(request.count as usize) else {
                        return Err(Status::invalid_argument(
                            "count must be >= 1 when inputs_list is empty",
                        ));
                    };

                    let base = request
                        .inputs
                        .as_ref()
                        .or(registration.initial_context.as_ref());
                    NEVec::from_elem(build_call_spec(base)?, target_count)
                }
            };

        let mut vm_ids = Vec::new();
        let include_ids = request.include_instance_ids;
        let queued = call_specs.len().get() as u32;

        for call_spec in call_specs {
            let vm_id = InstanceId::new_uuid_v4();
            if include_ids {
                vm_ids.push(vm_id.to_string());
            }

            store
                .register_vm_runtime(vm_id, executable_id, Arc::clone(&executable), call_spec)
                .await
                .map_err(|err| Status::internal(format!("register vm runtime: {err}")))?;
        }

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
        _request: Request<proto::RegisterScheduleRequest>,
    ) -> Result<Response<proto::RegisterScheduleResponse>, Status> {
        // Scheduling is currently inert: the scheduler loop that consumed
        // `find_due_schedules` was removed in the VM-execution rework and has
        // no replacement yet, so a persisted schedule would never fire. The
        // whole schedule API reports `Unimplemented` rather than silently
        // accepting writes or serving state that no scheduler acts on.
        Err(Status::unimplemented(
            "workflow scheduling is not currently supported",
        ))
    }

    async fn update_schedule_status(
        &self,
        _request: Request<proto::UpdateScheduleStatusRequest>,
    ) -> Result<Response<proto::UpdateScheduleStatusResponse>, Status> {
        Err(Status::unimplemented(
            "workflow scheduling is not currently supported",
        ))
    }

    async fn delete_schedule(
        &self,
        _request: Request<proto::DeleteScheduleRequest>,
    ) -> Result<Response<proto::DeleteScheduleResponse>, Status> {
        Err(Status::unimplemented(
            "workflow scheduling is not currently supported",
        ))
    }

    async fn list_schedules(
        &self,
        _request: Request<proto::ListSchedulesRequest>,
    ) -> Result<Response<proto::ListSchedulesResponse>, Status> {
        Err(Status::unimplemented(
            "workflow scheduling is not currently supported",
        ))
    }
}
