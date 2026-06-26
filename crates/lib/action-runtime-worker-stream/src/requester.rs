use tokio::sync::mpsc;
use waymark_action_core::ActionRef;
use waymark_action_runtime_core::ActionCallRequest;
use waymark_convert_core::TryConvert;
use waymark_proto::messages as proto;

/// Sends action dispatches as [`proto::WorkflowStreamResponse`] messages
/// on a tokio mpsc channel.
pub struct WorkerStreamActionRequester {
    /// Channel for sending workflow stream responses.
    pub tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, tonic::Status>>,

    /// Instance identifier included in each dispatch.
    pub instance_id: String,
}

impl waymark_action_runtime_core::ActionCallRequester for WorkerStreamActionRequester {
    type Error = mpsc::error::SendError<Result<proto::WorkflowStreamResponse, tonic::Status>>;

    type Argument = waymark_vm_value::ReadyValue;

    fn request_action_call(
        &self,
        request: ActionCallRequest<Self::Argument>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        let result = build_dispatch(&self.instance_id, request);
        async move { self.tx.send(result).await }
    }
}

#[allow(clippy::result_large_err)]
fn build_dispatch(
    instance_id: &str,
    request: ActionCallRequest<waymark_vm_value::ReadyValue>,
) -> Result<proto::WorkflowStreamResponse, tonic::Status> {
    let ActionCallRequest {
        action_ref,
        effect_number,
        promise_state_id,
        arguments,
    } = request;

    let ActionRef {
        action_name,
        module_name,
        call_args,
        timeout_seconds,
        max_retries,
        ..
    } = action_ref;

    let kwargs =
        waymark_action_runtime_convert::Converter::try_convert((&call_args[..], &arguments[..]))
            .map_err(|err| tonic::Status::internal(format!("action argument conversion: {err}")))?;

    let dispatch = proto::ActionDispatch {
        action_id: format!("{}/{:?}", effect_number, promise_state_id),
        instance_id: instance_id.to_owned(),
        sequence: u32::try_from(request.effect_number.0).unwrap_or(0),
        action_name: action_name.clone(),
        module_name: module_name.clone().unwrap_or_default(),
        kwargs,
        timeout_seconds: Some(timeout_seconds),
        max_retries: Some(max_retries),
        attempt_number: None,
        dispatch_token: None,
    };

    Ok(proto::WorkflowStreamResponse {
        kind: Some(proto::workflow_stream_response::Kind::ActionDispatch(
            dispatch,
        )),
    })
}
