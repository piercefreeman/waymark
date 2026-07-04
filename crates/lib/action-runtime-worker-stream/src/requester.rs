use tokio::sync::mpsc;
use waymark_action_core::ActionRef;
use waymark_action_runtime_core::{ActionCallRequest, ActionCallRequestFor};
use waymark_convert_core::TryConvert;
use waymark_proto::messages as proto;

/// Sends action dispatches as [`proto::WorkflowStreamResponse`] messages
/// on a tokio mpsc channel.
pub struct WorkerStreamActionRequester<Metadata> {
    /// Channel for sending workflow stream responses.
    pub tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, tonic::Status>>,

    /// The phantom data.
    pub phantom_data: std::marker::PhantomData<Metadata>,
}

impl<Metadata> WorkerStreamActionRequester<Metadata> {
    /// Create a new [`WorkerStreamActionRequester`].
    pub fn new(tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, tonic::Status>>) -> Self {
        Self {
            tx,
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<Metadata> waymark_action_runtime_core::WithActionCallMetadata
    for WorkerStreamActionRequester<Metadata>
{
    type ActionCallMetadata = Metadata;
}

impl<Metadata> waymark_action_runtime_core::ActionCallRequester
    for WorkerStreamActionRequester<Metadata>
where
    (String, String, u32): From<Metadata>,
    Metadata: std::marker::Sync,
{
    type Error = mpsc::error::SendError<Result<proto::WorkflowStreamResponse, tonic::Status>>;

    type Argument = waymark_vm_value::ReadyValue;

    fn request_action_call(
        &self,
        request: ActionCallRequestFor<Self>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        let result = build_dispatch(request);
        async move { self.tx.send(result).await }
    }
}

#[allow(clippy::result_large_err)]
fn build_dispatch<Metadata>(
    request: ActionCallRequest<waymark_vm_value::ReadyValue, Metadata>,
) -> Result<proto::WorkflowStreamResponse, tonic::Status>
where
    (String, String, u32): From<Metadata>,
{
    let ActionCallRequest {
        action_ref,
        arguments,
        metadata,
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

    let (action_id, instance_id, sequence) = metadata.into();

    let dispatch = proto::ActionDispatch {
        action_id,
        instance_id,
        sequence,
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
