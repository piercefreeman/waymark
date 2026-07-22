use tokio::sync::mpsc;
use waymark_action_core::ActionRef;
use waymark_action_runtime_core::ActionCallRequest;
use waymark_action_runtime_metadata_codec::Encode;
use waymark_convert_core::TryConvert;
use waymark_proto::messages as proto;

/// Sends action dispatches as [`proto::WorkflowStreamResponse`] messages
/// on a tokio mpsc channel.
pub struct WorkerStreamActionRequester<Metadata> {
    /// Channel for sending workflow stream responses.
    pub tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, tonic::Status>>,

    /// Phantom data for the metadata type parameter.
    _metadata: core::marker::PhantomData<fn() -> Metadata>,
}

impl<Metadata> WorkerStreamActionRequester<Metadata> {
    /// Create a new requester.
    pub fn new(tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, tonic::Status>>) -> Self {
        Self {
            tx,
            _metadata: core::marker::PhantomData,
        }
    }
}

impl<Metadata> waymark_action_runtime_core::ActionCallRequester
    for WorkerStreamActionRequester<Metadata>
where
    Metadata: Encode + Send + 'static,
{
    type Error = mpsc::error::SendError<Result<proto::WorkflowStreamResponse, tonic::Status>>;

    type Argument = waymark_vm_value::ReadyValue;

    type Metadata = Metadata;

    fn request_action_call(
        &self,
        request: ActionCallRequest<Self::Argument, Self::Metadata>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        let result = build_dispatch(request);
        async move { self.tx.send(result).await }
    }
}

#[allow(clippy::result_large_err)]
fn build_dispatch<Metadata: Encode>(
    request: ActionCallRequest<waymark_vm_value::ReadyValue, Metadata>,
) -> Result<proto::WorkflowStreamResponse, tonic::Status> {
    let ActionCallRequest {
        action_ref,
        metadata,
        arguments,
    } = request;

    let ActionRef {
        action_name,
        module_name,
        call_args,
    } = action_ref;

    let kwargs =
        waymark_action_runtime_convert::Converter::try_convert((&call_args[..], &arguments[..]))
            .map_err(|err| tonic::Status::internal(format!("action argument conversion: {err}")))?;

    let mut encoded_metadata = Vec::new();
    metadata.encode(&mut encoded_metadata);

    let dispatch = proto::ActionDispatch {
        action_id: String::new(),
        instance_id: String::new(),
        sequence: 0,
        action_name: action_name.clone(),
        module_name: module_name.clone().unwrap_or_default(),
        kwargs,
        timeout_seconds: None,
        max_retries: None,
        attempt_number: None,
        dispatch_token: None,
        metadata: encoded_metadata,
    };

    Ok(proto::WorkflowStreamResponse {
        kind: Some(proto::workflow_stream_response::Kind::ActionDispatch(
            dispatch,
        )),
    })
}
