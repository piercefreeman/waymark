use tokio::sync::mpsc;
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

    type Argument = waymark_vm_value_python::ReadyValue;

    type Metadata = Metadata;

    fn request_action_call(
        &self,
        request: ActionCallRequest<Self::Argument, Self::Metadata>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        // The conversion happens eagerly: a dispatch that cannot be built
        // is sent onto the stream as the error the caller sees.
        let result = waymark_action_runtime_convert::Converter::try_convert(request)
            .map(
                |dispatch: proto::ActionDispatch| proto::WorkflowStreamResponse {
                    kind: Some(proto::workflow_stream_response::Kind::ActionDispatch(
                        dispatch,
                    )),
                },
            )
            .map_err(|err| tonic::Status::internal(format!("action argument conversion: {err}")));

        async move { self.tx.send(result).await }
    }
}
