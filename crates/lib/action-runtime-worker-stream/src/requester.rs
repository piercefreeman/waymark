use tokio::sync::mpsc;
use waymark_action_runtime_core::ActionCallRequest;
use waymark_action_runtime_metadata_codec::Encode;
use waymark_convert_core::TryConvert;
use waymark_proto::messages as proto;

/// Sends action dispatches as [`proto::WorkflowStreamResponse`] messages
/// on a tokio mpsc channel.
pub struct WorkerStreamActionRequester<Metadata, Argument, ValueConverter> {
    /// Channel for sending workflow stream responses.
    pub tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, tonic::Status>>,

    /// Phantom data for the type parameters the requester only relays,
    /// kept out of the struct's auto-trait story: a fn pointer is
    /// `Send + Sync` no matter what it mentions.
    #[allow(clippy::type_complexity, reason = "a phantom, not a used type")]
    _phantom: core::marker::PhantomData<fn() -> (Metadata, Argument, ValueConverter)>,
}

impl<Metadata, Argument, ValueConverter>
    WorkerStreamActionRequester<Metadata, Argument, ValueConverter>
{
    /// Create a new requester.
    pub fn new(tx: mpsc::Sender<Result<proto::WorkflowStreamResponse, tonic::Status>>) -> Self {
        Self {
            tx,
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<Metadata, Argument, ValueConverter> waymark_action_runtime_core::ActionCallRequester
    for WorkerStreamActionRequester<Metadata, Argument, ValueConverter>
where
    Metadata: Encode + Send + 'static,
    Argument: Send,
    ValueConverter: Send,
    waymark_action_runtime_convert::Converter<ValueConverter>:
        TryConvert<ActionCallRequest<Argument, Metadata>, proto::ActionDispatch>,
    DispatchConvertErrorFor<ValueConverter, Argument, Metadata>: core::fmt::Display,
{
    type Error = mpsc::error::SendError<Result<proto::WorkflowStreamResponse, tonic::Status>>;

    type Argument = Argument;

    type Metadata = Metadata;

    fn request_action_call(
        &self,
        request: ActionCallRequest<Self::Argument, Self::Metadata>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_ {
        // The conversion happens eagerly: a dispatch that cannot be built
        // is sent onto the stream as the error the caller sees.
        let result =
            waymark_action_runtime_convert::Converter::<ValueConverter>::try_convert(request)
                .map(
                    |dispatch: proto::ActionDispatch| proto::WorkflowStreamResponse {
                        kind: Some(proto::workflow_stream_response::Kind::ActionDispatch(
                            dispatch,
                        )),
                    },
                )
                .map_err(|err| {
                    tonic::Status::internal(format!("action argument conversion: {err}"))
                });

        async move { self.tx.send(result).await }
    }
}

/// The error of the request-to-dispatch conversion the requester
/// delegates to, expressed as a projection through the action runtime's
/// converter.
pub type DispatchConvertErrorFor<ValueConverter, Argument, Metadata> =
    waymark_convert_core::ConvertErrorFor<
        waymark_action_runtime_convert::Converter<ValueConverter>,
        ActionCallRequest<Argument, Metadata>,
        proto::ActionDispatch,
    >;
