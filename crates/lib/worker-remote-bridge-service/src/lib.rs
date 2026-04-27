use std::{pin::Pin, sync::Arc};

use async_stream::try_stream;
use futures_core::Stream;
use prost::Message;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming, async_trait};
use tracing_futures::Instrument;

use waymark_proto::messages as proto;

type Registry = waymark_worker_reservation::Registry<waymark_worker_message_protocol::Channels>;

/// Buffer size of 64 provides reasonable backpressure while allowing
/// some pipelining of requests.
const MESSAGE_PROTOCOL_CHANNEL_SIZE: usize = 64;

/// gRPC service implementation for the WorkerBridge.
#[derive(Clone)]
pub struct WorkerBridgeService {
    pub workers_registry: Arc<Registry>,
}

type BridgeAttachStream =
    Pin<Box<dyn Stream<Item = Result<proto::Envelope, Status>> + Send + 'static>>;

#[async_trait]
impl proto::worker_bridge_server::WorkerBridge for WorkerBridgeService {
    type AttachStream = BridgeAttachStream;

    async fn attach(
        &self,
        request: Request<Streaming<proto::Envelope>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let mut stream = request.into_inner();

        // Read and validate the handshake message
        let handshake = stream
            .message()
            .await
            .map_err(|err| Status::internal(format!("failed to read handshake: {err}")))?
            .ok_or_else(|| Status::invalid_argument("missing worker handshake"))?;

        let kind = proto::MessageKind::try_from(handshake.kind)
            .map_err(|_| Status::invalid_argument("invalid message kind"))?;

        if kind != proto::MessageKind::WorkerHello {
            return Err(Status::failed_precondition(
                "expected WorkerHello as first message",
            ));
        }

        let hello = proto::WorkerHello::decode(&*handshake.payload).map_err(|err| {
            Status::invalid_argument(format!("invalid WorkerHello payload: {err}"))
        })?;

        let worker_id = hello.worker_id;
        tracing::info!(worker_id, "worker connected and sent hello");

        // Create channels for bidirectional communication.
        let (to_worker_tx, to_worker_rx) = mpsc::channel(MESSAGE_PROTOCOL_CHANNEL_SIZE);
        let (from_worker_tx, from_worker_rx) = mpsc::channel(MESSAGE_PROTOCOL_CHANNEL_SIZE);

        let reservation_id = waymark_worker_reservation::Id::from(worker_id);
        let channels = waymark_worker_message_protocol::Channels {
            to_worker: to_worker_tx,
            from_worker: from_worker_rx,
        };

        // Complete the registration - this unblocks the spawn code
        self.workers_registry
            .register(reservation_id, channels)
            .map_err(|err| tonic::Status::not_found(err.to_string()))?;

        let worker_span = tracing::info_span!("worker_bridge_attach", worker_id);

        let inbound = pipe_inbound_messages(stream, from_worker_tx);
        let outbound = stream_outbound_messages(inbound, to_worker_rx).instrument(worker_span);

        let outbound = Box::pin(outbound) as Self::AttachStream;
        Ok(Response::new(outbound))
    }
}

async fn pipe_inbound_messages(
    mut stream: Streaming<proto::Envelope>,
    from_worker_tx: mpsc::Sender<proto::Envelope>,
) {
    loop {
        let result = stream.message().await;
        let maybe_envelope = match result {
            Ok(val) => val,
            Err(error) => {
                tracing::warn!(?error, "worker stream receive error");
                break;
            }
        };

        let Some(envelope) = maybe_envelope else {
            tracing::info!("worker stream closed");
            break;
        };

        if from_worker_tx.send(envelope).await.is_err() {
            tracing::debug!("from worker channel closed");
            break;
        }
    }
}

fn stream_outbound_messages(
    inbound_pipe_fut: impl Future<Output = ()>,
    mut to_worker_rx: mpsc::Receiver<proto::Envelope>,
) -> impl Stream<Item = Result<proto::Envelope, Status>> {
    try_stream! {
        let mut pipe_inbound = true;
        let mut inbound_pipe_fut = std::pin::pin!(inbound_pipe_fut);

        loop {
            tokio::select! {
                () = &mut inbound_pipe_fut, if pipe_inbound => {
                    pipe_inbound = false;
                }
                Some(envelope) = to_worker_rx.recv() => {
                    yield envelope;
                }
                else => {
                    tracing::debug!("to worker channel closed");
                    break;
                }
            }
        }
    }
}
