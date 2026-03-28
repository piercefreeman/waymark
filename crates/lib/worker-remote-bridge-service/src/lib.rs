use std::{pin::Pin, sync::Arc};

use futures_core::Stream;
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming, async_trait};

use waymark_proto::messages as proto;

type Registry = waymark_worker_reservation::Registry<waymark_worker_message_protocol::Channels>;

/// gRPC service implementation for the WorkerBridge.
#[derive(Clone)]
pub struct WorkerBridgeService {
    pub workers_registry: Arc<Registry>,
}

#[async_trait]
impl proto::worker_bridge_server::WorkerBridge for WorkerBridgeService {
    type AttachStream =
        Pin<Box<dyn Stream<Item = Result<proto::Envelope, Status>> + Send + 'static>>;

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

        // Create channels for bidirectional communication
        // Buffer size of 64 provides reasonable backpressure while allowing
        // some pipelining of requests
        let (to_worker_tx, to_worker_rx) = mpsc::channel(64);
        let (from_worker_tx, from_worker_rx) = mpsc::channel(64);

        let reservation_id = waymark_worker_reservation::Id::from(worker_id);
        let channels = waymark_worker_message_protocol::Channels {
            to_worker: to_worker_tx,
            from_worker: from_worker_rx,
        };

        // Complete the registration - this unblocks the spawn code
        self.workers_registry
            .register(reservation_id, channels)
            .map_err(|err| tonic::Status::not_found(err.to_string()))?;

        // Spawn a task to read from the worker stream and forward to the channel
        // TODO: move this into the `waymark_worker_message_protocol` and
        // drop `rt` feature from `tokio`.
        tokio::spawn(async move {
            loop {
                let envelope = match stream.message().await {
                    Ok(Some(envelope)) => envelope,
                    Ok(None) => {
                        // Stream closed cleanly
                        tracing::info!(worker_id, "worker stream closed");
                        break;
                    }
                    Err(err) => {
                        tracing::warn!(?err, worker_id, "worker stream receive error");
                        break;
                    }
                };

                if from_worker_tx.send(envelope).await.is_err() {
                    // Receiver dropped, worker shutting down
                    break;
                }
            }
        });

        // Return a stream that sends from to_worker_rx to the Python client
        let outbound = ReceiverStream::new(to_worker_rx).map(Ok::<proto::Envelope, Status>);
        Ok(Response::new(Box::pin(outbound) as Self::AttachStream))
    }
}
