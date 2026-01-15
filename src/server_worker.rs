//! gRPC server for Python worker connections.
//!
//! The [`WorkerBridgeServer`] provides a bidirectional streaming gRPC service
//! that Python workers connect to. The protocol works as follows:
//!
//! 1. Rust spawns a worker process and reserves a worker ID
//! 2. Python worker connects to the bridge and sends a `WorkerHello` with its ID
//! 3. The bridge matches the connection to the reservation and establishes channels
//! 4. Bidirectional streaming begins: Rust sends actions, Python returns results

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result as AnyResult};
use futures::Stream;
use prost::Message;
use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::{
    StreamExt,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{Request, Response, Status, Streaming, async_trait, transport::Server};
use tracing::{error, info, warn};

use crate::messages::proto;

/// Channels for communicating with a connected worker.
/// Created when a worker successfully completes the handshake.
pub struct WorkerBridgeChannels {
    /// Send actions to the worker
    pub to_worker: mpsc::Sender<proto::Envelope>,
    /// Receive results from the worker
    pub from_worker: mpsc::Receiver<proto::Envelope>,
}

/// Internal state for pending worker connections.
/// Workers are reserved before they connect, so we can correlate
/// the connection with the spawned process.
struct WorkerBridgeState {
    /// Map of worker_id -> channel sender for completing the handshake
    pending: Mutex<HashMap<u64, oneshot::Sender<WorkerBridgeChannels>>>,
}

impl WorkerBridgeState {
    fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Reserve a slot for an incoming worker connection.
    /// Returns a receiver that will be signaled when the worker connects.
    async fn reserve_worker(&self, worker_id: u64) -> oneshot::Receiver<WorkerBridgeChannels> {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.pending.lock().await;
        guard.insert(worker_id, tx);
        rx
    }

    /// Cancel a pending worker reservation (e.g., if spawn fails).
    async fn cancel_worker(&self, worker_id: u64) {
        let mut guard = self.pending.lock().await;
        guard.remove(&worker_id);
    }

    /// Complete the worker registration after receiving the handshake.
    async fn register_worker(
        &self,
        worker_id: u64,
        channels: WorkerBridgeChannels,
    ) -> Result<(), Status> {
        let sender = {
            let mut guard = self.pending.lock().await;
            guard.remove(&worker_id)
        };
        match sender {
            Some(waiter) => waiter
                .send(channels)
                .map_err(|_| Status::unavailable("worker reservation dropped")),
            None => Err(Status::failed_precondition(format!(
                "unknown worker id: {}",
                worker_id
            ))),
        }
    }
}

/// gRPC service implementation for the WorkerBridge.
#[derive(Clone)]
struct WorkerBridgeService {
    state: Arc<WorkerBridgeState>,
}

impl WorkerBridgeService {
    fn new(state: Arc<WorkerBridgeState>) -> Self {
        Self { state }
    }
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
        info!(worker_id, "worker connected and sent hello");

        // Create channels for bidirectional communication
        // Buffer size of 64 provides reasonable backpressure while allowing
        // some pipelining of requests
        let (to_worker_tx, to_worker_rx) = mpsc::channel(64);
        let (from_worker_tx, from_worker_rx) = mpsc::channel(64);

        // Complete the registration - this unblocks the spawn code
        self.state
            .register_worker(
                worker_id,
                WorkerBridgeChannels {
                    to_worker: to_worker_tx,
                    from_worker: from_worker_rx,
                },
            )
            .await?;

        // Spawn a task to read from the worker stream and forward to the channel
        let reader_state = Arc::clone(&self.state);
        tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(envelope)) => {
                        if from_worker_tx.send(envelope).await.is_err() {
                            // Receiver dropped, worker shutting down
                            break;
                        }
                    }
                    Ok(None) => {
                        // Stream closed cleanly
                        info!(worker_id, "worker stream closed");
                        break;
                    }
                    Err(err) => {
                        warn!(?err, worker_id, "worker stream receive error");
                        break;
                    }
                }
            }
            // Clean up the pending map in case of reconnection attempts
            reader_state.cancel_worker(worker_id).await;
        });

        // Return a stream that sends from to_worker_rx to the Python client
        let outbound = ReceiverStream::new(to_worker_rx).map(Ok::<proto::Envelope, Status>);
        Ok(Response::new(Box::pin(outbound) as Self::AttachStream))
    }
}

/// gRPC server for worker connections.
///
/// Workers connect via bidirectional streaming and exchange action dispatch
/// and result messages. The server handles:
///
/// - Worker handshake and registration
/// - Channel creation for message passing
/// - Graceful shutdown
///
/// # Example
///
/// ```ignore
/// let bridge = WorkerBridgeServer::start(None).await?;
/// let (worker_id, connection_rx) = bridge.reserve_worker().await;
/// // ... spawn worker process with worker_id ...
/// let channels = connection_rx.await?; // Wait for worker to connect
/// ```
pub struct WorkerBridgeServer {
    addr: SocketAddr,
    state: Arc<WorkerBridgeState>,
    next_worker_id: AtomicU64,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    server_handle: Mutex<Option<JoinHandle<()>>>,
}

impl WorkerBridgeServer {
    /// Start the worker bridge server.
    ///
    /// If `bind_addr` is None, binds to localhost on an ephemeral port.
    /// The actual bound address can be retrieved with [`Self::addr`].
    pub async fn start(bind_addr: Option<SocketAddr>) -> AnyResult<Arc<Self>> {
        let bind_addr =
            bind_addr.unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));

        let listener = TcpListener::bind(bind_addr)
            .await
            .context("failed to bind worker bridge listener")?;

        let addr = listener
            .local_addr()
            .context("failed to resolve bridge addr")?;

        info!(%addr, "worker bridge server starting");

        let state = Arc::new(WorkerBridgeState::new());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let service = WorkerBridgeService::new(Arc::clone(&state));
        let server = tokio::spawn(async move {
            let incoming = TcpListenerStream::new(listener);
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            let result = Server::builder()
                .add_service(proto::worker_bridge_server::WorkerBridgeServer::new(
                    service,
                ))
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await;
            if let Err(err) = result {
                error!(?err, "worker bridge server exited with error");
            }
        });

        Ok(Arc::new(Self {
            addr,
            state,
            next_worker_id: AtomicU64::new(0),
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            server_handle: Mutex::new(Some(server)),
        }))
    }

    /// Get the address the server is bound to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Reserve a worker ID and get a receiver for when the worker connects.
    ///
    /// Call this before spawning a worker process. Pass the worker_id to the
    /// process, then await the receiver to get the communication channels
    /// once the worker connects and completes the handshake.
    pub async fn reserve_worker(&self) -> (u64, oneshot::Receiver<WorkerBridgeChannels>) {
        let worker_id = self.next_worker_id.fetch_add(1, Ordering::SeqCst);
        let rx = self.state.reserve_worker(worker_id).await;
        (worker_id, rx)
    }

    /// Cancel a pending worker reservation.
    ///
    /// Call this if the worker process failed to spawn.
    pub async fn cancel_worker(&self, worker_id: u64) {
        self.state.cancel_worker(worker_id).await;
    }

    /// Gracefully shut down the server.
    pub async fn shutdown(&self) {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.server_handle.lock().await.take()
            && let Err(err) = handle.await
        {
            warn!(?err, "worker bridge task join failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_starts_and_binds() {
        let server = WorkerBridgeServer::start(None).await.expect("start server");
        assert!(server.addr().port() > 0);
        server.shutdown().await;
    }

    #[tokio::test]
    async fn test_reserve_worker_ids_increment() {
        let server = WorkerBridgeServer::start(None).await.expect("start server");
        let (id1, _) = server.reserve_worker().await;
        let (id2, _) = server.reserve_worker().await;
        let (id3, _) = server.reserve_worker().await;
        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
        server.shutdown().await;
    }
}
