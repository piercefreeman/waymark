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
use tracing::{error, warn};

use crate::messages::proto;

pub struct WorkerBridgeChannels {
    pub to_worker: mpsc::Sender<proto::Envelope>,
    pub from_worker: mpsc::Receiver<proto::Envelope>,
}

struct WorkerBridgeState {
    pending: Mutex<HashMap<u64, oneshot::Sender<WorkerBridgeChannels>>>,
}

impl WorkerBridgeState {
    fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    async fn reserve_worker(&self, worker_id: u64) -> oneshot::Receiver<WorkerBridgeChannels> {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.pending.lock().await;
        guard.insert(worker_id, tx);
        rx
    }

    async fn cancel_worker(&self, worker_id: u64) {
        let mut guard = self.pending.lock().await;
        guard.remove(&worker_id);
    }

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
            None => Err(Status::failed_precondition("unknown worker id")),
        }
    }
}

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
        let (to_worker_tx, to_worker_rx) = mpsc::channel(64);
        let (from_worker_tx, from_worker_rx) = mpsc::channel(64);
        self.state
            .register_worker(
                worker_id,
                WorkerBridgeChannels {
                    to_worker: to_worker_tx,
                    from_worker: from_worker_rx,
                },
            )
            .await?;

        let reader_state = Arc::clone(&self.state);
        tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(envelope)) => {
                        if from_worker_tx.send(envelope).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(?err, worker_id, "worker stream receive error");
                        break;
                    }
                }
            }
            reader_state.cancel_worker(worker_id).await;
        });

        let outbound = ReceiverStream::new(to_worker_rx).map(Ok::<proto::Envelope, Status>);
        Ok(Response::new(Box::pin(outbound) as Self::AttachStream))
    }
}

pub struct WorkerBridgeServer {
    addr: SocketAddr,
    state: Arc<WorkerBridgeState>,
    next_worker_id: AtomicU64,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    server_handle: Mutex<Option<JoinHandle<()>>>,
}

impl WorkerBridgeServer {
    pub async fn start(bind_addr: Option<SocketAddr>) -> AnyResult<Arc<Self>> {
        let bind_addr =
            bind_addr.unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
        let listener = TcpListener::bind(bind_addr)
            .await
            .context("failed to bind worker bridge listener")?;
        let addr = listener
            .local_addr()
            .context("failed to resolve bridge addr")?;
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
            next_worker_id: AtomicU64::new(1),
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            server_handle: Mutex::new(Some(server)),
        }))
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn reserve_worker(&self) -> (u64, oneshot::Receiver<WorkerBridgeChannels>) {
        let worker_id = self.next_worker_id.fetch_add(1, Ordering::SeqCst);
        let rx = self.state.reserve_worker(worker_id).await;
        (worker_id, rx)
    }

    pub async fn cancel_worker(&self, worker_id: u64) {
        self.state.cancel_worker(worker_id).await;
    }

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
