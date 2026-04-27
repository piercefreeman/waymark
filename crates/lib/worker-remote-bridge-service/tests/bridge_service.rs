use std::{net::SocketAddr, sync::Arc, time::Duration};

use prost::Message;
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::{Code, Request, transport::Server};
use waymark_proto::messages as proto;
use waymark_worker_remote_bridge_service::WorkerBridgeService;

type Registry = waymark_worker_reservation::Registry<waymark_worker_message_protocol::Channels>;

struct TestServer {
    addr: SocketAddr,
    shutdown_guard: Option<DropGuard>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let Some(shutdown_guard) = self.shutdown_guard.take() else {
            return;
        };

        let Some(server_handle) = self.server_handle.take() else {
            drop(shutdown_guard);
            return;
        };

        if let Ok(runtime_handle) = tokio::runtime::Handle::try_current() {
            runtime_handle.spawn(async move {
                drop(shutdown_guard);
                let _ = server_handle.await;
            });
        } else {
            drop(shutdown_guard);
            server_handle.abort();
        }
    }
}

fn worker_hello_envelope(worker_id: u64) -> proto::Envelope {
    proto::Envelope {
        delivery_id: 0,
        partition_id: 0,
        kind: proto::MessageKind::WorkerHello as i32,
        payload: proto::WorkerHello { worker_id }.encode_to_vec(),
    }
}

fn heartbeat_envelope(delivery_id: u64) -> proto::Envelope {
    proto::Envelope {
        delivery_id,
        partition_id: 0,
        kind: proto::MessageKind::Heartbeat as i32,
        payload: Vec::new(),
    }
}

async fn start_test_server(workers_registry: Arc<Registry>) -> TestServer {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind test worker bridge listener");
    let addr = listener.local_addr().expect("resolve test bridge addr");
    let shutdown_token = CancellationToken::new();
    let shutdown_guard = shutdown_token.clone().drop_guard();

    let service = WorkerBridgeService { workers_registry };
    let server_shutdown = shutdown_token.clone();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::worker_bridge_server::WorkerBridgeServer::new(
                service,
            ))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                server_shutdown.cancelled_owned(),
            )
            .await
            .expect("test worker bridge server should shut down cleanly");
    });

    TestServer {
        addr,
        shutdown_guard: Some(shutdown_guard),
        server_handle: Some(server_handle),
    }
}

async fn connect_client(
    addr: SocketAddr,
) -> proto::worker_bridge_client::WorkerBridgeClient<tonic::transport::Channel> {
    proto::worker_bridge_client::WorkerBridgeClient::connect(format!("http://{addr}"))
        .await
        .expect("connect test worker bridge client")
}

#[tokio::test]
async fn attach_registers_reserved_worker_and_bridges_messages() {
    let registry = Arc::new(Registry::default());
    let server = start_test_server(Arc::clone(&registry)).await;

    let reservation = registry.reserve();
    let worker_id = u64::from(reservation.id());

    let mut client = connect_client(server.addr).await;
    let (request_tx, request_rx) = mpsc::channel(4);
    request_tx
        .send(worker_hello_envelope(worker_id))
        .await
        .expect("queue handshake envelope");

    let response = client
        .attach(Request::new(ReceiverStream::new(request_rx)))
        .await
        .expect("attach should succeed for a reserved worker");
    let mut outbound = response.into_inner();

    let mut channels = timeout(Duration::from_secs(1), reservation.wait())
        .await
        .expect("reservation registration timed out")
        .expect("reservation should receive bridge channels");

    let outbound_envelope = heartbeat_envelope(11);
    channels
        .to_worker
        .send(outbound_envelope.clone())
        .await
        .expect("send envelope to bridged worker");

    let received_by_client = timeout(Duration::from_secs(1), outbound.message())
        .await
        .expect("client receive timed out")
        .expect("outbound stream should stay healthy")
        .expect("client should receive outbound envelope");
    assert_eq!(received_by_client, outbound_envelope);

    let inbound_envelope = heartbeat_envelope(22);
    request_tx
        .send(inbound_envelope.clone())
        .await
        .expect("send inbound envelope from worker");

    let received_by_registry = timeout(Duration::from_secs(1), channels.from_worker.recv())
        .await
        .expect("registry receive timed out")
        .expect("registry should receive forwarded envelope");
    assert_eq!(received_by_registry, inbound_envelope);
}

#[tokio::test]
async fn attach_rejects_missing_handshake() {
    let registry = Arc::new(Registry::default());
    let server = start_test_server(registry).await;

    let mut client = connect_client(server.addr).await;
    let (request_tx, request_rx) = mpsc::channel(1);
    drop(request_tx);

    let err = client
        .attach(Request::new(ReceiverStream::new(request_rx)))
        .await
        .expect_err("empty stream must be rejected");

    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(err.message(), "missing worker handshake");
}

#[tokio::test]
async fn attach_rejects_non_hello_first_message() {
    let registry = Arc::new(Registry::default());
    let server = start_test_server(registry).await;

    let mut client = connect_client(server.addr).await;
    let (request_tx, request_rx) = mpsc::channel(1);
    request_tx
        .send(heartbeat_envelope(1))
        .await
        .expect("queue non-hello handshake envelope");
    drop(request_tx);

    let err = client
        .attach(Request::new(ReceiverStream::new(request_rx)))
        .await
        .expect_err("non-hello handshake must be rejected");

    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(err.message(), "expected WorkerHello as first message");
}

#[tokio::test]
async fn attach_rejects_unreserved_worker_id() {
    let registry = Arc::new(Registry::default());
    let server = start_test_server(registry).await;

    let mut client = connect_client(server.addr).await;
    let (request_tx, request_rx) = mpsc::channel(1);
    request_tx
        .send(worker_hello_envelope(42))
        .await
        .expect("queue handshake for unreserved worker");
    drop(request_tx);

    let err = client
        .attach(Request::new(ReceiverStream::new(request_rx)))
        .await
        .expect_err("unreserved worker must be rejected");

    assert_eq!(err.code(), Code::NotFound);
    assert!(err.message().contains("not found"));
}
