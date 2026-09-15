use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::info;

use waymark_proto::messages as proto;

type Registry = waymark_worker_reservation::Registry<waymark_worker_message_protocol::Channels>;

/// Start the worker bridge server.
///
/// If `bind_addr` is None, binds to localhost on an ephemeral port.
/// The actual bound address is returned.
pub async fn start<Spawner>(
    mut spawner: Spawner,
    shutdown_token: tokio_util::sync::CancellationToken,
    workers_registry: Arc<Registry>,
    bind_addr: Option<SocketAddr>,
) -> Result<SocketAddr, std::io::Error>
where
    Spawner: waymark_managed_spawner::Spawner,
{
    let bind_addr =
        bind_addr.unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));

    // TODO: annotoate errors via custom error type.
    let listener = TcpListener::bind(bind_addr).await?;

    let addr = listener.local_addr()?;

    info!(%addr, "worker bridge server starting");

    let service = waymark_worker_remote_bridge_service::WorkerBridgeService { workers_registry };

    spawner.spawn("worker bridge server", async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let worker_bridge_service = proto::worker_bridge_server::WorkerBridgeServer::new(service)
            .max_decoding_message_size(waymark_proto::GRPC_MAX_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(waymark_proto::GRPC_MAX_MESSAGE_SIZE_BYTES);

        Server::builder()
            .add_service(worker_bridge_service)
            .serve_with_incoming_shutdown(incoming, shutdown_token.cancelled())
            .await
    });

    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use waymark_managed_spawner_supervised::SupervisorExt as _;

    #[tokio::test]
    async fn test_server_starts_and_binds() {
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let mut supervisor = waymark_managed_spawner_supervised::supervisor::start::<
            waymark_fn_main_common::Error,
        >(shutdown_token.clone());

        let registry = Default::default();

        let addr = start(
            supervisor.spawner(waymark_fn_main_common::ErrorConverter),
            shutdown_token.clone(),
            registry,
            None,
        )
        .await
        .unwrap();
        assert!(addr.port() > 0);

        shutdown_token.cancel();

        let report = supervisor.drain().await;
        assert!(!report.any_before_shutdown(), "{report}");
    }
}
