use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use tokio::{net::TcpListener, task::JoinHandle};
use tonic::transport::Server;
use tracing::{error, info};

use waymark_proto::messages as proto;

type Registry = waymark_worker_reservation::Registry<waymark_worker_message_protocol::Channels>;

/// Start the worker bridge server.
///
/// If `bind_addr` is None, binds to localhost on an ephemeral port.
/// The actual bound address can be retrieved with [`Self::addr`].
pub async fn start(
    shutdown_token: tokio_util::sync::CancellationToken,
    workers_registry: Arc<Registry>,
    bind_addr: Option<SocketAddr>,
) -> Result<(SocketAddr, JoinHandle<()>), std::io::Error> {
    let bind_addr =
        bind_addr.unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0));

    // TODO: annotoate errors via custom error type.
    let listener = TcpListener::bind(bind_addr).await?;

    let addr = listener.local_addr()?;

    info!(%addr, "worker bridge server starting");

    let service = waymark_worker_remote_bridge_service::WorkerBridgeService { workers_registry };

    let task = tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let result = Server::builder()
            .add_service(proto::worker_bridge_server::WorkerBridgeServer::new(
                service,
            ))
            .serve_with_incoming_shutdown(incoming, shutdown_token.cancelled())
            .await;
        if let Err(err) = result {
            error!(?err, "worker bridge server exited with error");
        }
    });

    Ok((addr, task))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_starts_and_binds() {
        let shutdown_token = tokio_util::sync::CancellationToken::new();

        let registry = Default::default();

        let (addr, task) = start(shutdown_token.clone(), registry, None).await.unwrap();
        assert!(addr.port() > 0);

        shutdown_token.cancel();

        task.await.unwrap();
    }
}
