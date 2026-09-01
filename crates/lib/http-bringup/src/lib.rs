//! Bringup for the HTTP server: the listener and the serve loop.
//!
//! Owns nothing but the socket. Everything served comes in as the
//! [`axum::Router`] argument.

use std::net::SocketAddr;

/// Error returned when starting the HTTP server fails.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// Binding the listener failed.
    #[error("bind http listener on {bind_addr}: {source}")]
    Bind {
        /// The address the listener was binding to.
        bind_addr: SocketAddr,

        /// The underlying bind error.
        #[source]
        source: std::io::Error,
    },

    /// Reading the bound listener address failed.
    #[error("read http listener local address: {0}")]
    LocalAddr(#[source] std::io::Error),
}

/// Start the HTTP server.
///
/// Returns the address the listener bound: `bind_addr` itself, or the
/// port the OS picked when `bind_addr` names port 0. The task serving it
/// comes with it.
pub async fn start(
    bind_addr: SocketAddr,
    router: axum::Router,
    shutdown_signal: tokio_util::sync::WaitForCancellationFutureOwned,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), StartError> {
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|source| StartError::Bind { bind_addr, source })?;

    let actual_addr = listener.local_addr().map_err(StartError::LocalAddr)?;

    let task = tokio::spawn(async move {
        let result = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal)
            .await;
        if let Err(error) = result {
            tracing::error!(?error, "http server failed");
        }
    });

    tracing::info!(addr = %actual_addr, "http server started");

    Ok((actual_addr, task))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn starts_on_an_ephemeral_port_and_returns_it() {
        let shutdown_token = tokio_util::sync::CancellationToken::new();

        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (addr, task) = start(
            bind_addr,
            axum::Router::new(),
            shutdown_token.clone().cancelled_owned(),
        )
        .await
        .unwrap();
        assert_eq!(addr.ip(), bind_addr.ip());
        assert!(addr.port() > 0);

        shutdown_token.cancel();
        task.await.expect("http server task");
    }
}
