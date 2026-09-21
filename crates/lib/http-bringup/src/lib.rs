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
/// The server runs as the `http server` task on `spawner`; it ends when
/// `shutdown_signal` completes, or with the serve error.
pub async fn start<Spawner>(
    mut spawner: Spawner,
    bind_addr: SocketAddr,
    router: axum::Router,
    shutdown_signal: tokio_util::sync::WaitForCancellationFutureOwned,
) -> Result<(), StartError>
where
    Spawner: waymark_managed_spawner::Spawner,
{
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|source| StartError::Bind { bind_addr, source })?;

    let actual_addr = listener.local_addr().map_err(StartError::LocalAddr)?;

    spawner.spawn("http server", async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal)
            .await
    });

    tracing::info!(addr = %actual_addr, "http server started");

    Ok(())
}
