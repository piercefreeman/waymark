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
/// port the OS picked when `bind_addr` names port 0.
///
/// The server runs as the `http server` task on `spawner`; it ends when
/// `shutdown_signal` completes, or with the serve error.
pub async fn start<Spawner>(
    mut spawner: Spawner,
    bind_addr: SocketAddr,
    router: axum::Router,
    shutdown_signal: tokio_util::sync::WaitForCancellationFutureOwned,
) -> Result<SocketAddr, StartError>
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

    Ok(actual_addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use waymark_managed_spawner_supervised::SupervisorExt as _;

    #[tokio::test]
    async fn starts_on_an_ephemeral_port_and_returns_it() {
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let mut supervisor = waymark_managed_spawner_supervised::supervisor::start::<
            waymark_fn_main_common::Error,
        >(shutdown_token.clone());

        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr = start(
            supervisor.spawner(waymark_fn_main_common::ErrorConverter),
            bind_addr,
            axum::Router::new(),
            shutdown_token.clone().cancelled_owned(),
        )
        .await
        .unwrap();
        assert_eq!(addr.ip(), bind_addr.ip());
        assert!(addr.port() > 0);

        shutdown_token.cancel();

        let report = supervisor.drain().await;
        assert!(!report.any_before_shutdown(), "{report}");
    }
}
