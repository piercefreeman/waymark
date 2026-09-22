//! Bringup for the HTTP server: the listener and the serve loop.
//!
//! Owns nothing but the socket. Everything served comes in as the
//! [`axum::Router`] argument.

use std::net::SocketAddr;

/// [`StartError`] with the task label [`start`] was called with.
#[derive(Debug, thiserror::Error)]
#[error("{task_name}: {error}")]
pub struct NamedStartError {
    /// The task label [`start`] was called with.
    pub task_name: &'static str,

    /// What failed.
    #[source]
    pub error: StartError,
}

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
/// The server runs on `spawner` as the `task_name` task; it ends when
/// `shutdown_signal` completes, or with the serve error.
pub async fn start<Spawner>(
    mut spawner: Spawner,
    task_name: &'static str,
    bind_addr: SocketAddr,
    router: axum::Router,
    shutdown_signal: tokio_util::sync::WaitForCancellationFutureOwned,
) -> Result<SocketAddr, NamedStartError>
where
    Spawner: waymark_managed_spawner::Spawner,
{
    let named = |error| NamedStartError { task_name, error };

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|source| StartError::Bind { bind_addr, source })
        .map_err(named)?;

    let actual_addr = listener
        .local_addr()
        .map_err(StartError::LocalAddr)
        .map_err(named)?;

    spawner.spawn(task_name, async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal)
            .await
    });

    tracing::info!(task_name, addr = %actual_addr, "http server started");

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
            waymark_eyre_error::ReportError,
        >(shutdown_token.clone());

        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr = start(
            supervisor.spawner(waymark_fn_main_common::ErrorConverter),
            "test http server",
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
