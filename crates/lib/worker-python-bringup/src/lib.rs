//! Bringup for the Python worker bridge + worker pool.

use std::{
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
};

use tokio::task::JoinHandle;
use waymark_worker_python::{Config, ResolveError, Spec};

/// Errors returned by [`start`].
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// Failed to resolve the Python worker configuration.
    #[error("resolve python worker config: {0}")]
    Resolve(#[source] ResolveError),

    /// Failed to bring up the bridge + worker pool.
    #[error("start worker pool: {0}")]
    Start(#[source] waymark_worker_remote_bringup::StartError),
}

/// Resolve a Python worker [`Config`] and bring up the bridge + worker pool.
pub async fn start(
    shutdown_token: tokio_util::sync::CancellationToken,
    bind_addr: Option<SocketAddr>,
    config: Config,
    worker_pool_size: NonZeroUsize,
    max_action_lifecycle: Option<NonZeroU64>,
    max_concurrent_per_worker: NonZeroUsize,
) -> Result<(waymark_worker_process_pool::Pool<Spec>, JoinHandle<()>), StartError> {
    let factory = waymark_worker_python::resolve(config)
        .await
        .map_err(StartError::Resolve)?;
    waymark_worker_remote_bringup::start(
        shutdown_token,
        bind_addr,
        move |bridge_server_addr| Spec {
            bridge_server_addr,
            factory,
        },
        worker_pool_size,
        max_action_lifecycle,
        max_concurrent_per_worker,
    )
    .await
    .map_err(StartError::Start)
}
