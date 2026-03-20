use std::{
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use tokio::task::JoinHandle;

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("bridge: {0}")]
    Bridge(#[source] std::io::Error),

    #[error("pool: {0}")]
    Pool(#[source] waymark_worker_process_pool::InitError),
}

pub async fn start<Spec>(
    shutdown_token: tokio_util::sync::CancellationToken,
    bind_addr: Option<SocketAddr>,
    worker_process_spec_builder: impl FnOnce(SocketAddr) -> Spec,
    worker_pool_size: NonZeroUsize,
    max_action_lifecycle: Option<NonZeroU64>,
    max_concurrent_per_worker: NonZeroUsize,
) -> Result<(waymark_worker_process_pool::Pool<Spec>, JoinHandle<()>), StartError>
where
    Spec: waymark_worker_process_spec::Spec,
{
    let workers_registry = Default::default();

    // Bringup server first.
    let (bridge_addr, bridge_task) = waymark_worker_remote_bridge_bringup::start(
        shutdown_token,
        Arc::clone(&workers_registry),
        bind_addr,
    )
    .await
    .map_err(StartError::Bridge)?;

    let worker_process_spec = (worker_process_spec_builder)(bridge_addr);

    let pool = waymark_worker_process_pool::Pool::new_with_concurrency(
        workers_registry,
        worker_process_spec,
        worker_pool_size,
        max_action_lifecycle,
        max_concurrent_per_worker,
    )
    .await
    .map_err(StartError::Pool)?;

    tracing::info!(
        %worker_pool_size,
        %bridge_addr,
        "worker pool started"
    );

    Ok((pool, bridge_task))
}
