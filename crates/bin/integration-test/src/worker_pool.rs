//! Python worker pool lifecycle, shared by both execution modes.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::ground_truth::PreparedCase;

pub type PythonWorkerPool =
    Arc<waymark_worker_remote_pool::RemoteWorkerPool<waymark_worker_python::Spec>>;

pub async fn setup_worker_pool(
    shutdown_token: tokio_util::sync::CancellationToken,
    repo_root: &Path,
    cases: &[PreparedCase],
    worker_count: NonZeroUsize,
) -> Result<(PythonWorkerPool, tokio::task::JoinHandle<()>)> {
    let mut modules = cases
        .iter()
        .map(|prepared| prepared.case.module_name.to_string())
        .collect::<Vec<_>>();
    modules.sort();
    modules.dedup();

    let config = waymark_worker_python::Config::new()
        .with_user_modules(modules)
        .with_python_paths(vec![
            repo_root.join("python"),
            repo_root.join("tests"),
            repo_root.join("tests/integration_tests"),
        ]);

    let (process_pool, bridge_server_task) = waymark_worker_remote_bringup::start(
        shutdown_token,
        None,
        |bridge_server_addr| waymark_worker_python::Spec {
            bridge_server_addr,
            config,
        },
        worker_count,
        None,
        10.try_into().unwrap(),
    )
    .await
    .context("create remote worker pool")?;

    let worker_pool = Arc::new(waymark_worker_remote_pool::RemoteWorkerPool::new(
        process_pool,
    ));

    Ok((worker_pool, bridge_server_task))
}

pub async fn teardown_worker_pool(
    shutdown_token: tokio_util::sync::CancellationToken,
    mut bridge_server_task: tokio::task::JoinHandle<()>,
    worker_pool: PythonWorkerPool,
) {
    shutdown_token.cancel();
    let bridge_server_shutdown =
        tokio::time::timeout(Duration::from_secs(5), &mut bridge_server_task).await;
    if bridge_server_shutdown.is_err() {
        tracing::warn!("bridge server did not stop in time, aborting it");
        bridge_server_task.abort();
        let _ = bridge_server_task.await;
    }

    if let Err(err) = worker_pool.shutdown_arc().await {
        eprintln!("failed to shutdown worker pool: {err}");
    }
}
