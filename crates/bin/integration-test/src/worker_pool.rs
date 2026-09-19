//! Python worker pool lifecycle, shared by both execution modes.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::WrapErr as _;

use crate::ground_truth::PreparedCase;

pub type PythonWorkerPool = Arc<waymark_worker_remote_pool::Pool>;

/// The spawned worker pool loop; ends with the worker process pool's
/// shutdown result once every worker pool handle is dropped.
pub type PoolLoop = tokio::task::JoinHandle<Result<(), waymark_managed_process::ShutdownError>>;

pub async fn setup_worker_pool(
    shutdown_token: tokio_util::sync::CancellationToken,
    repo_root: &Path,
    cases: &[PreparedCase],
    worker_count: NonZeroUsize,
) -> Result<(PythonWorkerPool, tokio::task::JoinHandle<()>, PoolLoop), color_eyre::eyre::Report> {
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
    .wrap_err("create remote worker pool")?;

    let (worker_pool, pool_loop) = waymark_worker_remote_pool::run(process_pool);
    let pool_loop = tokio::spawn(pool_loop);
    let worker_pool = Arc::new(worker_pool);

    Ok((worker_pool, bridge_server_task, pool_loop))
}

pub async fn teardown_worker_pool(
    shutdown_token: tokio_util::sync::CancellationToken,
    mut bridge_server_task: tokio::task::JoinHandle<()>,
    pool_loop: PoolLoop,
    worker_pool: PythonWorkerPool,
) {
    // Dropping the last handle lets the worker pool loop shut the workers
    // down; a case that left a clone behind keeps it running, hence the
    // bound. The bridge server's graceful shutdown then has no streams
    // left to wait for.
    drop(worker_pool);
    match tokio::time::timeout(Duration::from_secs(5), pool_loop).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(err))) => eprintln!("failed to shutdown worker pool: {err}"),
        Ok(Err(err)) => eprintln!("worker pool loop panicked: {err}"),
        Err(_elapsed) => eprintln!("worker pool did not shut down in time"),
    }

    shutdown_token.cancel();
    let bridge_server_shutdown =
        tokio::time::timeout(Duration::from_secs(5), &mut bridge_server_task).await;
    if bridge_server_shutdown.is_err() {
        tracing::warn!("bridge server did not stop in time, aborting it");
        bridge_server_task.abort();
        let _ = bridge_server_task.await;
    }
}
