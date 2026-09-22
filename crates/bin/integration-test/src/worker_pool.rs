//! Python worker pool lifecycle, shared by both execution modes.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use color_eyre::eyre::{WrapErr as _, eyre};

use crate::ground_truth::PreparedCase;

/// The two sides of a run's worker pool.
pub struct PythonWorkerPool {
    pub requests: Arc<waymark_worker_remote_pool::Requests>,
    pub completions: waymark_worker_remote_pool::Completions,
}

/// Start the worker pool under `spawner`: the bridge server and the
/// worker pool loop are its tasks.
pub async fn setup_worker_pool<Spawner>(
    mut spawner: Spawner,
    shutdown_token: tokio_util::sync::CancellationToken,
    repo_root: &Path,
    cases: &[PreparedCase],
    worker_count: NonZeroUsize,
) -> Result<PythonWorkerPool, color_eyre::eyre::Report>
where
    Spawner: waymark_managed_spawner::Spawner,
{
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

    let process_pool = waymark_worker_remote_bringup::start(
        &mut spawner,
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

    let (worker_pool_requests, worker_pool_completions, worker_pool_loop) =
        waymark_worker_remote_pool::run(process_pool);
    spawner.spawn("worker pool loop", worker_pool_loop);

    Ok(PythonWorkerPool {
        requests: Arc::new(worker_pool_requests),
        completions: worker_pool_completions,
    })
}

/// Drain a run's tasks, however long it takes, once its shutdown has been
/// requested and its worker pool handles released. A task that ended
/// before the request is a failed run.
pub async fn drain_run(
    supervisor: waymark_managed_spawner_supervised::supervisor::Supervisor<
        waymark_eyre_error::ReportError,
    >,
) -> Result<(), color_eyre::eyre::Report> {
    let report = supervisor.drain().await;

    if report.any_before_shutdown() {
        return Err(eyre!(
            "a task ended before the shutdown was requested:\n{report}"
        ));
    }

    Ok(())
}
