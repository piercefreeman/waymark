//! Python worker pool lifecycle, shared by both execution modes.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use color_eyre::eyre::{WrapErr as _, eyre};

use crate::ground_truth::PreparedCase;

pub type PythonWorkerPool = Arc<waymark_worker_remote_pool::Pool>;

/// The supervisor of a run's tasks: their errors differ per task, so they
/// are supervised erased.
pub type Supervisor = waymark_task_supervisor::Supervisor<waymark_task_supervisor::BoxedError>;

/// Start the worker pool under `supervisor`: the bridge server and the
/// worker pool loop are its tasks.
pub async fn setup_worker_pool(
    supervisor: &mut Supervisor,
    shutdown_token: tokio_util::sync::CancellationToken,
    repo_root: &Path,
    cases: &[PreparedCase],
    worker_count: NonZeroUsize,
) -> Result<PythonWorkerPool, color_eyre::eyre::Report> {
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

    supervisor.track("worker bridge server", bridge_server_task);

    let (worker_pool, pool_loop) = waymark_worker_remote_pool::run(process_pool);
    supervisor.spawn("worker pool loop", pool_loop);

    Ok(Arc::new(worker_pool))
}

/// Drain a run's tasks, however long it takes, once its shutdown has been
/// requested and its worker pool handles released. A task that ended
/// before the request is a failed run.
pub async fn drain_run(supervisor: Supervisor) -> Result<(), color_eyre::eyre::Report> {
    let report = supervisor.drain().await;

    if report.any_before_shutdown() {
        return Err(eyre!(
            "a task ended before the shutdown was requested:\n{report}"
        ));
    }

    Ok(())
}
