/// The supervisor of the run's tasks: their errors differ per task, so
/// they are supervised erased.
type Supervisor = waymark_task_supervisor::Supervisor<waymark_task_supervisor::BoxedError>;

/// Run the runtime over a freshly spawned Python worker pool and return
/// the workflow outcome.
pub async fn run(
    runtime: waymark_system_vm::Runtime,
) -> Result<
    waymark_workflow_completion_core::Outcome<waymark_system_vm::ReadyValue>,
    waymark_fn_main_common::Error,
> {
    let shutdown_token = tokio_util::sync::CancellationToken::new();

    let worker_config = waymark_worker_python::Config::new()
        .with_user_module("tests.fixtures.test_actions")
        .with_python_paths(vec![repo_root().join("python")]);
    let (process_pool, bridge_server_task) = waymark_worker_remote_bringup::start(
        shutdown_token.clone(),
        None,
        |bridge_server_addr| waymark_worker_python::Spec {
            bridge_server_addr,
            config: worker_config,
        },
        1.try_into().expect("worker count is nonzero"),
        None,
        10.try_into().expect("concurrency is nonzero"),
    )
    .await?;

    let mut supervisor: Supervisor = waymark_task_supervisor::start(shutdown_token.clone());
    supervisor.track("worker bridge server", bridge_server_task);

    let (worker_pool, pool_loop) = waymark_worker_remote_pool::run(process_pool);
    supervisor.spawn("worker pool loop", pool_loop);

    // The bringup wants a `Clone` worker pool, hence the `Arc`; the VM
    // driver owns the only worker pool handle, so joining it is what lets
    // the worker pool loop end.
    let waymark_transient_execution_bringup::Execution {
        workflow_outcome_rx,
        driver_handle,
    } = waymark_transient_execution_worker_pool_bringup::execute(
        runtime,
        std::sync::Arc::new(worker_pool),
        false,
        tokio_util::sync::CancellationToken::new(),
    );

    let workflow_outcome = workflow_outcome_rx.await;

    // The outcome is the end of the work, so the shutdown is requested
    // here, before the VM driver is joined: joining it drops the only
    // worker pool handle, which is what ends the worker pool loop, after
    // which the bridge server's graceful shutdown has no streams left to
    // wait for.
    shutdown_token.cancel();

    // The driver terminates right after delivering the workflow outcome —
    // including on success — so join it unconditionally for its exit report.
    let Err(driver_exit) = driver_handle.await;
    tracing::debug!(?driver_exit, "vm driver exited");

    // The outcome is what the run is for, so it is returned whatever the
    // report says; an early end is reported, not returned.
    let report = supervisor.drain().await;
    if report.any_before_shutdown() {
        tracing::error!(%report, "a task ended before the shutdown was requested");
    } else {
        tracing::debug!(%report, "tasks drained");
    }

    let workflow_outcome = workflow_outcome.map_err(|_recv_error| {
        waymark_fn_main_common::Error::msg(
            "vm driver exited without delivering the workflow outcome",
        )
    })?;

    Ok(workflow_outcome)
}

/// The workspace root, resolved from this crate's manifest directory
/// (`crates/bin/vm-cli`).
fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("the manifest dir has a workspace root three levels up")
        .to_path_buf()
}
