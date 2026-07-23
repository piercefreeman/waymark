use waymark_worker_core::BaseWorkerPool as _;

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
    let (process_pool, mut bridge_server_task) = waymark_worker_remote_bringup::start(
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

    let worker_pool = std::sync::Arc::new(waymark_worker_remote_pool::RemoteWorkerPool::new(
        process_pool,
    ));
    worker_pool.launch().await?;

    let waymark_transient_execution_bringup::Execution {
        workflow_outcome_rx,
        driver_handle,
    } = waymark_transient_execution_worker_pool_bringup::execute(
        runtime,
        std::sync::Arc::clone(&worker_pool),
        false,
        tokio_util::sync::CancellationToken::new(),
    );

    let workflow_outcome = workflow_outcome_rx.await;

    // The driver terminates right after delivering the workflow outcome —
    // including on success — so join it unconditionally for its exit report.
    let Err(driver_exit) = driver_handle.await;
    tracing::debug!(?driver_exit, "vm driver exited");

    let workflow_outcome = workflow_outcome.map_err(|_recv_error| {
        waymark_fn_main_common::Error::msg(
            "vm driver exited without delivering the workflow outcome",
        )
    })?;

    shutdown_token.cancel();
    let bridge_server_shutdown =
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut bridge_server_task).await;
    if bridge_server_shutdown.is_err() {
        tracing::warn!("bridge server did not stop in time, aborting it");
        bridge_server_task.abort();
        let _ = bridge_server_task.await;
    }
    worker_pool.shutdown_arc().await?;

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
