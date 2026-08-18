//! Transient execution mode: in-memory VM runtime, no persistence.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::{WrapErr as _, bail, eyre};
use waymark_worker_core::LaunchWorkerPool as _;

use crate::ground_truth::PreparedCase;
use crate::outcome::{CaseOutcome, check_case_outcome, outcome_from_vm};
use crate::worker_pool::{PythonWorkerPool, setup_worker_pool, teardown_worker_pool};

pub async fn run_transient_mode(
    repo_root: &Path,
    prepared_cases: &[PreparedCase],
    worker_count: NonZeroUsize,
    timeout: Duration,
) -> Result<Vec<String>, color_eyre::eyre::Report> {
    let mut failures = Vec::new();
    for prepared in prepared_cases {
        // The worker-pool transport round-trips correlation metadata verbatim
        // with no per-VM filtering, so an action that outlives its case — a
        // harness timeout, or a workflow that settles while an action is
        // still in flight (e.g. a VM-level action timeout) — would deliver
        // its completion into whatever case polls the pool next. Bound every
        // completion's lifetime by its case: each case gets its own pool.
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let (worker_pool, bridge_server_task) = setup_worker_pool(
            shutdown_token.clone(),
            repo_root,
            std::slice::from_ref(prepared),
            worker_count,
        )
        .await
        .wrap_err_with(|| {
            format!(
                "start transient worker pool for case '{}'",
                prepared.case.id
            )
        })?;
        worker_pool.launch().await.wrap_err_with(|| {
            format!(
                "launch transient worker pool for case '{}'",
                prepared.case.id
            )
        })?;

        let actual = run_case_transient(prepared, Arc::clone(&worker_pool), timeout).await;
        teardown_worker_pool(shutdown_token, bridge_server_task, worker_pool).await;

        if let Some(mismatch) = check_case_outcome(prepared, actual) {
            failures.push(mismatch);
        }
    }

    Ok(failures)
}

async fn run_case_transient(
    prepared: &PreparedCase,
    worker_pool: PythonWorkerPool,
    timeout: Duration,
) -> Result<CaseOutcome, color_eyre::eyre::Report> {
    let runtime = waymark_transient_execution_bringup::setup_runtime(
        &prepared.program,
        prepared.inputs.clone(),
    )
    .wrap_err_with(|| format!("set up VM runtime for case '{}'", prepared.case.id))?;

    let cancel = tokio_util::sync::CancellationToken::new();
    let waymark_transient_execution_bringup::Execution {
        workflow_outcome_rx,
        driver_handle,
    } = waymark_transient_execution_worker_pool_bringup::execute(
        runtime,
        worker_pool,
        false,
        cancel.clone(),
    );

    let workflow_outcome = match tokio::time::timeout(timeout, workflow_outcome_rx).await {
        Ok(received) => received,
        Err(_elapsed) => {
            cancel.cancel();
            let Err(driver_exit) = driver_handle.await;
            tracing::debug!(?driver_exit, "vm driver exited after cancellation");
            bail!(
                "case '{}' timed out after {}s",
                prepared.case.id,
                timeout.as_secs()
            )
        }
    };

    // The driver terminates right after delivering the workflow outcome —
    // including on success — so join it unconditionally for its exit report.
    let Err(driver_exit) = driver_handle.await;
    tracing::debug!(?driver_exit, "vm driver exited");

    let workflow_outcome = workflow_outcome.map_err(|_recv_error| {
        eyre!(
            "vm driver exited without delivering a workflow outcome for case '{}'",
            prepared.case.id
        )
    })?;

    Ok(outcome_from_vm(workflow_outcome))
}
