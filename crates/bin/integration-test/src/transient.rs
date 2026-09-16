//! Transient execution mode: in-memory VM runtime, no persistence.

use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use color_eyre::eyre::{WrapErr as _, bail, eyre};

use crate::ground_truth::PreparedCase;
use crate::outcome::{CaseOutcome, check_case_outcome, outcome_from_vm};
use crate::worker_pool::{PythonWorkerPool, drain_run, setup_worker_pool};
use waymark_managed_spawner_supervised::SupervisorExt as _;

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
        let mut supervisor =
            waymark_managed_spawner_supervised::supervisor::start(shutdown_token.clone());

        // The case under the supervisor: a failure part-way leaves the tasks
        // already up supervised, and they are shut down and drained below
        // like on any other exit.
        let case_result: Result<_, color_eyre::eyre::Report> = async {
            let mut supervisor = supervisor.spawner(waymark_fn_main_common::ErrorConverter);

            let worker_pool = setup_worker_pool(
                &mut supervisor,
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

            // The runner keeps its own worker pool requests handle so the
            // loop cannot stop before the shutdown request.
            let worker_pool_requests = std::sync::Arc::clone(&worker_pool.requests);

            let actual = run_case_transient(prepared, worker_pool, timeout).await;

            // The case is done, so the shutdown is requested here, and then
            // the runner's worker pool requests handle is dropped, the last
            // one now that the driver is joined.
            shutdown_token.cancel();
            drop(worker_pool_requests);

            Ok(actual)
        }
        .await;

        // Whatever the case did, the shutdown is requested and the run is
        // drained. A task that ended before the request is the root cause of
        // whatever the case saw, so it is reported over the case's own
        // failure.
        shutdown_token.cancel();
        drain_run(supervisor)
            .await
            .wrap_err_with(|| format!("drain the run for case '{}'", prepared.case.id))?;
        let actual = case_result?;

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
        worker_pool.requests,
        worker_pool.completions,
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
