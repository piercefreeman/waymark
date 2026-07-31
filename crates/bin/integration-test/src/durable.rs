//! Durable execution mode: postgres-backed snapshots, action calls, and
//! sleeps via the execution subsystem.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use waymark_secret_string::SecretString;
use waymark_support_integration::{LOCAL_POSTGRES_DSN, connect_pool, ensure_local_postgres};

use crate::ground_truth::PreparedCase;
use crate::outcome::{CaseOutcome, canonicalize_outcome, check_case_outcome, outcome_from_vm};
use crate::worker_pool::{setup_worker_pool, teardown_worker_pool};

/// The postgres-backed services the durable mode drives directly: workflow
/// submission (compile + register) and outcome polling. Execution itself is
/// carried by the execution subsystem spawned in [`run_durable_mode`].
struct DurableStack {
    backend: waymark_backend_postgres::PostgresBackend,
    executables: waymark_workflow_service_vm_executables::ExecutablesService<
        waymark_backend_postgres::PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >,
    registration: waymark_workflow_service_vm_runtimes::RegistrationService<
        waymark_backend_postgres::PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
    >,
    outcome_polling: waymark_workflow_service_vm_runtimes::OutcomePollingService<
        waymark_backend_postgres::PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::ReadyValue,
    >,
}

pub async fn run_durable_mode(
    repo_root: &Path,
    prepared_cases: &[PreparedCase],
    worker_count: NonZeroUsize,
    timeout: Duration,
) -> Result<Vec<String>> {
    let stack = connect_durable_stack().await?;

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let force_shutdown_token = tokio_util::sync::CancellationToken::new();
    let (worker_pool, bridge_server_task) = setup_worker_pool(
        shutdown_token.clone(),
        repo_root,
        prepared_cases,
        worker_count,
    )
    .await
    .context("start durable worker pool")?;

    // The execution subsystem launches the worker pool itself.
    let execution_handles = waymark_execution_bringup::start(
        durable_execution_config(),
        Arc::new(stack.backend.clone()),
        Arc::clone(&worker_pool),
        shutdown_token.child_token(),
        force_shutdown_token.child_token(),
    )
    .await
    .context("start durable execution subsystem")?;

    let mut failures = Vec::new();
    for prepared in prepared_cases {
        let actual = run_case_durable(prepared, &stack, timeout).await;
        if let Some(mismatch) = check_case_outcome(prepared, actual) {
            failures.push(mismatch);
        }
    }

    // Every outcome has been received, so nothing is draining — force the
    // pinning manager out of its drain loop along with the graceful stop.
    shutdown_token.cancel();
    force_shutdown_token.cancel();
    shutdown_execution(execution_handles).await;
    teardown_worker_pool(shutdown_token, bridge_server_task, worker_pool).await;

    Ok(failures)
}

async fn connect_durable_stack() -> Result<DurableStack> {
    let dsn = std::env::var("WAYMARK_DATABASE_URL")
        .map(SecretString::from)
        .unwrap_or_else(|_| SecretString::from(LOCAL_POSTGRES_DSN));

    if dsn.expose_secret() == LOCAL_POSTGRES_DSN.expose_secret() {
        ensure_local_postgres()
            .await
            .context("auto-bootstrap local postgres for integration runner")?;
    }

    let pool = connect_pool(&dsn)
        .await
        .with_context(|| format!("connect postgres backend: {dsn}"))?;
    waymark_backend_postgres_migrations::run(&pool)
        .await
        .context("run postgres migrations for integration runner")?;

    // Reset the durable-VM tables so stale runnable workloads from prior
    // (crashed) runs cannot be revived into this run's execution subsystem.
    sqlx::query(
        r#"
        TRUNCATE action_call_completions,
                 action_call_requests,
                 sleep_requests,
                 vm_executables,
                 vm_runtime_snapshots,
                 runnable_workloads,
                 vm_execution_results
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(&pool)
    .await
    .context("truncate durable-VM tables")?;

    let backend = waymark_backend_postgres::PostgresBackend::new(pool);
    let codec = waymark_vm_codec_rmp::RmpCodec;

    Ok(DurableStack {
        executables: waymark_workflow_service_vm_executables::ExecutablesService::new(
            backend.clone(),
            codec,
        ),
        registration: waymark_workflow_service_vm_runtimes::RegistrationService::new(
            backend.clone(),
            codec,
        ),
        outcome_polling: waymark_workflow_service_vm_runtimes::OutcomePollingService::new(
            backend.clone(),
            codec,
        ),
        backend,
    })
}

fn durable_execution_config() -> waymark_execution_bringup::Config<uuid::Uuid> {
    waymark_execution_bringup::Config {
        node_id: uuid::Uuid::new_v4(),
        action_effect_reconciler_lock_ttl: Duration::from_secs(15).try_into().unwrap(),
        action_effect_reconciler_lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
        max_pinned: 16.try_into().unwrap(),
        pinning_ttl: Duration::from_secs(15).try_into().unwrap(),
        pinning_heartbeat: Duration::from_secs(5).try_into().unwrap(),
        pinning_fencing_margin: Duration::from_secs(1).try_into().unwrap(),
        workload_poll_rate_limit: 1000.try_into().unwrap(),
        snapshot_batch_max: 256.try_into().unwrap(),
        snapshot_batch_delay: Duration::from_millis(5).try_into().unwrap(),
        action_effect_reconciler_request_batch_max: 256.try_into().unwrap(),
        action_effect_reconciler_request_batch_delay: Duration::from_millis(5).try_into().unwrap(),
        workflow_completion_batch_max: 256.try_into().unwrap(),
        workflow_completion_batch_delay: Duration::from_millis(5).try_into().unwrap(),
        sleep_poll_interval: Duration::from_millis(250).try_into().unwrap(),
        vm_retention: Duration::from_secs(60).try_into().unwrap(),
        vm_sweep_interval: Duration::from_secs(10).try_into().unwrap(),
        executable_retention: Duration::from_secs(300).try_into().unwrap(),
        executable_sweep_interval: Duration::from_secs(60).try_into().unwrap(),
    }
}

async fn shutdown_execution(handles: waymark_execution_bringup::Handles) {
    let waymark_execution_bringup::Handles {
        pinning_manager,
        execution_driver,
        executable_sweeper,
        vm_sweeper,
        durable_action_completions_writer,
        durable_action_completions_poller,
        durable_action_completions_acker,
        durable_sleeps_poller,
        durable_sleeps_acker,
        action_effect_reconciler_lock_renewal,
        snapshot_batcher,
        action_effect_reconciler_request_batcher,
        workflow_completion_batcher,
    } = handles;

    let _ = tokio::time::timeout(Duration::from_secs(5), pinning_manager).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), execution_driver).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), executable_sweeper).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), vm_sweeper).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_writer).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_poller).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_acker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_sleeps_poller).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_sleeps_acker).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        action_effect_reconciler_lock_renewal,
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(5), snapshot_batcher).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        action_effect_reconciler_request_batcher,
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(5), workflow_completion_batcher).await;
}

async fn run_case_durable(
    prepared: &PreparedCase,
    stack: &DurableStack,
    timeout: Duration,
) -> Result<CaseOutcome> {
    let (executable_id, executable, metadata) = stack
        .executables
        .compile_and_store(
            &prepared.workflow_name,
            &prepared.workflow_version,
            &prepared.program,
        )
        .await
        .map_err(|err| {
            anyhow!(
                "compile and store executable for case '{}': {err}",
                prepared.case.id
            )
        })?;

    let call_spec = waymark_vm_runtime_builder::builder(&metadata)
        .first_fn()
        .map_err(|err| {
            anyhow!(
                "select entry function for case '{}': {err}",
                prepared.case.id
            )
        })?
        .args(prepared.inputs.clone())
        .map_err(|err| {
            anyhow!(
                "match entry function arguments for case '{}': {err}",
                prepared.case.id
            )
        })?;

    let runtime = waymark_system_vm::Runtime::with_custom_entrypoint(
        waymark_system_vm::Interpreter::default(),
        Arc::new(executable),
        call_spec,
    )
    .map_err(|err| anyhow!("create VM runtime for case '{}': {err}", prepared.case.id))?;

    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    stack
        .registration
        .register_vm(vm_id, executable_id, |serializer| {
            runtime.snapshot(serializer)
        })
        .await
        .map_err(|err| anyhow!("register VM for case '{}': {err}", prepared.case.id))?;

    let workflow_outcome = tokio::time::timeout(
        timeout,
        stack
            .outcome_polling
            .wait_for_outcome(&vm_id, Duration::from_millis(100)),
    )
    .await
    .map_err(|_elapsed| {
        anyhow!(
            "case '{}' timed out after {}s",
            prepared.case.id,
            timeout.as_secs()
        )
    })?
    .map_err(|err| anyhow!("wait for outcome of case '{}': {err}", prepared.case.id))?;

    Ok(canonicalize_outcome(outcome_from_vm(workflow_outcome)?))
}
