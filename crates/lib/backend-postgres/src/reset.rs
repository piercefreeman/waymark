//! Destructive database reset helpers, by strength.
//!
//! Free functions over a pool — deliberately not methods on
//! [`PostgresBackend`](crate::PostgresBackend), so the destructive surface
//! stays off the production handle.

use sqlx::PgPool;
use waymark_backends_core::{BackendError, BackendResult};

/// Truncate every live table, resetting identity sequences.
///
/// Keep the table list in sync with the migrations in
/// `waymark-backend-postgres-migrations`.
pub async fn truncate_all(pool: &PgPool) -> BackendResult<()> {
    sqlx::query(
        r#"
        TRUNCATE action_call_completions,
                 action_call_requests,
                 sleep_requests,
                 vm_executables,
                 vm_execution_results,
                 vm_runtime_snapshots,
                 runnable_workloads,
                 worker_status
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Drop every table (including `_sqlx_migrations` and the trigger
/// functions) and re-run the migrations from scratch.
pub async fn rebuild_schema(pool: &PgPool) -> BackendResult<()> {
    // The legacy-table names must stay in this list for as long as
    // migrations 0001-0011 create them: the re-run below replays the full
    // migration history from scratch, so a half-migrated database would
    // otherwise fail on "table already exists".
    sqlx::query(
        r#"
        DROP TABLE IF EXISTS
            worker_status,
            workflow_schedules,
            queued_instances,
            runner_instances,
            runner_actions_done,
            runner_instances_done,
            runner_graph_updates,
            workflow_versions,
            action_call_completions,
            action_call_requests,
            sleep_requests,
            vm_executables,
            vm_runtime_snapshots,
            runnable_workloads,
            vm_execution_results,
            _sqlx_migrations
        CASCADE
        "#,
    )
    .execute(pool)
    .await?;

    // The migrations also create trigger functions; they survive the table
    // drops and would fail the re-run with "function already exists".
    sqlx::query(
        r#"
        DROP FUNCTION IF EXISTS
            action_call_requests_remove_on_completion,
            vm_runtime_snapshots_cleanup_on_delete
        CASCADE
        "#,
    )
    .execute(pool)
    .await?;

    waymark_backend_postgres_migrations::run(pool)
        .await
        .map_err(|err| BackendError::Message(err.to_string()))?;
    Ok(())
}
