//! Destructive database reset helpers, by strength.
//!
//! Free functions — deliberately not methods on
//! [`PostgresBackend`](crate::PostgresBackend), so the destructive surface
//! stays off the production handle.

use sqlx::PgPool;

/// Truncate every live table, resetting identity sequences.
///
/// Keep the table list in sync with the migrations in
/// `waymark-backend-postgres-migrations`.
pub async fn truncate_all(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        TRUNCATE action_call_completions,
                 action_call_requests,
                 schedules,
                 sleep_requests,
                 vm_executables,
                 vm_execution_results,
                 vm_runtime_snapshots,
                 runnable_workloads
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Drop every table the migrations create, `_sqlx_migrations` and the
/// trigger functions, so a migration run from scratch succeeds on
/// `connection` afterwards.
pub async fn drop_schema(connection: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
    // The legacy-table names must stay in this list for as long as the
    // migrations create them: a run from scratch replays the full migration
    // history, so a leftover table would fail it on "table already exists".
    sqlx::query(
        r#"
        DROP TABLE IF EXISTS
            worker_status,
            workflow_schedules,
            schedules,
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
            workload_pinnings,
            runnable_workloads,
            vm_execution_results,
            _sqlx_migrations
        CASCADE
        "#,
    )
    .execute(&mut *connection)
    .await?;

    // The migrations also create trigger functions; they survive the table
    // drops and would fail a run from scratch with "function already
    // exists".
    sqlx::query(
        r#"
        DROP FUNCTION IF EXISTS
            action_call_requests_remove_on_completion,
            vm_runtime_snapshots_cleanup_on_delete
        CASCADE
        "#,
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}
