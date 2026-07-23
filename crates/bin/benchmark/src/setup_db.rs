//! Database reset for a from-scratch benchmark run.

use sqlx::PgPool;

pub async fn drop_benchmark_tables(pool: &PgPool) {
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
    .await
    .expect("drop benchmark tables");

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
    .await
    .expect("drop benchmark trigger functions");
}
