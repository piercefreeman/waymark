//! Migrations for the postgres backend.

/// The tables the migrations engine keeps in the main database.
#[derive(Debug)]
pub struct Tables;

impl waymark_migrations_engine_backend_sqlx_postgres::Tables for Tables {
    const LEDGER: &'static str = "_waymark_main_migrations";

    const LOCKS: &'static str = "_waymark_main_migrations_locks";
}

/// The assertions of the main database.
pub type Assertions =
    (waymark_migrations_engine_backend_sqlx_postgres::SqlxMigrationsLedgerAbsentAssertion,);

/// The assertions the main database has to meet before the migrations
/// engine does anything with it.
pub fn assertions() -> Assertions {
    (waymark_migrations_engine_backend_sqlx_postgres::SqlxMigrationsLedgerAbsentAssertion,)
}

/// A migration of the main database.
pub type Migration = waymark_migrations_source::MigrationSource<sqlx::PgConnection>;

/// The migrations of the main database, in the order of their versions.
pub fn migrations() -> [Migration; 27] {
    waymark_migrations_source::migrations![
        "migrations/0001_init.sql",
        "migrations/0002_runner_actions_done_execution_id.sql",
        "migrations/0003_instance_locks.sql",
        "migrations/0004_workflow_versions.sql",
        "migrations/0005_runner_instances_workflow_version_id.sql",
        "migrations/0006_drop_unused_runner_tables.sql",
        "migrations/0007_runner_instances_schedule_id.sql",
        "migrations/0008_runner_actions_done_timing.sql",
        "migrations/0009_instance_search_columns.sql",
        "migrations/0010_runner_instances_perf.sql",
        "migrations/0011_runner_instances_state_not_null.sql",
        "migrations/0012_vm_runtime_snapshots.sql",
        "migrations/0013_workload_pinnings.sql",
        "migrations/0014_vm_execution_results.sql",
        "migrations/0015_vm_executables.sql",
        "migrations/0016_action_call_completions.sql",
        "migrations/0017_action_call_requests.sql",
        "migrations/0018_rename_workload_pinnings_to_runnable_workloads.sql",
        "migrations/0019_sleep_requests.sql",
        "migrations/0020_snapshot_delete_cleanup.sql",
        "migrations/0021_drop_legacy_tables.sql",
        "migrations/0022_schedules.sql",
        "migrations/0023_canonical_row_lock_order.sql",
        "migrations/0024_rename_action_call_completions_outcome.sql",
        "migrations/0025_drop_worker_status.sql",
        "migrations/0026_action_call_request_lock_nonce.sql",
        waymark_backend_postgres_migration_0027_id_map_tables,
    ]
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod transition_tests;
