//! Migrations for the Postgres observability store.

#![warn(missing_docs)]

/// The tables the migrations engine keeps in the observability store: in
/// the store's own schema, like its other tables.
#[derive(Debug)]
pub struct Tables;

impl waymark_migrations_engine_backend_sqlx_postgres::Tables for Tables {
    const LEDGER: &'static str = "_waymark_observability_migrations";

    const LOCKS: &'static str = "_waymark_observability_migrations_locks";
}

/// The assertions of the observability store.
pub type Assertions =
    (waymark_migrations_engine_backend_sqlx_postgres::SqlxMigrationsLedgerAbsentAssertion,);

/// The assertions the observability store has to meet before the migrations
/// engine does anything with it.
pub fn assertions() -> Assertions {
    (waymark_migrations_engine_backend_sqlx_postgres::SqlxMigrationsLedgerAbsentAssertion,)
}

/// A migration of the observability store.
pub type Migration = waymark_migrations_source::MigrationSource<sqlx::PgConnection>;

/// The migrations of the observability store, in the order of their
/// versions.
pub fn migrations() -> [Migration; 6] {
    waymark_migrations_source::migrations![
        "migrations/0001_essential_metrics_node_samples.sql",
        "migrations/0002_observability_events.sql",
        "migrations/0003_observability_events_vm_timeline.sql",
        "migrations/0004_observability_vm_instances.sql",
        "migrations/0005_essential_metrics_observability_events_dropped.sql",
        "migrations/0006_observability_vm_executables.sql",
    ]
}

/// The embedded SQLx migrations.
const MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// Run the embedded SQLx migrations.
pub async fn run<'m, Migrator>(pool: Migrator) -> Result<(), sqlx::migrate::MigrateError>
where
    Migrator: sqlx::Acquire<'m>,
    <Migrator::Connection as std::ops::Deref>::Target: sqlx::migrate::Migrate,
{
    MIGRATOR.run(pool).await
}

/// Check that the store is migrated exactly to the embedded SQLx
/// migrations, without running them.
pub async fn check<'m, Acquire>(
    connection: Acquire,
) -> Result<(), waymark_sqlx_postgres_migrations::CheckError>
where
    Acquire: sqlx::Acquire<'m, Database = sqlx::Postgres>,
{
    waymark_sqlx_postgres_migrations::check(&MIGRATOR, connection).await
}

#[cfg(test)]
mod tests;
