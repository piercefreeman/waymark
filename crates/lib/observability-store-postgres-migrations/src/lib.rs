//! Migrations for the Postgres observability store.

#![warn(missing_docs)]

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
