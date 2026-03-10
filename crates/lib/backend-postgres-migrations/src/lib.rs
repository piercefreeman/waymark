//! Migrations for the postgres backend.

/// Run the embedded SQLx migrations.
pub async fn run<'m, Migrator>(pool: Migrator) -> Result<(), sqlx::migrate::MigrateError>
where
    Migrator: sqlx::Acquire<'m>,
    <Migrator::Connection as std::ops::Deref>::Target: sqlx::migrate::Migrate,
{
    sqlx::migrate!().run(pool).await
}
