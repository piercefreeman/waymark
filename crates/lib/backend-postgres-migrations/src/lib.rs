//! Migrations for the postgres backend.

use sqlx::PgPool;

/// Run the embedded SQLx migrations.
pub async fn run(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!().run(pool).await
}
