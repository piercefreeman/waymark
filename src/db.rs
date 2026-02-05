//! Database helpers shared across services.

use sqlx::PgPool;

use crate::backends::{BackendError, BackendResult};

/// Run the embedded SQLx migrations.
pub async fn run_migrations(pool: &PgPool) -> BackendResult<()> {
    sqlx::migrate!()
        .run(pool)
        .await
        .map_err(|err| BackendError::Message(err.to_string()))?;
    Ok(())
}
