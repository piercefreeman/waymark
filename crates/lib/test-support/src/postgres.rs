//! Shared Postgres fixture bootstrapped from root docker-compose.

use sqlx::PgPool;

use waymark_integration_support::{LOCAL_POSTGRES_DSN, connect_pool, ensure_local_postgres};

/// Ensure test Postgres is available and migrated, then return a pooled connection.
pub async fn postgres_setup() -> PgPool {
    ensure_local_postgres()
        .await
        .unwrap_or_else(|err| panic!("postgres_setup bootstrap failed: {err:#}"));
    connect_pool(LOCAL_POSTGRES_DSN)
        .await
        .unwrap_or_else(|err| panic!("postgres_setup connect failed: {err:#}"))
}
