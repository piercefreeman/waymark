//! Harness shared by every family's tests.

use crate::Store;

/// A store over a fresh schema of its own, so tests never share tables.
pub async fn test_store(schema: &str) -> Store {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE"#))
        .execute(&bootstrap)
        .await
        .expect("drop leftover test schema");

    let pool = waymark_sqlx_postgres_schema_pool::connect(
        waymark_support_integration::LOCAL_POSTGRES_DSN.expose_secret(),
        schema,
    )
    .await
    .expect("schema pool comes up");
    waymark_observability_store_postgres_migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("migrations run");
    Store { pool }
}
