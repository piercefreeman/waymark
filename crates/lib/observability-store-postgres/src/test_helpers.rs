//! Harness shared by every observability subsystem's tests.

use crate::Store;

/// A store over a fresh schema of its own, so tests never share tables.
pub async fn test_store(schema: &str) -> Store {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE"#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");

    // The migrations create the schema; the pool comes up over it.
    let observability_store_assertions =
        waymark_observability_store_postgres_migrations::assertions();
    let observability_store_migrations = waymark_migrations_engine::validate_migrations_array(
        waymark_observability_store_postgres_migrations::migrations(),
    )
    .expect("the store's migrations are in the order of their versions");
    waymark_support_integration::migrate_database::<
        waymark_observability_store_postgres_migrations::Tables,
        _,
        _,
        _,
    >(
        waymark_support_integration::LOCAL_POSTGRES_DSN,
        Some(schema),
        &observability_store_assertions,
        &observability_store_migrations,
    )
    .await
    .expect("migrations run");

    let pool = waymark_sqlx_postgres_schema_pool::connect(
        waymark_support_integration::LOCAL_POSTGRES_DSN.expose_secret(),
        schema,
    )
    .await
    .expect("schema pool comes up");
    Store { pool }
}
