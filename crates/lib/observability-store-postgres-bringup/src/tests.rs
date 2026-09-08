use std::num::NonZeroU32;

use waymark_observability_store_postgres_config::PoolConfig;

use super::*;

const TEST_SCHEMA: &str = "observability_postgres_bringup_test";

#[tokio::test]
async fn schema_pool_creates_the_schema_and_scopes_the_search_path() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{TEST_SCHEMA}" CASCADE"#))
        .execute(&bootstrap)
        .await
        .expect("drop leftover test schema");

    let config = PoolConfig {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        max_connections: NonZeroU32::new(2).expect("non-zero"),
        statement_timeout: waymark_nonzero_duration::NonZeroDuration::from_millis(2_500)
            .expect("non-zero"),
    };
    let pool = schema_pool(&config, TEST_SCHEMA)
        .await
        .expect("schema pool comes up");

    let (current_schema,): (String,) = sqlx::query_as("SELECT current_schema()")
        .fetch_one(&pool)
        .await
        .expect("read current schema");
    assert_eq!(current_schema, TEST_SCHEMA);

    // Every connection carries the statement timeout.
    let (statement_timeout,): (String,) =
        sqlx::query_as("SELECT current_setting('statement_timeout')")
            .fetch_one(&pool)
            .await
            .expect("read statement_timeout");
    assert_eq!(statement_timeout, "2500ms");

    // An unqualified statement lands in the scoped schema.
    sqlx::query("CREATE TABLE probe (id int)")
        .execute(&pool)
        .await
        .expect("create probe table");
    let (probe_schema,): (String,) = sqlx::query_as(
        "SELECT table_schema::text FROM information_schema.tables WHERE table_name = 'probe'",
    )
    .fetch_one(&bootstrap)
    .await
    .expect("locate probe table");
    assert_eq!(probe_schema, TEST_SCHEMA);

    sqlx::query(&format!(r#"DROP SCHEMA "{TEST_SCHEMA}" CASCADE"#))
        .execute(&bootstrap)
        .await
        .expect("drop test schema");
}

#[tokio::test]
async fn read_schema_pool_requires_the_schema_and_creates_nothing() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    let schema = "observability_postgres_bringup_test_read";
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{schema}" CASCADE"#))
        .execute(&bootstrap)
        .await
        .expect("drop leftover test schema");
    let config = PoolConfig {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        max_connections: NonZeroU32::new(2).expect("non-zero"),
        statement_timeout: waymark_nonzero_duration::NonZeroDuration::from_millis(2_500)
            .expect("non-zero"),
    };

    let error = read_schema_pool(&config, schema)
        .await
        .expect_err("nothing provisioned the schema");
    assert!(
        matches!(
            error,
            ReadSchemaPoolError::Connect(
                waymark_sqlx_postgres_schema_pool::ExistingSchemaError::Missing { .. }
            )
        ),
        "{error}"
    );

    // The write side provisions; the read side then finds it.
    let write_pool = schema_pool(&config, schema)
        .await
        .expect("the write pool creates the schema");
    let read_pool = read_schema_pool(&config, schema)
        .await
        .expect("the read pool finds the schema");
    let (current_schema,): (String,) = sqlx::query_as("SELECT current_schema()")
        .fetch_one(&read_pool)
        .await
        .expect("read current schema");
    assert_eq!(current_schema, schema);
    let (statement_timeout,): (String,) =
        sqlx::query_as("SELECT current_setting('statement_timeout')")
            .fetch_one(&read_pool)
            .await
            .expect("read statement_timeout");
    assert_eq!(statement_timeout, "2500ms");

    drop(write_pool);
    sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
        .execute(&bootstrap)
        .await
        .expect("drop test schema");
}
