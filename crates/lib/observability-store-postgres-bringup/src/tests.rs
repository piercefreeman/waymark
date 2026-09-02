use std::num::NonZeroU32;

use waymark_observability_store_postgres_config::PostgresConfig;

use super::*;

const TEST_SCHEMA: &str = "observability_postgres_bringup_test";

#[tokio::test]
async fn schema_pool_creates_the_schema_and_scopes_the_search_path() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{TEST_SCHEMA}" CASCADE"#))
        .execute(&bootstrap)
        .await
        .expect("drop leftover test schema");

    let config = PostgresConfig {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        max_connections: NonZeroU32::new(2).expect("non-zero"),
    };
    let pool = schema_pool(&config, TEST_SCHEMA)
        .await
        .expect("schema pool comes up");

    let (current_schema,): (String,) = sqlx::query_as("SELECT current_schema()")
        .fetch_one(&pool)
        .await
        .expect("read current schema");
    assert_eq!(current_schema, TEST_SCHEMA);

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
