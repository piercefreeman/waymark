use super::*;

const TEST_SCHEMA: &str = "postgres_schema_pool_test";

#[tokio::test]
async fn search_path_scoping_and_schema_creation() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(&format!(r#"DROP SCHEMA IF EXISTS "{TEST_SCHEMA}" CASCADE"#))
        .execute(&bootstrap)
        .await
        .expect("drop leftover test schema");

    let pool = connect(
        waymark_support_integration::LOCAL_POSTGRES_DSN.expose_secret(),
        TEST_SCHEMA,
    )
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

#[tokio::test]
async fn existing_schema_needs_no_create_privilege() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    for statement in [
        // PUBLIC never has database CREATE by default; revoking makes the
        // test independent of ambient grants.
        "REVOKE CREATE ON DATABASE waymark FROM PUBLIC",
        "DROP SCHEMA IF EXISTS lowpriv_provisioned CASCADE",
        "DROP SCHEMA IF EXISTS lowpriv_missing CASCADE",
        "DROP ROLE IF EXISTS schema_pool_lowpriv",
        "CREATE ROLE schema_pool_lowpriv LOGIN PASSWORD 'lowpriv'",
        "CREATE SCHEMA lowpriv_provisioned",
        "GRANT USAGE ON SCHEMA lowpriv_provisioned TO schema_pool_lowpriv",
    ] {
        sqlx::query(statement)
            .execute(&bootstrap)
            .await
            .expect(statement);
    }

    let options = waymark_support_integration::LOCAL_POSTGRES_DSN
        .expose_secret()
        .parse::<sqlx::postgres::PgConnectOptions>()
        .expect("parse DSN")
        .username("schema_pool_lowpriv")
        .password("lowpriv");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("low-privilege pool comes up");

    create_schema_if_not_exists(&pool, "lowpriv_provisioned")
        .await
        .expect("a provisioned schema needs no CREATE privilege");
    create_schema_if_not_exists(&pool, "lowpriv_missing")
        .await
        .expect_err("a missing schema without CREATE privilege must fail");

    for statement in [
        "DROP SCHEMA lowpriv_provisioned CASCADE",
        "DROP ROLE schema_pool_lowpriv",
    ] {
        sqlx::query(statement)
            .execute(&bootstrap)
            .await
            .expect(statement);
    }
}
