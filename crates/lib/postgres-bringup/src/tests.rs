use std::num::NonZeroU32;

use waymark_postgres_config::{connection, pool};

use super::*;

const TEST_SCHEMA: &str = "postgres_bringup_test";

const READ_TEST_SCHEMA: &str = "postgres_bringup_test_read";

const CONNECTION_TEST_SCHEMA: &str = "postgres_bringup_test_connection";

const MIGRATE_TEST_SCHEMA: &str = "postgres_bringup_test_migrate";

#[tokio::test]
async fn a_creating_pool_creates_the_schema_and_scopes_the_search_path() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(const_format::formatcp!(
        r#"DROP SCHEMA IF EXISTS "{TEST_SCHEMA}" CASCADE"#
    ))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");

    let config = pool::Config {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        max_connections: NonZeroU32::new(2).expect("non-zero"),
        statement_timeout: Some(
            waymark_nonzero_duration::NonZeroDuration::from_millis(2_500).expect("non-zero"),
        ),
    };
    let pool = pool(Params {
        config: &config,
        transactions: Transactions::Unrestricted,
        schema: Some(Schema::Create(TEST_SCHEMA.into())),
    })
    .await
    .expect("the pool comes up");
    assert_eq!(pool.options().get_max_connections(), 2);

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
    sqlx::query("CREATE TABLE postgres_bringup_probe (id int)")
        .execute(&pool)
        .await
        .expect("create probe table");
    let (probe_schema,): (String,) = sqlx::query_as(
        "SELECT table_schema::text FROM information_schema.tables WHERE table_name = 'postgres_bringup_probe'",
    )
    .fetch_one(&bootstrap)
    .await
    .expect("locate probe table");
    assert_eq!(probe_schema, TEST_SCHEMA);

    sqlx::query(const_format::formatcp!(
        r#"DROP SCHEMA "{TEST_SCHEMA}" CASCADE"#
    ))
    .execute(&bootstrap)
    .await
    .expect("drop test schema");
}

#[tokio::test]
async fn a_read_only_requiring_pool_creates_nothing() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(const_format::formatcp!(
        r#"DROP SCHEMA IF EXISTS "{READ_TEST_SCHEMA}" CASCADE"#
    ))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");
    let config = pool::Config {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        max_connections: NonZeroU32::new(2).expect("non-zero"),
        statement_timeout: Some(
            waymark_nonzero_duration::NonZeroDuration::from_millis(2_500).expect("non-zero"),
        ),
    };

    let read_params = || Params {
        config: &config,
        transactions: Transactions::ReadOnly,
        schema: Some(Schema::Require(READ_TEST_SCHEMA.into())),
    };

    let error = pool(read_params())
        .await
        .expect_err("nothing provisioned the schema");
    assert!(
        matches!(
            error,
            PoolError::ConnectRequiringSchema(
                waymark_sqlx_postgres_schema_pool::ExistingSchemaError::Missing { .. }
            )
        ),
        "{error}"
    );
    let (created,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
    )
    .bind(READ_TEST_SCHEMA)
    .fetch_one(&bootstrap)
    .await
    .expect("look the schema up");
    assert!(!created, "the read pool created the schema");

    // A creating pool provisions; the read pool then finds it.
    pool(Params {
        config: &config,
        transactions: Transactions::Unrestricted,
        schema: Some(Schema::Create(READ_TEST_SCHEMA.into())),
    })
    .await
    .expect("a creating pool creates the schema");
    let read_pool = pool(read_params())
        .await
        .expect("the read pool finds the schema");
    let (current_schema,): (String,) = sqlx::query_as("SELECT current_schema()")
        .fetch_one(&read_pool)
        .await
        .expect("read current schema");
    assert_eq!(current_schema, READ_TEST_SCHEMA);
    let (statement_timeout,): (String,) =
        sqlx::query_as("SELECT current_setting('statement_timeout')")
            .fetch_one(&read_pool)
            .await
            .expect("read statement_timeout");
    assert_eq!(statement_timeout, "2500ms");

    // A write through the read pool is refused, even on the primary.
    let error = sqlx::query("CREATE TABLE postgres_bringup_read_probe (id int)")
        .execute(&read_pool)
        .await
        .expect_err("the read pool refuses writes");
    assert!(
        error
            .as_database_error()
            .is_some_and(|error| error.code().as_deref() == Some("25006")),
        "a read_only_sql_transaction error: {error}"
    );

    sqlx::query(const_format::formatcp!(
        r#"DROP SCHEMA "{READ_TEST_SCHEMA}" CASCADE"#
    ))
    .execute(&bootstrap)
    .await
    .expect("drop test schema");
}

#[tokio::test]
async fn a_creating_connection_creates_the_schema_and_carries_its_timeout() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(const_format::formatcp!(
        r#"DROP SCHEMA IF EXISTS "{CONNECTION_TEST_SCHEMA}" CASCADE"#
    ))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");

    let config = connection::Config {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        statement_timeout: Some(
            waymark_nonzero_duration::NonZeroDuration::from_millis(7_500).expect("non-zero"),
        ),
    };
    let mut connection = connection(ConnectionParams {
        config: &config,
        transactions: Transactions::Unrestricted,
        schema: Some(Schema::Create(CONNECTION_TEST_SCHEMA.into())),
    })
    .await
    .expect("the connection comes up");

    let (current_schema,): (String,) = sqlx::query_as("SELECT current_schema()")
        .fetch_one(&mut connection)
        .await
        .expect("read current schema");
    assert_eq!(current_schema, CONNECTION_TEST_SCHEMA);
    let (statement_timeout,): (String,) =
        sqlx::query_as("SELECT current_setting('statement_timeout')")
            .fetch_one(&mut connection)
            .await
            .expect("read statement_timeout");
    assert_eq!(statement_timeout, "7500ms");

    sqlx::query(const_format::formatcp!(
        r#"DROP SCHEMA "{CONNECTION_TEST_SCHEMA}" CASCADE"#
    ))
    .execute(&bootstrap)
    .await
    .expect("drop test schema");
}

#[tokio::test]
async fn migrate_creates_the_schema_runs_and_closes() {
    let bootstrap = waymark_support_test::postgres_setup().await;
    sqlx::query(const_format::formatcp!(
        r#"DROP SCHEMA IF EXISTS "{MIGRATE_TEST_SCHEMA}" CASCADE"#
    ))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");

    let config = connection::Config {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        statement_timeout: None,
    };

    let mut migrations_pid = None;
    migrate(
        &config,
        Some(MIGRATE_TEST_SCHEMA.into()),
        async |connection: &mut sqlx::PgConnection| {
            let (pid,): (i32,) = sqlx::query_as("SELECT pg_backend_pid()")
                .fetch_one(&mut *connection)
                .await?;
            migrations_pid = Some(pid);

            sqlx::query("CREATE TABLE postgres_bringup_migrated (id int)")
                .execute(&mut *connection)
                .await?;

            Ok::<(), sqlx::Error>(())
        },
    )
    .await
    .expect("the migrations run");

    let (migrated_schema,): (String,) = sqlx::query_as(
        "SELECT table_schema::text FROM information_schema.tables WHERE table_name = 'postgres_bringup_migrated'",
    )
    .fetch_one(&bootstrap)
    .await
    .expect("locate the migrated table");
    assert_eq!(migrated_schema, MIGRATE_TEST_SCHEMA);

    let migrations_pid = migrations_pid.expect("the run saw its connection");
    assert_backend_gone(&bootstrap, migrations_pid).await;

    sqlx::query(const_format::formatcp!(
        r#"DROP SCHEMA "{MIGRATE_TEST_SCHEMA}" CASCADE"#
    ))
    .execute(&bootstrap)
    .await
    .expect("drop test schema");
}

#[tokio::test]
async fn a_failed_migration_is_reported_and_its_connection_closed() {
    let bootstrap = waymark_support_test::postgres_setup().await;

    let config = connection::Config {
        url: waymark_support_integration::LOCAL_POSTGRES_DSN.into(),
        statement_timeout: None,
    };

    let mut migrations_pid = None;
    let error = migrate(
        &config,
        None,
        async |connection: &mut sqlx::PgConnection| {
            let (pid,): (i32,) = sqlx::query_as("SELECT pg_backend_pid()")
                .fetch_one(&mut *connection)
                .await?;
            migrations_pid = Some(pid);

            sqlx::query("SELECT * FROM postgres_bringup_no_such_table")
                .execute(&mut *connection)
                .await?;

            Ok::<(), sqlx::Error>(())
        },
    )
    .await
    .expect_err("the failing migration is reported");
    assert!(matches!(error, MigrateError::Run(_)), "{error}");

    let migrations_pid = migrations_pid.expect("the run saw its connection");
    assert_backend_gone(&bootstrap, migrations_pid).await;
}

/// Wait for the server to drop the backend `pid`; a closed connection's
/// backend exits shortly after the close.
async fn assert_backend_gone(bootstrap: &sqlx::PgPool, pid: i32) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

    loop {
        let (alive,): (bool,) =
            sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
                .bind(pid)
                .fetch_one(bootstrap)
                .await
                .expect("look the backend up");

        if !alive {
            return;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "the migrations connection's backend {pid} is still there"
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// The statement timeout `connection` reads back on a connection to `url`
/// with `statement_timeout`.
async fn connection_statement_timeout(
    url: &str,
    statement_timeout: Option<waymark_nonzero_duration::NonZeroDuration>,
) -> String {
    let config = connection::Config {
        url: url.into(),
        statement_timeout,
    };
    let mut connection = connection(ConnectionParams {
        config: &config,
        transactions: Transactions::Unrestricted,
        schema: None,
    })
    .await
    .expect("the connection comes up");

    let (statement_timeout,): (String,) =
        sqlx::query_as("SELECT current_setting('statement_timeout')")
            .fetch_one(&mut connection)
            .await
            .expect("read statement_timeout");

    statement_timeout
}

#[tokio::test]
async fn the_config_statement_timeout_wins_over_the_url_default() {
    waymark_support_test::postgres_setup().await;

    // The URL carries a default of its own, as a role or a server would.
    let url = format!(
        "{}?options=-c%20statement_timeout%3D5000",
        waymark_support_integration::LOCAL_POSTGRES_DSN.expose_secret()
    );

    let mut plain = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .expect("a plain connection comes up");
    let (url_default,): (String,) = sqlx::query_as("SELECT current_setting('statement_timeout')")
        .fetch_one(&mut plain)
        .await
        .expect("read statement_timeout");
    assert_eq!(url_default, "5s");

    let off = connection_statement_timeout(&url, None).await;
    assert_eq!(off, "0");

    let set = connection_statement_timeout(
        &url,
        Some(waymark_nonzero_duration::NonZeroDuration::from_millis(7_500).expect("non-zero")),
    )
    .await;
    assert_eq!(set, "7500ms");
}
