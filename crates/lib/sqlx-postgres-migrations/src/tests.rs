use super::*;

const CREATE_ONE: &str = r#"
CREATE TABLE one (id int)
"#;

const CREATE_TWO: &str = r#"
CREATE TABLE two (id int)
"#;

const CREATE_ONE_CHANGED: &str = r#"
CREATE TABLE one (id bigint)
"#;

fn migration(version: i64, sql: &'static str) -> sqlx::migrate::Migration {
    sqlx::migrate::Migration::new(
        version,
        "test".into(),
        sqlx::migrate::MigrationType::Simple,
        sqlx::SqlSafeStr::into_sql_str(sql),
        false,
    )
}

/// The connect options of the test database, every connection scoped to
/// `schema`.
fn scoped_options(schema: &str) -> sqlx::postgres::PgConnectOptions {
    let options: sqlx::postgres::PgConnectOptions = waymark_support_integration::LOCAL_POSTGRES_DSN
        .expose_secret()
        .parse()
        .expect("parse test DSN");

    options.options([("search_path", schema)])
}

/// A connection to a fresh, empty `schema`, which it is scoped to: the
/// migrations table and the migrations' tables land there.
async fn scoped_connection(schema: &str) -> sqlx::PgConnection {
    let bootstrap = waymark_support_test::postgres_setup().await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA IF EXISTS "{schema}" CASCADE
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test schema");

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
CREATE SCHEMA "{schema}"
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("create test schema");

    <sqlx::PgConnection as sqlx::Connection>::connect_with(&scoped_options(schema))
        .await
        .expect("connect to the test schema")
}

/// Drop the test `schema` and everything in it.
async fn drop_schema(schema: &str) {
    let bootstrap = waymark_support_test::postgres_setup().await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA "{schema}" CASCADE
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop test schema");
}

#[tokio::test]
async fn exact_match_passes() {
    let mut connection = scoped_connection("migrations_check_exact").await;
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![
        migration(1, CREATE_ONE),
        migration(2, CREATE_TWO),
    ]);
    migrator.run(&mut connection).await.expect("migrate");

    check(&migrator, &mut connection)
        .await
        .expect("an exactly migrated database passes");

    drop_schema("migrations_check_exact").await;
}

#[tokio::test]
async fn never_migrated_is_refused() {
    let mut connection = scoped_connection("migrations_check_never").await;
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);

    let error = check(&migrator, &mut connection)
        .await
        .expect_err("a never migrated database is refused");

    assert!(matches!(error, CheckError::NotMigrated), "{error:?}");

    drop_schema("migrations_check_never").await;
}

#[tokio::test]
async fn pending_migration_is_refused_and_nothing_changes() {
    let mut connection = scoped_connection("migrations_check_pending").await;
    let older = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);
    older.run(&mut connection).await.expect("migrate");
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![
        migration(1, CREATE_ONE),
        migration(2, CREATE_TWO),
    ]);

    let error = check(&migrator, &mut connection)
        .await
        .expect_err("an older database is refused");

    assert!(matches!(error, CheckError::Pending(2)), "{error:?}");

    let applied: i64 = sqlx::query_scalar(
        r#"
SELECT count(*)
FROM _sqlx_migrations
        "#,
    )
    .fetch_one(&mut connection)
    .await
    .expect("count applied migrations");
    assert_eq!(applied, 1);

    let created: bool = sqlx::query_scalar(
        r#"
SELECT to_regclass('two') IS NOT NULL
        "#,
    )
    .fetch_one(&mut connection)
    .await
    .expect("look for the pending migration's table");
    assert!(!created);

    drop_schema("migrations_check_pending").await;
}

#[tokio::test]
async fn refusal_releases_the_lock() {
    let mut connection = scoped_connection("migrations_check_lock").await;
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);

    let error = check(&migrator, &mut connection)
        .await
        .expect_err("a never migrated database is refused");
    assert!(matches!(error, CheckError::NotMigrated), "{error:?}");

    let held: i64 = sqlx::query_scalar(
        r#"
SELECT count(*)
FROM pg_locks
WHERE locktype = 'advisory'
    AND pid = pg_backend_pid()
        "#,
    )
    .fetch_one(&mut connection)
    .await
    .expect("count the session's advisory locks");
    assert_eq!(held, 0);

    drop_schema("migrations_check_lock").await;
}

#[tokio::test]
async fn a_held_migrations_lock_is_waited_for() {
    let mut connection = scoped_connection("migrations_check_wait").await;
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);
    migrator.run(&mut connection).await.expect("migrate");

    // A migration in progress elsewhere holds the lock.
    let mut migrating = <sqlx::PgConnection as sqlx::Connection>::connect_with(&scoped_options(
        "migrations_check_wait",
    ))
    .await
    .expect("connect the migrating session");
    sqlx::migrate::Migrate::lock(&mut migrating)
        .await
        .expect("take the migrations lock");

    let (checker_pid,): (i32,) = sqlx::query_as("SELECT pg_backend_pid()")
        .fetch_one(&mut connection)
        .await
        .expect("read the checking session's pid");

    // The check waits once its lock request shows as not granted; the
    // outer timeout only guards against a hang.
    let queued = async {
        loop {
            let requests: i64 = sqlx::query_scalar(
                r#"
SELECT count(*)
FROM pg_locks
WHERE locktype = 'advisory'
    AND NOT granted
    AND pid = $1
                "#,
            )
            .bind(checker_pid)
            .fetch_one(&mut migrating)
            .await
            .expect("look the lock request up");

            if requests == 1 {
                return;
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };

    let raced = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            checked = check(&migrator, &mut connection) => Err(checked),
            () = queued => Ok(()),
        }
    })
    .await;

    match raced {
        Ok(Ok(())) => {}
        Ok(Err(checked)) => panic!("the check did not wait: {checked:?}"),
        Err(_) => panic!("the check's lock request never showed up"),
    }

    // Its lock request is still queued: drop the connection, so its
    // backend exits once granted the lock instead of holding it.
    drop(connection);

    sqlx::migrate::Migrate::unlock(&mut migrating)
        .await
        .expect("release the migrations lock");

    let mut connection = <sqlx::PgConnection as sqlx::Connection>::connect_with(&scoped_options(
        "migrations_check_wait",
    ))
    .await
    .expect("connect again");
    check(&migrator, &mut connection)
        .await
        .expect("the check passes once the lock is released");

    drop_schema("migrations_check_wait").await;
}

#[tokio::test]
async fn read_rights_alone_are_enough() {
    let mut connection = scoped_connection("migrations_check_reader").await;
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);
    migrator.run(&mut connection).await.expect("migrate");

    let bootstrap = waymark_support_test::postgres_setup().await;
    for statement in [
        "DROP ROLE IF EXISTS migrations_check_reader",
        "CREATE ROLE migrations_check_reader LOGIN PASSWORD 'reader'",
        "GRANT USAGE ON SCHEMA migrations_check_reader TO migrations_check_reader",
        "GRANT SELECT ON migrations_check_reader._sqlx_migrations TO migrations_check_reader",
    ] {
        sqlx::query(statement)
            .execute(&bootstrap)
            .await
            .expect(statement);
    }

    // Every transaction read-only on top of the missing rights: the check
    // must not write at all.
    let options = scoped_options("migrations_check_reader")
        .username("migrations_check_reader")
        .password("reader")
        .options([("default_transaction_read_only", "on")]);
    let mut reader = <sqlx::PgConnection as sqlx::Connection>::connect_with(&options)
        .await
        .expect("connect as the reader");

    check(&migrator, &mut reader)
        .await
        .expect("the check needs only read rights");

    drop_schema("migrations_check_reader").await;

    sqlx::query("DROP ROLE migrations_check_reader")
        .execute(&bootstrap)
        .await
        .expect("drop the reader role");
}

#[tokio::test]
async fn newer_database_is_refused() {
    let mut connection = scoped_connection("migrations_check_newer").await;
    let newer = sqlx::migrate::Migrator::with_migrations(vec![
        migration(1, CREATE_ONE),
        migration(2, CREATE_TWO),
    ]);
    newer.run(&mut connection).await.expect("migrate");
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);

    let error = check(&migrator, &mut connection)
        .await
        .expect_err("a newer database is refused");

    assert!(
        matches!(
            error,
            CheckError::Migrate(sqlx::migrate::MigrateError::VersionMissing(2))
        ),
        "{error:?}"
    );

    drop_schema("migrations_check_newer").await;
}

#[tokio::test]
async fn changed_migration_is_refused() {
    let mut connection = scoped_connection("migrations_check_changed").await;
    let applied = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);
    applied.run(&mut connection).await.expect("migrate");
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE_CHANGED)]);

    let error = check(&migrator, &mut connection)
        .await
        .expect_err("a changed migration is refused");

    assert!(
        matches!(
            error,
            CheckError::Migrate(sqlx::migrate::MigrateError::VersionMismatch(1))
        ),
        "{error:?}"
    );

    drop_schema("migrations_check_changed").await;
}

#[tokio::test]
async fn dirty_migration_is_refused() {
    let mut connection = scoped_connection("migrations_check_dirty").await;
    let migrator = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);
    migrator.run(&mut connection).await.expect("migrate");
    sqlx::query(
        r#"
UPDATE _sqlx_migrations
SET success = false
WHERE version = 1
        "#,
    )
    .execute(&mut connection)
    .await
    .expect("mark the migration failed");

    let error = check(&migrator, &mut connection)
        .await
        .expect_err("a dirty migration is refused");

    assert!(
        matches!(
            error,
            CheckError::Migrate(sqlx::migrate::MigrateError::Dirty(1))
        ),
        "{error:?}"
    );

    drop_schema("migrations_check_dirty").await;
}

#[tokio::test]
async fn schema_to_create_is_refused_only_when_missing() {
    let mut connection = scoped_connection("migrations_check_schemas").await;
    let mut migrator = sqlx::migrate::Migrator::with_migrations(vec![migration(1, CREATE_ONE)]);
    migrator.run(&mut connection).await.expect("migrate");

    migrator.create_schema("migrations_check_schemas");

    check(&migrator, &mut connection)
        .await
        .expect("an existing schema passes");

    // Unquoted, the migrator would create it case-folded: the same schema.
    migrator.create_schema("Migrations_Check_Schemas");

    check(&migrator, &mut connection)
        .await
        .expect("an existing schema passes under any unquoted case");

    migrator.create_schema("migrations_check_schemas_missing");

    let error = check(&migrator, &mut connection)
        .await
        .expect_err("a missing schema is refused");

    assert!(
        matches!(&error, CheckError::CreateSchema(schema) if schema == "migrations_check_schemas_missing"),
        "{error:?}"
    );

    drop_schema("migrations_check_schemas").await;
}
