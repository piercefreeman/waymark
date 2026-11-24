//! Tests of the SQLx migrations ledger transition file against the local
//! test Postgres, each in a database of its own: the file runs as it
//! ships.

use sha2::Digest as _;
use waymark_migrations_engine::MigrationsComparison;
use waymark_migrations_engine_backend::SetupMigrationsEngine as _;
use waymark_migrations_engine_migration::HasMigrationIdentity as _;
use waymark_migrations_source::MigrationSource;

/// The transition file, as it ships.
const TRANSITION: &str = include_str!("../transitions/sqlx_migrations_ledger.sql");

/// The schema the main database's tables are in.
const SCHEMA: &str = "public";

/// The schema the migrations engine is set up in, to compare with.
const REFERENCE_SCHEMA: &str = "migrations_engine_reference";

/// The status of the migrations as the migrations engine tells it.
type Status = Result<
    MigrationsComparison,
    waymark_migrations_engine::migrations_status::ErrorFor<
        waymark_migrations_engine_backend_sqlx_postgres::Backend<super::Tables>,
        super::Assertions,
    >,
>;

/// Make the database `database` afresh, empty, and return the connect
/// options of it.
async fn fresh_database(database: &str) -> sqlx::postgres::PgConnectOptions {
    let bootstrap = waymark_support_test::postgres_setup().await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP DATABASE IF EXISTS "{database}" WITH (FORCE)
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop leftover test database");

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
CREATE DATABASE "{database}" TEMPLATE template0
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("create test database");

    let options: sqlx::postgres::PgConnectOptions = waymark_support_integration::LOCAL_POSTGRES_DSN
        .expose_secret()
        .parse()
        .expect("parse test DSN");

    options.database(database)
}

/// Drop the test database `database`.
async fn drop_database(database: &str) {
    let bootstrap = waymark_support_test::postgres_setup().await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP DATABASE "{database}" WITH (FORCE)
        "#
    )))
    .execute(&bootstrap)
    .await
    .expect("drop test database");
}

/// A connection to the test database of `options`, as `psql` makes one.
async fn connect(options: &sqlx::postgres::PgConnectOptions) -> sqlx::PgConnection {
    <sqlx::PgConnection as sqlx::Connection>::connect_with(options)
        .await
        .expect("connect to the test database")
}

/// A connection to the test database of `options`, scoped to `schema`.
async fn connect_scoped(
    options: &sqlx::postgres::PgConnectOptions,
    schema: &str,
) -> sqlx::PgConnection {
    connect(&options.clone().options([("search_path", schema)])).await
}

/// The content of an SQL file as Waymark 0.30 had it, out of `sql`, its
/// content now: without the metadata header, which the file gained later.
fn content_as_of_waymark_0_30(sql: &str) -> &str {
    let (_metadata_header, rest) = sql
        .split_once("\n\n")
        .expect("the file has a metadata header");

    rest
}

/// Put, through `connection`, a ledger as the SQLx migrator of Waymark
/// 0.30 left it: a successful row for every SQL-file migration, with the
/// SHA-384 checksum of the file as that release had it.
async fn plant_sqlx_migrations_ledger(connection: &mut sqlx::PgConnection) {
    sqlx::query(
        r#"
CREATE TABLE _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    success BOOLEAN NOT NULL,
    checksum BYTEA NOT NULL,
    execution_time BIGINT NOT NULL
)
        "#,
    )
    .execute(&mut *connection)
    .await
    .expect("create the SQLx migrations ledger");

    for migration in &super::migrations() {
        let MigrationSource::SqlFile(migration) = migration else {
            continue;
        };
        let identity = migration.migration_identity();
        let checksum = sha2::Sha384::digest(content_as_of_waymark_0_30(migration.sql).as_bytes());

        sqlx::query(
            r#"
INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
VALUES ($1, $2, TRUE, $3, 0)
            "#,
        )
        .bind(i64::from(identity.version.to_raw()))
        .bind(identity.description)
        .bind(checksum.as_slice())
        .execute(&mut *connection)
        .await
        .expect("record a migration in the SQLx migrations ledger");
    }
}

/// How many SQL-file migrations the main database has.
fn sql_file_migrations() -> usize {
    super::migrations()
        .iter()
        .filter(|migration| matches!(migration, MigrationSource::SqlFile(_)))
        .count()
}

/// The status of the main database's migrations, through a connection of
/// its own to the test database of `options`.
async fn migrations_status(options: &sqlx::postgres::PgConnectOptions) -> Status {
    let mut backend =
        waymark_migrations_engine_backend_sqlx_postgres::Backend::<super::Tables>::new(
            connect_scoped(options, SCHEMA).await,
        );

    let status = waymark_migrations_engine::migrations_status(
        &mut backend,
        &super::assertions(),
        &waymark_migrations_engine::validate_migrations_array(super::migrations())
            .expect("the array is in the order of its versions"),
    )
    .await;

    sqlx::Connection::close(backend.into_connection())
        .await
        .expect("close the connection");

    status
}

/// Whether the table `table` is in `schema`, as `connection` sees it.
async fn table_exists(connection: &mut sqlx::PgConnection, schema: &str, table: &str) -> bool {
    sqlx::query_scalar(
        r#"
SELECT to_regclass(format('%I.%I', $1, $2)) IS NOT NULL
        "#,
    )
    .bind(schema)
    .bind(table)
    .fetch_one(connection)
    .await
    .expect("look the table up")
}

/// A row of the migrations engine's ledger without the time it was
/// written at: the version, the description, the revision, the source
/// digest and the state.
type LedgerRow = (i64, String, String, String, String);

/// The rows of the migrations engine's ledger in `schema`, in the order
/// of their versions.
async fn engine_ledger_rows(connection: &mut sqlx::PgConnection, schema: &str) -> Vec<LedgerRow> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        r#"
SELECT version, description, revision, applied_source_digest, state
FROM "{schema}"."{ledger}"
ORDER BY version
        "#,
        ledger = <super::Tables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LEDGER,
    )))
    .fetch_all(connection)
    .await
    .expect("read the ledger")
}

/// The rows the migrations engine writes into its ledger as it applies
/// the SQL-file migrations of this build to a database, once each is
/// finished.
fn ledger_rows_of_a_migrated_database() -> Vec<LedgerRow> {
    super::migrations()
        .iter()
        .filter_map(|migration| match migration {
            MigrationSource::SqlFile(migration) => Some(migration.migration_identity()),
            MigrationSource::RustCode(_) => None,
        })
        .map(|identity| {
            (
                i64::from(identity.version.to_raw()),
                identity.description.to_owned(),
                identity.revision.as_raw().to_owned(),
                identity.source_digest.to_owned(),
                "finished".to_owned(),
            )
        })
        .collect()
}

/// The definitions of the migrations engine's two tables in `schema`:
/// every column, every constraint and every comment, in words.
async fn engine_tables_definitions(
    connection: &mut sqlx::PgConnection,
    schema: &str,
) -> Vec<String> {
    let tables = [
        <super::Tables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LEDGER,
        <super::Tables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LOCKS,
    ];

    let mut definitions: Vec<String> = sqlx::query_scalar(
        r#"
SELECT concat_ws(
    ' | ',
    'column',
    table_name,
    column_name,
    data_type,
    is_nullable,
    coalesce(column_default, '-'),
    is_identity,
    coalesce(identity_generation, '-'),
    is_generated,
    coalesce(generation_expression, '-')
)
FROM information_schema.columns
WHERE table_schema = $1
    AND table_name = ANY($2)
ORDER BY table_name, ordinal_position
        "#,
    )
    .bind(schema)
    .bind(tables.as_slice())
    .fetch_all(&mut *connection)
    .await
    .expect("read the columns");

    let constraints: Vec<String> = sqlx::query_scalar(
        r#"
SELECT concat_ws(
    ' | ',
    'constraint',
    tables.relname,
    constraints.contype::text,
    pg_get_constraintdef(constraints.oid)
)
FROM pg_constraint AS constraints
JOIN pg_class AS tables ON tables.oid = constraints.conrelid
JOIN pg_namespace AS schemas ON schemas.oid = tables.relnamespace
WHERE schemas.nspname = $1
    AND tables.relname = ANY($2)
ORDER BY 1
        "#,
    )
    .bind(schema)
    .bind(tables.as_slice())
    .fetch_all(&mut *connection)
    .await
    .expect("read the constraints");

    definitions.extend(constraints);

    // A table's own comment is under the column number 0.
    let comments: Vec<String> = sqlx::query_scalar(
        r#"
SELECT concat_ws(
    ' | ',
    'comment',
    tables.relname,
    coalesce(columns.attname::text, '-'),
    comments.description
)
FROM pg_description AS comments
JOIN pg_class AS tables
    ON tables.oid = comments.objoid
    AND comments.classoid = 'pg_class'::regclass
JOIN pg_namespace AS schemas ON schemas.oid = tables.relnamespace
LEFT JOIN pg_attribute AS columns
    ON columns.attrelid = tables.oid
    AND columns.attnum = comments.objsubid
    AND comments.objsubid <> 0
WHERE schemas.nspname = $1
    AND tables.relname = ANY($2)
ORDER BY 1
        "#,
    )
    .bind(schema)
    .bind(tables.as_slice())
    .fetch_all(&mut *connection)
    .await
    .expect("read the comments");

    definitions.extend(comments);

    definitions
}

#[tokio::test]
async fn transition_moves_the_sqlx_migrations_ledger_into_the_engine_ledger() {
    let database = "waymark_test_main_ledger_transition";
    let options = fresh_database(database).await;
    let mut connection = connect(&options).await;
    plant_sqlx_migrations_ledger(&mut connection).await;

    let before = migrations_status(&options).await;

    sqlx::raw_sql(TRANSITION)
        .execute(&mut connection)
        .await
        .expect("the transition file runs");

    let after = migrations_status(&options).await;

    // The tables the file made are the ones the migrations engine sets up.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
CREATE SCHEMA "{REFERENCE_SCHEMA}"
        "#
    )))
    .execute(&mut connection)
    .await
    .expect("create the reference schema");
    let mut reference =
        waymark_migrations_engine_backend_sqlx_postgres::Backend::<super::Tables>::new(
            connect_scoped(&options, REFERENCE_SCHEMA).await,
        );
    reference
        .setup_migrations_engine()
        .await
        .expect("the engine is set up");
    sqlx::Connection::close(reference.into_connection())
        .await
        .expect("close the connection");
    let made_by_the_file = engine_tables_definitions(&mut connection, SCHEMA).await;
    let set_up_by_the_engine = engine_tables_definitions(&mut connection, REFERENCE_SCHEMA).await;

    assert!(matches!(
        before,
        Err(
            waymark_migrations_engine::migrations_status::Error::Assertions(
                waymark_migrations_engine::migrations_engine_assertions::Error::Violated { .. }
            )
        )
    ));
    let Ok(MigrationsComparison::InOrder { applied }) = after else {
        panic!("the engine reads the ledger the file wrote: {after:?}");
    };
    assert_eq!(applied, sql_file_migrations());
    assert!(!table_exists(&mut connection, SCHEMA, "_sqlx_migrations").await);
    assert!(!made_by_the_file.is_empty());
    assert_eq!(made_by_the_file, set_up_by_the_engine);

    // The ledger the file wrote is the one of a database this build
    // migrated itself.
    assert_eq!(
        engine_ledger_rows(&mut connection, SCHEMA).await,
        ledger_rows_of_a_migrated_database()
    );

    sqlx::Connection::close(connection)
        .await
        .expect("close the connection");
    drop_database(database).await;
}

#[tokio::test]
async fn transition_changes_nothing_when_the_ledger_is_not_as_waymark_0_30_leaves_it() {
    let database = "waymark_test_main_ledger_transition_refused";
    let options = fresh_database(database).await;
    let mut connection = connect(&options).await;

    // Each is a statement that makes the ledger another one than Waymark
    // 0.30 leaves, and the versions the file then says differ.
    let cases = [
        (
            "a migration that did not succeed",
            r#"
UPDATE _sqlx_migrations
SET success = FALSE
WHERE version = 3
            "#,
            "3",
        ),
        (
            "a migration with another checksum",
            r#"
UPDATE _sqlx_migrations
SET checksum = '\x00'
WHERE version = 2
            "#,
            "2",
        ),
        (
            "the ledger of an earlier release, without the last migration",
            r#"
DELETE FROM _sqlx_migrations
WHERE version = 26
            "#,
            "26",
        ),
        (
            "migrations Waymark 0.30 does not have",
            r#"
INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
VALUES
    (100, 'unknown', TRUE, '\x00', 0),
    (99, 'unknown', TRUE, '\x00', 0)
            "#,
            "99, 100",
        ),
    ];

    for (case, statement, differing) in cases {
        plant_sqlx_migrations_ledger(&mut connection).await;
        sqlx::query(statement)
            .execute(&mut connection)
            .await
            .expect("make the ledger another one");

        let refused = sqlx::raw_sql(TRANSITION).execute(&mut connection).await;

        // The file stops at its first error, with its transaction open:
        // `psql` goes on to the `COMMIT`, which rolls it back.
        sqlx::query(
            r#"
ROLLBACK
            "#,
        )
        .execute(&mut connection)
        .await
        .expect("roll the file's transaction back");

        let refused = refused.expect_err("the file refuses the ledger");
        let message = refused
            .as_database_error()
            .map(|error| error.message().to_owned());
        assert_eq!(
            message,
            Some(format!(
                "_sqlx_migrations is not as Waymark 0.30 leaves it; the migrations that differ: \
                 {differing}"
            )),
            "{case}"
        );
        assert!(
            table_exists(&mut connection, SCHEMA, "_sqlx_migrations").await,
            "{case}"
        );
        assert!(
            !table_exists(
                &mut connection,
                SCHEMA,
                <super::Tables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LEDGER
            )
            .await,
            "{case}"
        );
        assert!(
            !table_exists(
                &mut connection,
                SCHEMA,
                <super::Tables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LOCKS
            )
            .await,
            "{case}"
        );

        sqlx::query(
            r#"
DROP TABLE _sqlx_migrations
            "#,
        )
        .execute(&mut connection)
        .await
        .expect("drop the ledger of the case");
    }

    sqlx::Connection::close(connection)
        .await
        .expect("close the connection");
    drop_database(database).await;
}
