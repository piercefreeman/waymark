//! Tests of the migrations entry points against the local test Postgres,
//! each in a schema of its own.

use std::num::NonZeroU64;

use waymark_migrations_engine::migrations_lock_keeper::{TakeExclusiveError, TakeSharedError};
use waymark_migrations_engine::run_migrations::Outcome;
use waymark_migrations_engine::{
    MigrationsLockTiming, MigrationsRunTransactions, ValidMigrationsArray,
};
use waymark_migrations_engine_backend_sqlx_postgres::SqlxMigrationsLedgerAbsentAssertion;
use waymark_migrations_engine_migration::{
    MigrationRevision, MigrationTransactionSupport, MigrationVersion,
};
use waymark_nonzero_duration::NonZeroDuration;
use waymark_postgres_config::{connection, migrations_lock};
use waymark_secret_string::SecretString;

use crate::{
    HeldSharedMigrationsLock, MigrateUnderExclusiveMigrationsLockError,
    keep_shared_migrations_lock, migrate_and_release_migrations_lock,
    migrate_and_take_shared_migrations_lock, shared_migrations_locks_keeping,
    take_shared_migrations_lock_and_check,
};

/// The tables of the tests.
struct TestTables;

impl waymark_migrations_engine_backend_sqlx_postgres::Tables for TestTables {
    const LEDGER: &'static str = "_bringup_test_migrations";

    const LOCKS: &'static str = "_bringup_test_migrations_locks";
}

/// A migration of the tests: an SQL-file one, running through a Postgres
/// connection.
type TestMigration = waymark_migrations_sql_file::Migration<sqlx::PgConnection>;

/// A migration of `version` whose run is `sql`, able to run inside a
/// transaction.
fn migration(version: u32, sql: &'static str) -> TestMigration {
    waymark_migrations_sql_file::Migration {
        version: MigrationVersion::from_raw(version),
        description: "test migration",
        sql,
        revision: MigrationRevision::from_raw(format!("test-migration-{version}")),
        source_digest: "sha256:test",
        transaction_support: MigrationTransactionSupport::Supported,
        connection_type: core::marker::PhantomData,
    }
}

/// The first test migration: it creates the table `one`.
fn create_one() -> TestMigration {
    migration(
        1,
        r#"
CREATE TABLE one (id INT)
        "#,
    )
}

/// The second test migration: it creates the table `two`.
fn create_two() -> TestMigration {
    migration(
        2,
        r#"
CREATE TABLE two (id INT)
        "#,
    )
}

/// The URL of the test database.
fn url() -> SecretString {
    waymark_support_integration::LOCAL_POSTGRES_DSN.into()
}

/// The config of a connection to the test database, for migrations and
/// for checks.
fn connection_config() -> connection::Config {
    connection::Config {
        url: url(),
        statement_timeout: NonZeroDuration::from_secs(10),
    }
}

/// A migrations lock that is renewed every 200 ms and expires 5 s after a
/// renewal.
fn lock_config() -> migrations_lock::Config {
    let millis =
        |millis| NonZeroDuration::from_nonzero_millis(NonZeroU64::new(millis).expect("non-zero"));

    let timing = MigrationsLockTiming {
        heartbeat: millis(200),
        time_to_live: millis(5_000),
        fencing_margin: millis(500),
    };

    migrations_lock::Config {
        timing: timing.validate().expect("the timing is valid"),
        statement_timeout: Some(millis(2_000)),
    }
}

/// The holder id with the value `value`.
fn holder(value: u128) -> waymark_migrations_engine_backend::migrations_lock::HolderId {
    waymark_migrations_engine_backend::migrations_lock::HolderId::from_raw(uuid::Uuid::from_u128(
        value,
    ))
}

/// `migrations` as a valid migrations array; the tests give them in the
/// order of their versions.
fn valid(migrations: &[TestMigration]) -> ValidMigrationsArray<&[TestMigration]> {
    waymark_migrations_engine::validate_migrations_array(migrations)
        .expect("the test migrations are in the order of their versions")
}

/// Drop `schema` if a test left it behind.
async fn drop_leftover_schema(bootstrap: &sqlx::PgPool, schema: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA IF EXISTS "{schema}" CASCADE
        "#
    )))
    .execute(bootstrap)
    .await
    .expect("drop leftover test schema");
}

/// Make `schema` afresh, empty.
async fn fresh_schema(bootstrap: &sqlx::PgPool, schema: &str) {
    drop_leftover_schema(bootstrap, schema).await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
CREATE SCHEMA "{schema}"
        "#
    )))
    .execute(bootstrap)
    .await
    .expect("create test schema");
}

/// Drop the test `schema` and everything in it.
async fn drop_schema(bootstrap: &sqlx::PgPool, schema: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DROP SCHEMA "{schema}" CASCADE
        "#
    )))
    .execute(bootstrap)
    .await
    .expect("drop test schema");
}

/// The migrations locks in `schema`: each holder's value and mode, in the
/// order of the holders.
async fn locks(bootstrap: &sqlx::PgPool, schema: &str) -> Vec<(u128, String)> {
    let rows: Vec<(uuid::Uuid, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        r#"
SELECT holder_id, mode
FROM "{schema}"."{locks}"
ORDER BY holder_id
        "#,
        locks = <TestTables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LOCKS,
    )))
    .fetch_all(bootstrap)
    .await
    .expect("read the migrations locks");

    rows.into_iter()
        .map(|(holder_id, mode)| (holder_id.as_u128(), mode))
        .collect()
}

/// The versions the migrations ledger in `schema` has, in order.
async fn ledger_versions(bootstrap: &sqlx::PgPool, schema: &str) -> Vec<i64> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        r#"
SELECT version
FROM "{schema}"."{ledger}"
ORDER BY version
        "#,
        ledger = <TestTables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LEDGER,
    )))
    .fetch_all(bootstrap)
    .await
    .expect("read the migrations ledger")
}

/// Whether `schema` has a table of `table`.
async fn table_exists(bootstrap: &sqlx::PgPool, schema: &str, table: &str) -> bool {
    sqlx::query_scalar(
        r#"
SELECT to_regclass(format('%I.%I', $1, $2)) IS NOT NULL
        "#,
    )
    .bind(schema)
    .bind(table)
    .fetch_one(bootstrap)
    .await
    .expect("look the table up")
}

/// The type of the assertions of the tests' database.
type TestAssertions = (SqlxMigrationsLedgerAbsentAssertion,);

/// The assertions of the tests' database.
const ASSERTIONS: TestAssertions = (SqlxMigrationsLedgerAbsentAssertion,);

/// The error of checking the assertions of the tests.
type AssertionsCheckError = waymark_migrations_engine_assertion::CheckErrorFor<TestAssertions>;

/// The error of a migrations run of the tests.
type RunError = waymark_migrations_engine::run_migrations::ErrorFor<
    waymark_migrations_engine_backend_sqlx_postgres::Backend<TestTables>,
    TestAssertions,
    TestMigration,
>;

/// Migrate `schema` to `migrations` and take a shared lock for the holder
/// of `holder_value`, as a process that migrates and then runs does.
async fn migrate_and_take(
    schema: &str,
    holder_value: u128,
    migrations: &[TestMigration],
) -> Result<
    HeldSharedMigrationsLock<TestTables>,
    crate::migrate_and_take_shared_migrations_lock::Error<AssertionsCheckError, RunError>,
> {
    migrate_and_take_shared_migrations_lock::<TestTables, _, _, _>(
        crate::migrate_and_take_shared_migrations_lock::Params {
            migrations_connection: &connection_config(),
            shared_lock_url: &url(),
            migrations_lock: &lock_config(),
            schema: Some(schema.into()),
            holder_id: holder(holder_value),
            assertions: &ASSERTIONS,
            migrations: &valid(migrations),
            transactions: MigrationsRunTransactions::WholeRun,
        },
    )
    .await
}

/// Migrate `schema` to `migrations` and release the lock, as a tool does.
async fn try_migrate_and_release(
    schema: &str,
    migrations: &[TestMigration],
) -> Result<
    Outcome,
    crate::migrate_and_release_migrations_lock::Error<AssertionsCheckError, RunError>,
> {
    migrate_and_release_migrations_lock::<TestTables, _, _, _>(
        crate::migrate_and_release_migrations_lock::Params {
            migrations_connection: &connection_config(),
            migrations_lock: &lock_config(),
            schema: Some(schema.into()),
            holder_id: holder(100),
            assertions: &ASSERTIONS,
            migrations: &valid(migrations),
            transactions: MigrationsRunTransactions::WholeRun,
        },
    )
    .await
}

/// Migrate `schema` to `migrations` and release the lock, as a tool does;
/// the run is expected to pass.
async fn migrate_and_release(schema: &str, migrations: &[TestMigration]) -> Outcome {
    try_migrate_and_release(schema, migrations)
        .await
        .expect("the database is migrated")
}

/// Release the lock of `held` the way a process that stops does.
async fn stop_keeping(held: HeldSharedMigrationsLock<TestTables>) {
    keep_shared_migrations_lock(held.keeper, std::future::ready(()))
        .await
        .expect("the lock is released");
}

#[tokio::test]
async fn migrating_process_migrates_a_fresh_database_into_a_shared_lock() {
    let schema = "postgres_bringup_migrations_fresh";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    let held = migrate_and_take(schema, 1, &[create_one(), create_two()])
        .await
        .expect("the database is migrated and the lock taken");

    assert!(!held.lock.is_migrations_lock_fenced());
    assert_eq!(locks(&bootstrap, schema).await, [(1, "shared".to_owned())]);
    assert_eq!(ledger_versions(&bootstrap, schema).await, [1, 2]);
    assert!(table_exists(&bootstrap, schema, "one").await);
    assert!(table_exists(&bootstrap, schema, "two").await);

    let lock = held.lock;
    keep_shared_migrations_lock(held.keeper, std::future::ready(()))
        .await
        .expect("the lock is released");

    assert!(lock.is_migrations_lock_fenced());
    assert!(locks(&bootstrap, schema).await.is_empty());

    drop_schema(&bootstrap, schema).await;
}

#[tokio::test]
async fn migrating_process_with_nothing_pending_takes_a_shared_lock_beside_another() {
    let schema = "postgres_bringup_migrations_nothing_pending";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    let first = migrate_and_take(schema, 1, &[create_one()])
        .await
        .expect("the first process migrates and takes its lock");
    let second = migrate_and_take(schema, 2, &[create_one()])
        .await
        .expect("the second process takes its lock beside the first");

    assert_eq!(
        locks(&bootstrap, schema).await,
        [(1, "shared".to_owned()), (2, "shared".to_owned())]
    );
    assert_eq!(ledger_versions(&bootstrap, schema).await, [1]);

    stop_keeping(first).await;
    stop_keeping(second).await;
    drop_schema(&bootstrap, schema).await;
}

#[tokio::test]
async fn migrating_process_refuses_to_migrate_a_database_in_use() {
    let schema = "postgres_bringup_migrations_in_use";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    let running = migrate_and_take(schema, 1, &[create_one()])
        .await
        .expect("the first process migrates and takes its lock");

    let refused = migrate_and_take(schema, 2, &[create_one(), create_two()]).await;

    let Err(
        error @ crate::migrate_and_take_shared_migrations_lock::Error::Migrate(
            MigrateUnderExclusiveMigrationsLockError::TakeLock(TakeExclusiveError::DatabaseInUse),
        ),
    ) = refused
    else {
        panic!("the migration is refused while the first process runs");
    };
    assert_eq!(
        error.to_string(),
        "migrating: exclusive migrations lock: the database is in use: other holders have shared \
         migrations locks"
    );
    assert_eq!(locks(&bootstrap, schema).await, [(1, "shared".to_owned())]);
    assert_eq!(ledger_versions(&bootstrap, schema).await, [1]);
    assert!(!table_exists(&bootstrap, schema, "two").await);

    stop_keeping(running).await;
    drop_schema(&bootstrap, schema).await;
}

#[tokio::test]
async fn failing_migration_leaves_no_lock_and_nothing_applied() {
    let schema = "postgres_bringup_migrations_failing";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    // The second migration creates the first one's table again.
    let failing = migration(
        2,
        r#"
CREATE TABLE one (id INT)
        "#,
    );

    let failed = migrate_and_take(schema, 1, &[create_one(), failing]).await;

    assert!(matches!(
        failed,
        Err(
            crate::migrate_and_take_shared_migrations_lock::Error::Migrate(
                MigrateUnderExclusiveMigrationsLockError::Run(_)
            )
        )
    ));
    assert!(locks(&bootstrap, schema).await.is_empty());
    assert!(ledger_versions(&bootstrap, schema).await.is_empty());
    assert!(!table_exists(&bootstrap, schema, "one").await);

    drop_schema(&bootstrap, schema).await;
}

#[tokio::test]
async fn tool_migrates_and_leaves_no_lock() {
    let schema = "postgres_bringup_migrations_tool";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    let first = migrate_and_release(schema, &[create_one(), create_two()]).await;

    let Outcome::Applied { migrations } = first else {
        panic!("the first run applies the migrations");
    };
    assert_eq!(migrations.get(), 2);
    assert_eq!(ledger_versions(&bootstrap, schema).await, [1, 2]);
    assert!(locks(&bootstrap, schema).await.is_empty());

    let second = migrate_and_release(schema, &[create_one(), create_two()]).await;

    assert!(matches!(second, Outcome::NothingPending));
    assert!(locks(&bootstrap, schema).await.is_empty());

    drop_schema(&bootstrap, schema).await;
}

/// Take a shared lock on `schema` for the holder of `holder_value` and
/// check it against `migrations`, as a process that only runs does.
async fn take_and_check(
    schema: &str,
    holder_value: u128,
    migrations: &[TestMigration],
) -> Result<
    HeldSharedMigrationsLock<TestTables>,
    crate::take_shared_migrations_lock_and_check::Error<AssertionsCheckError>,
> {
    take_shared_migrations_lock_and_check::<TestTables, _, _, _>(
        crate::take_shared_migrations_lock_and_check::Params {
            lock_url: &url(),
            migrations_lock: &lock_config(),
            schema: Some(schema.into()),
            holder_id: holder(holder_value),
            assertions: &ASSERTIONS,
            migrations: &valid(migrations),
        },
    )
    .await
}

/// Put an exclusive lock of a holder that is not there into `schema`, as
/// a migration in progress has one.
async fn plant_exclusive_lock(bootstrap: &sqlx::PgPool, schema: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
INSERT INTO "{schema}"."{locks}" (holder_id, mode, expires_at)
VALUES ($1, 'exclusive', clock_timestamp() + interval '1 minute')
        "#,
        locks = <TestTables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LOCKS,
    )))
    .bind(uuid::Uuid::from_u128(999))
    .execute(bootstrap)
    .await
    .expect("plant the exclusive lock");
}

#[tokio::test]
async fn running_process_takes_a_shared_lock_on_a_database_migrated_to_its_migrations() {
    let schema = "postgres_bringup_migrations_running";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;
    migrate_and_release(schema, &[create_one(), create_two()]).await;

    let held = take_and_check(schema, 3, &[create_one(), create_two()])
        .await
        .expect("the lock is taken and the check passes");

    assert!(!held.lock.is_migrations_lock_fenced());
    assert_eq!(locks(&bootstrap, schema).await, [(3, "shared".to_owned())]);

    stop_keeping(held).await;
    drop_schema(&bootstrap, schema).await;
}

#[tokio::test]
async fn running_process_refuses_a_database_that_is_not_at_its_migrations() {
    let schema = "postgres_bringup_migrations_running_refused";
    let bootstrap = waymark_support_test::postgres_setup().await;
    fresh_schema(&bootstrap, schema).await;

    let never_migrated = take_and_check(schema, 3, &[create_one()]).await;

    migrate_and_release(schema, &[create_one()]).await;

    let older = take_and_check(schema, 3, &[create_one(), create_two()]).await;

    assert!(matches!(
        never_migrated,
        Err(crate::take_shared_migrations_lock_and_check::Error::Check(
            waymark_migrations_engine::check_migrations::Error::NotMigrated
        ))
    ));
    let Err(error @ crate::take_shared_migrations_lock_and_check::Error::Check(_)) = older else {
        panic!("a database older than the migrations is refused");
    };
    assert_eq!(
        error.to_string(),
        "checking the migrations: migration 2 is not applied yet"
    );
    assert!(locks(&bootstrap, schema).await.is_empty());

    drop_schema(&bootstrap, schema).await;
}

#[tokio::test]
async fn running_process_refuses_to_start_while_a_migration_is_in_progress() {
    let schema = "postgres_bringup_migrations_running_in_progress";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;
    migrate_and_release(schema, &[create_one()]).await;
    plant_exclusive_lock(&bootstrap, schema).await;

    let refused = take_and_check(schema, 3, &[create_one()]).await;

    assert!(matches!(
        refused,
        Err(
            crate::take_shared_migrations_lock_and_check::Error::TakeLock(
                TakeSharedError::MigrationInProgress
            )
        )
    ));
    assert_eq!(
        locks(&bootstrap, schema).await,
        [(999, "exclusive".to_owned())]
    );

    drop_schema(&bootstrap, schema).await;
}

/// Put a ledger table as the SQLx migrator has into `schema`.
async fn plant_sqlx_migrations_ledger(bootstrap: &sqlx::PgPool, schema: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
CREATE TABLE "{schema}"._sqlx_migrations (version BIGINT PRIMARY KEY)
        "#
    )))
    .execute(bootstrap)
    .await
    .expect("plant the SQLx migrator's ledger");
}

#[tokio::test]
async fn database_that_does_not_meet_an_assertion_is_refused_before_anything_is_set_up() {
    let schema = "postgres_bringup_migrations_assertion_violated";
    let bootstrap = waymark_support_test::postgres_setup().await;
    fresh_schema(&bootstrap, schema).await;
    plant_sqlx_migrations_ledger(&bootstrap, schema).await;

    let migrating_and_running = migrate_and_take(schema, 1, &[create_one()]).await;
    let migrating = try_migrate_and_release(schema, &[create_one()]).await;
    let running = take_and_check(schema, 3, &[create_one()]).await;

    let refusal = "the database does not meet an assertion: it has a `_sqlx_migrations` table \
                   left by the SQLx migrator";
    for refused in [
        migrating_and_running.err().map(|error| error.to_string()),
        migrating.err().map(|error| error.to_string()),
        running.err().map(|error| error.to_string()),
    ] {
        let refused = refused.expect("the entry point refuses the database");

        assert!(refused.ends_with(refusal), "{refused}");
    }
    assert!(
        !table_exists(
            &bootstrap,
            schema,
            <TestTables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LEDGER
        )
        .await
    );
    assert!(
        !table_exists(
            &bootstrap,
            schema,
            <TestTables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LOCKS
        )
        .await
    );
    assert!(!table_exists(&bootstrap, schema, "one").await);

    drop_schema(&bootstrap, schema).await;
}

/// Whether `schema` is there.
async fn schema_exists(bootstrap: &sqlx::PgPool, schema: &str) -> bool {
    let (exists,): (bool,) = sqlx::query_as(
        r#"
SELECT to_regnamespace($1) IS NOT NULL
        "#,
    )
    .bind(schema)
    .fetch_one(bootstrap)
    .await
    .expect("look the schema up");

    exists
}

#[tokio::test]
async fn running_process_refuses_a_missing_schema_without_creating_it() {
    let schema = "postgres_bringup_migrations_running_missing";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    let refused = take_and_check(schema, 3, &[create_one()]).await;

    assert!(matches!(
        refused,
        Err(
            crate::take_shared_migrations_lock_and_check::Error::Connect(
                crate::ConnectionError::ConnectRequiringSchema(
                    waymark_sqlx_postgres_schema_pool::ExistingSchemaError::Missing { .. }
                )
            )
        )
    ));
    assert!(!schema_exists(&bootstrap, schema).await);
}

#[tokio::test]
async fn keeping_reports_a_lock_a_renewal_finds_gone() {
    let schema = "postgres_bringup_migrations_lost";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    let held = migrate_and_take(schema, 1, &[create_one()])
        .await
        .expect("the database is migrated and the lock taken");
    let lock = held.lock;

    // What a migrator does to a lock that expired: the row is gone.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DELETE FROM "{schema}"."{locks}"
        "#,
        locks = <TestTables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LOCKS,
    )))
    .execute(&bootstrap)
    .await
    .expect("delete the lock from under its holder");

    // The stop never completes: the next renewal, a heartbeat away, is
    // what returns.
    let kept = keep_shared_migrations_lock(held.keeper, std::future::pending()).await;

    assert!(matches!(
        kept,
        Err(crate::keep_shared_migrations_lock::Error::Lost)
    ));
    assert!(lock.is_migrations_lock_fenced());

    drop_schema(&bootstrap, schema).await;
}

#[tokio::test]
async fn locks_handed_over_are_kept_until_the_stop_and_released_then() {
    let schema = "postgres_bringup_migrations_keeping";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    let first = migrate_and_take(schema, 1, &[create_one()])
        .await
        .expect("the first lock is taken");
    let second = migrate_and_take(schema, 2, &[create_one()])
        .await
        .expect("the second lock is taken");

    let (handover, keeping) = shared_migrations_locks_keeping();
    handover
        .keep(first.keeper)
        .expect("the first lock is handed over");
    handover
        .keep(second.keeper)
        .expect("the second lock is handed over");
    drop(handover);

    let stop = tokio_util::sync::CancellationToken::new();
    let on_failure = tokio_util::sync::CancellationToken::new();

    let (kept, ()) = tokio::join!(keeping.run(stop.cancelled(), on_failure.clone()), async {
        // Past two heartbeats, both locks are still there.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_eq!(
            locks(&bootstrap, schema).await,
            [(1, "shared".to_owned()), (2, "shared".to_owned())]
        );

        stop.cancel();
    });

    kept.expect("both locks are kept and released");
    assert!(!on_failure.is_cancelled());
    assert!(locks(&bootstrap, schema).await.is_empty());

    drop_schema(&bootstrap, schema).await;
}

#[tokio::test]
async fn fenced_lock_cancels_the_failure_token_while_the_other_is_kept() {
    let schema = "postgres_bringup_migrations_keeping_fenced";
    let bootstrap = waymark_support_test::postgres_setup().await;
    drop_leftover_schema(&bootstrap, schema).await;

    let first = migrate_and_take(schema, 1, &[create_one()])
        .await
        .expect("the first lock is taken");
    let second = migrate_and_take(schema, 2, &[create_one()])
        .await
        .expect("the second lock is taken");

    let (handover, keeping) = shared_migrations_locks_keeping();
    handover
        .keep(first.keeper)
        .expect("the first lock is handed over");
    handover
        .keep(second.keeper)
        .expect("the second lock is handed over");
    drop(handover);

    // The first holder's lock goes from under it.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"
DELETE FROM "{schema}"."{locks}"
WHERE holder_id = $1
        "#,
        locks = <TestTables as waymark_migrations_engine_backend_sqlx_postgres::Tables>::LOCKS,
    )))
    .bind(uuid::Uuid::from_u128(1))
    .execute(&bootstrap)
    .await
    .expect("delete the first lock from under its holder");

    let stop = tokio_util::sync::CancellationToken::new();
    let on_failure = tokio_util::sync::CancellationToken::new();

    let (kept, ()) = tokio::join!(keeping.run(stop.cancelled(), on_failure.clone()), async {
        // The failure is signalled before any stop, and the second
        // lock is still kept then.
        on_failure.cancelled().await;
        assert_eq!(locks(&bootstrap, schema).await, [(2, "shared".to_owned())]);

        stop.cancel();
    });

    assert!(matches!(
        kept,
        Err(crate::keep_shared_migrations_lock::Error::Lost)
    ));
    assert!(locks(&bootstrap, schema).await.is_empty());

    drop_schema(&bootstrap, schema).await;
}
