//! Tests of the backend against the local test Postgres, each in a schema
//! of its own.

use std::time::{Duration, Instant};

use waymark_migrations_engine_backend::{
    AccessMigrationsConnection as _, BeginWholeRunTransaction as _, CommitWholeRunTransaction as _,
    ConvertMigrationsLockToShared as _, FindExclusiveMigrationsLock as _,
    MarkMigrationFinished as _, MarkMigrationStarted as _, MigrationsLedger,
    ReadMigrationsLedger as _, ReleaseMigrationsLock as _, RenewMigrationsLock as _,
    RollbackWholeRunTransaction as _, SetupMigrationsEngine as _, TakeExclusiveMigrationsLock as _,
    TakeSharedMigrationsLock as _, migrations_ledger, migrations_lock,
};
use waymark_migrations_engine_migration::{MigrationIdentity, MigrationRevision, MigrationVersion};
use waymark_nonzero_duration::NonZeroDuration;

use super::{LedgerEntryVersionError, MarkMigrationFinishedError};

/// The tables of the tests.
struct TestTables;

impl super::Tables for TestTables {
    const LEDGER: &'static str = "_test_migrations";

    const LOCKS: &'static str = "_test_migrations_locks";
}

/// The backend of the tests.
type TestBackend = super::Backend<TestTables>;

/// The connect options of the test database, every connection scoped to
/// `schema`.
fn scoped_options(schema: &str) -> sqlx::postgres::PgConnectOptions {
    let options: sqlx::postgres::PgConnectOptions = waymark_support_integration::LOCAL_POSTGRES_DSN
        .expose_secret()
        .parse()
        .expect("parse test DSN");

    options.options([("search_path", schema)])
}

/// Make `schema` afresh, empty.
async fn fresh_schema(schema: &str) {
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
}

/// A backend over a connection of its own, scoped to `schema`.
async fn backend(schema: &str) -> TestBackend {
    let connection =
        <sqlx::PgConnection as sqlx::Connection>::connect_with(&scoped_options(schema))
            .await
            .expect("connect to the test schema");

    super::Backend::new(connection)
}

/// A backend over a connection scoped to `schema`, made afresh and set
/// up.
async fn set_up_backend(schema: &str) -> TestBackend {
    fresh_schema(schema).await;

    let mut backend = backend(schema).await;
    backend
        .setup_migrations_engine()
        .await
        .expect("the engine is set up");

    backend
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

/// The holder id with the value `value`.
fn holder(value: u128) -> migrations_lock::HolderId {
    migrations_lock::HolderId::from_raw(uuid::Uuid::from_u128(value))
}

/// An expiry a minute from now.
fn in_a_minute() -> migrations_lock::Expiry {
    migrations_lock::Expiry {
        now: Instant::now(),
        time_to_live: NonZeroDuration::from_secs(60).expect("a minute is not zero"),
    }
}

/// An expiry that passed a minute ago: a time to live of a minute,
/// counted from two minutes ago.
fn a_minute_ago() -> migrations_lock::Expiry {
    migrations_lock::Expiry {
        now: Instant::now()
            .checked_sub(Duration::from_secs(120))
            .expect("the clock is two minutes past its start"),
        time_to_live: NonZeroDuration::from_secs(60).expect("a minute is not zero"),
    }
}

/// Whether a table of `table` is there, as `backend`'s connection sees
/// it.
async fn table_exists(backend: &mut TestBackend, table: &str) -> bool {
    sqlx::query_scalar(
        r#"
SELECT to_regclass($1) IS NOT NULL
        "#,
    )
    .bind(table)
    .fetch_one(backend.migrations_connection())
    .await
    .expect("look the table up")
}

#[tokio::test]
async fn setup_creates_the_ledger_and_the_locks_and_can_be_repeated() {
    let schema = "migrations_engine_backend_setup";
    fresh_schema(schema).await;
    let mut backend = backend(schema).await;

    assert!(!table_exists(&mut backend, <TestTables as super::Tables>::LEDGER).await);
    assert!(!table_exists(&mut backend, <TestTables as super::Tables>::LOCKS).await);

    backend
        .setup_migrations_engine()
        .await
        .expect("the engine is set up");
    backend
        .setup_migrations_engine()
        .await
        .expect("the engine is set up again");

    assert!(table_exists(&mut backend, <TestTables as super::Tables>::LEDGER).await);
    assert!(table_exists(&mut backend, <TestTables as super::Tables>::LOCKS).await);

    drop_schema(schema).await;
}

#[tokio::test]
async fn setup_from_two_connections_at_once_succeeds_for_both() {
    let schema = "migrations_engine_backend_setup_at_once";

    // Unserialised, the second `CREATE TABLE IF NOT EXISTS` of a pair
    // fails on the catalog's unique index; several rounds make the two
    // meet.
    for _ in 0..5 {
        fresh_schema(schema).await;
        let mut one = backend(schema).await;
        let mut other = backend(schema).await;

        let (one_set_up, other_set_up) = tokio::join!(
            one.setup_migrations_engine(),
            other.setup_migrations_engine()
        );

        one_set_up.expect("the first setup succeeds");
        other_set_up.expect("the second setup succeeds");
    }

    drop_schema(schema).await;
}

#[tokio::test]
async fn ledger_of_a_schema_not_set_up_reads_as_not_set_up() {
    let schema = "migrations_engine_backend_not_set_up";
    fresh_schema(schema).await;
    let mut backend = backend(schema).await;

    let ledger = backend
        .read_migrations_ledger()
        .await
        .expect("the ledger is read");

    assert!(matches!(ledger, MigrationsLedger::NotSetUp));

    drop_schema(schema).await;
}

#[tokio::test]
async fn setup_comments_the_tables_and_every_column_of_them() {
    let schema = "migrations_engine_backend_comments";
    let mut backend = set_up_backend(schema).await;

    let not_commented: Vec<(String, String)> = sqlx::query_as(
        r#"
SELECT tables.relname::text, columns.attname::text
FROM pg_class AS tables
JOIN pg_attribute AS columns
    ON columns.attrelid = tables.oid
WHERE tables.relnamespace = current_schema()::regnamespace
    AND tables.relname IN ('_test_migrations', '_test_migrations_locks')
    AND columns.attnum > 0
    AND NOT columns.attisdropped
    AND col_description(tables.oid, columns.attnum) IS NULL
UNION ALL
SELECT tables.relname::text, ''
FROM pg_class AS tables
WHERE tables.relnamespace = current_schema()::regnamespace
    AND tables.relname IN ('_test_migrations', '_test_migrations_locks')
    AND obj_description(tables.oid, 'pg_class') IS NULL
        "#,
    )
    .fetch_all(backend.migrations_connection())
    .await
    .expect("read what has no comment");

    let state_comment: Option<String> = sqlx::query_scalar(
        r#"
SELECT col_description('_test_migrations'::regclass, columns.attnum)
FROM pg_attribute AS columns
WHERE columns.attrelid = '_test_migrations'::regclass
    AND columns.attname = 'state'
        "#,
    )
    .fetch_one(backend.migrations_connection())
    .await
    .expect("read the comment of the state column");

    assert_eq!(not_commented, []);
    assert_eq!(
        state_comment.as_deref(),
        Some("How far the migration got: 'started' or 'finished'.")
    );

    drop_schema(schema).await;
}

#[tokio::test]
async fn ledger_reads_back_what_was_marked() {
    let schema = "migrations_engine_backend_ledger";
    let mut backend = set_up_backend(schema).await;

    let empty = backend
        .read_migrations_ledger()
        .await
        .expect("the ledger is read");
    let MigrationsLedger::SetUp(entries) = empty else {
        panic!("the ledger is set up");
    };
    assert!(entries.is_empty());

    let first_revision = MigrationRevision::from_static("first-1");
    let second_revision = MigrationRevision::from_static("second-3");

    backend
        .mark_migration_started(MigrationIdentity {
            version: MigrationVersion::from_raw(1),
            description: "the first one",
            revision: &first_revision,
            source_digest: "sha256:01",
        })
        .await
        .expect("the first migration is marked as started");
    backend
        .mark_migration_finished(MigrationVersion::from_raw(1))
        .await
        .expect("the first migration is marked as finished");
    backend
        .mark_migration_started(MigrationIdentity {
            version: MigrationVersion::from_raw(2),
            description: "the second one",
            revision: &second_revision,
            source_digest: "sha256:02",
        })
        .await
        .expect("the second migration is marked as started");

    let MigrationsLedger::SetUp(mut entries) = backend
        .read_migrations_ledger()
        .await
        .expect("the ledger is read")
    else {
        panic!("the ledger is set up");
    };
    entries.sort_by_key(|entry| entry.version);

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].version, MigrationVersion::from_raw(1));
    assert_eq!(entries[0].revision, first_revision);
    assert!(matches!(
        entries[0].state,
        migrations_ledger::EntryState::Finished
    ));
    assert_eq!(entries[1].version, MigrationVersion::from_raw(2));
    assert_eq!(entries[1].revision, second_revision);
    assert!(matches!(
        entries[1].state,
        migrations_ledger::EntryState::Started
    ));

    // What the engine does not read back is stored too.
    let for_people: Vec<(i64, String, String)> = sqlx::query_as(
        r#"
SELECT version, description, applied_source_digest
FROM "_test_migrations"
ORDER BY version
        "#,
    )
    .fetch_all(backend.migrations_connection())
    .await
    .expect("read the columns the engine does not read back");
    assert_eq!(
        for_people,
        [
            (1, "the first one".to_owned(), "sha256:01".to_owned()),
            (2, "the second one".to_owned(), "sha256:02".to_owned()),
        ]
    );

    backend
        .mark_migration_finished(MigrationVersion::from_raw(2))
        .await
        .expect("the second migration is marked as finished");

    let MigrationsLedger::SetUp(entries) = backend
        .read_migrations_ledger()
        .await
        .expect("the ledger is read")
    else {
        panic!("the ledger is set up");
    };

    assert!(
        entries
            .iter()
            .all(|entry| matches!(entry.state, migrations_ledger::EntryState::Finished))
    );

    drop_schema(schema).await;
}

#[tokio::test]
async fn marking_as_finished_needs_a_started_entry() {
    let schema = "migrations_engine_backend_mark_finished";
    let mut backend = set_up_backend(schema).await;
    let revision = MigrationRevision::from_static("first-1");

    let no_entry = backend
        .mark_migration_finished(MigrationVersion::from_raw(1))
        .await;

    backend
        .mark_migration_started(MigrationIdentity {
            version: MigrationVersion::from_raw(1),
            description: "the first one",
            revision: &revision,
            source_digest: "sha256:01",
        })
        .await
        .expect("the migration is marked as started");
    backend
        .mark_migration_finished(MigrationVersion::from_raw(1))
        .await
        .expect("the migration is marked as finished");

    let finished_already = backend
        .mark_migration_finished(MigrationVersion::from_raw(1))
        .await;

    assert!(matches!(
        no_entry,
        Err(MarkMigrationFinishedError::NoStartedEntry { .. })
    ));
    let Err(error @ MarkMigrationFinishedError::NoStartedEntry { .. }) = finished_already else {
        panic!("a finished entry is not a started one");
    };
    assert_eq!(
        error.to_string(),
        "the migrations ledger has no started entry of migration 1"
    );

    drop_schema(schema).await;
}

#[tokio::test]
async fn ledger_entry_no_migration_can_have_is_a_typed_decode_error() {
    let schema = "migrations_engine_backend_decode";
    let mut backend = set_up_backend(schema).await;

    sqlx::query(
        r#"
INSERT INTO "_test_migrations" (version, description, revision, applied_source_digest, state)
VALUES (-1, 'a negative version', 'negative-1', 'sha256:00', 'finished')
        "#,
    )
    .execute(backend.migrations_connection())
    .await
    .expect("insert the entry with a negative version");

    let negative_version = backend.read_migrations_ledger().await;

    let Err(sqlx::Error::Decode(error)) = negative_version else {
        panic!("a negative version does not decode");
    };
    assert!(error.is::<LedgerEntryVersionError>());
    assert_eq!(
        error.to_string(),
        "migrations ledger entry has the version -1, which no migration can have"
    );

    drop_schema(schema).await;
}

#[tokio::test]
async fn locks_refuse_one_another_as_their_modes_say() {
    let schema = "migrations_engine_backend_lock_modes";
    let mut backend = set_up_backend(schema).await;

    let first_shared = backend
        .take_shared_migrations_lock(&holder(1), in_a_minute())
        .await
        .expect("take the first shared lock");
    let second_shared = backend
        .take_shared_migrations_lock(&holder(2), in_a_minute())
        .await
        .expect("take the second shared lock");
    let exclusive_among_shared = backend
        .take_exclusive_migrations_lock(&holder(3), in_a_minute())
        .await
        .expect("try the exclusive lock among shared ones");

    assert!(matches!(
        first_shared,
        waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Taken
    ));
    assert!(matches!(
        second_shared,
        waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Taken
    ));
    assert!(matches!(
        exclusive_among_shared,
        waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Refused
    ));

    backend
        .release_migrations_lock(&holder(1))
        .await
        .expect("release the first shared lock");
    backend
        .release_migrations_lock(&holder(2))
        .await
        .expect("release the second shared lock");

    let exclusive_alone = backend
        .take_exclusive_migrations_lock(&holder(3), in_a_minute())
        .await
        .expect("take the exclusive lock alone");
    let shared_under_exclusive = backend
        .take_shared_migrations_lock(&holder(4), in_a_minute())
        .await
        .expect("try a shared lock under the exclusive one");
    let second_exclusive = backend
        .take_exclusive_migrations_lock(&holder(5), in_a_minute())
        .await
        .expect("try a second exclusive lock");

    assert!(matches!(
        exclusive_alone,
        waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Taken
    ));
    assert!(matches!(
        shared_under_exclusive,
        waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Refused
    ));
    assert!(matches!(
        second_exclusive,
        waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Refused
    ));

    drop_schema(schema).await;
}

#[tokio::test]
async fn expired_lock_is_not_found_and_not_in_the_way() {
    let schema = "migrations_engine_backend_lock_expiry";
    let mut backend = set_up_backend(schema).await;

    let expired_exclusive = backend
        .take_exclusive_migrations_lock(&holder(1), a_minute_ago())
        .await
        .expect("take an exclusive lock that has expired already");
    let found = backend
        .find_exclusive_migrations_lock()
        .await
        .expect("look for the exclusive lock");
    let shared = backend
        .take_shared_migrations_lock(&holder(2), in_a_minute())
        .await
        .expect("take a shared lock past the expired exclusive one");
    let renewed_after_takeover = backend
        .renew_migrations_lock(&holder(1), in_a_minute())
        .await
        .expect("renew the lock that was taken over");

    assert!(matches!(
        expired_exclusive,
        waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Taken
    ));
    assert!(matches!(
        found,
        waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::NotFound
    ));
    assert!(matches!(
        shared,
        waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Taken
    ));
    assert!(matches!(
        renewed_after_takeover,
        waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Lost
    ));

    drop_schema(schema).await;
}

#[tokio::test]
async fn renewal_moves_the_expiry() {
    let schema = "migrations_engine_backend_lock_renewal";
    let mut backend = set_up_backend(schema).await;

    backend
        .take_exclusive_migrations_lock(&holder(1), a_minute_ago())
        .await
        .expect("take an exclusive lock that has expired already");
    let renewed = backend
        .renew_migrations_lock(&holder(1), in_a_minute())
        .await
        .expect("renew the lock before anything took it over");
    let found = backend
        .find_exclusive_migrations_lock()
        .await
        .expect("look for the exclusive lock");
    let shared = backend
        .take_shared_migrations_lock(&holder(2), in_a_minute())
        .await
        .expect("try a shared lock under the renewed exclusive one");
    let renewed_unknown = backend
        .renew_migrations_lock(&holder(9), in_a_minute())
        .await
        .expect("renew the lock of a holder with none");

    assert!(matches!(
        renewed,
        waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed
    ));
    assert!(matches!(
        found,
        waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::Found
    ));
    assert!(matches!(
        shared,
        waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Refused
    ));
    assert!(matches!(
        renewed_unknown,
        waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Lost
    ));

    drop_schema(schema).await;
}

#[tokio::test]
async fn conversion_turns_the_exclusive_lock_into_a_shared_one() {
    let schema = "migrations_engine_backend_lock_conversion";
    let mut backend = set_up_backend(schema).await;

    backend
        .take_exclusive_migrations_lock(&holder(1), in_a_minute())
        .await
        .expect("take the exclusive lock");
    let converted = backend
        .convert_migrations_lock_to_shared(&holder(1), in_a_minute())
        .await
        .expect("convert the exclusive lock");
    let found = backend
        .find_exclusive_migrations_lock()
        .await
        .expect("look for the exclusive lock");
    let shared = backend
        .take_shared_migrations_lock(&holder(2), in_a_minute())
        .await
        .expect("take a shared lock beside the converted one");
    let converted_again = backend
        .convert_migrations_lock_to_shared(&holder(1), in_a_minute())
        .await
        .expect("convert the lock that is shared already");
    let renewed = backend
        .renew_migrations_lock(&holder(1), in_a_minute())
        .await
        .expect("renew the converted lock");

    assert!(matches!(
        converted,
        waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome::Converted
    ));
    assert!(matches!(
        found,
        waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::NotFound
    ));
    assert!(matches!(
        shared,
        waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Taken
    ));
    assert!(matches!(
        converted_again,
        waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome::Lost
    ));
    assert!(matches!(
        renewed,
        waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed
    ));

    drop_schema(schema).await;
}

#[tokio::test]
async fn release_removes_the_lock_and_can_be_repeated() {
    let schema = "migrations_engine_backend_lock_release";
    let mut backend = set_up_backend(schema).await;

    backend
        .take_exclusive_migrations_lock(&holder(1), in_a_minute())
        .await
        .expect("take the exclusive lock");
    backend
        .release_migrations_lock(&holder(1))
        .await
        .expect("release the lock");
    backend
        .release_migrations_lock(&holder(1))
        .await
        .expect("release the lock that is gone");

    let renewed = backend
        .renew_migrations_lock(&holder(1), in_a_minute())
        .await
        .expect("renew the released lock");
    let exclusive = backend
        .take_exclusive_migrations_lock(&holder(2), in_a_minute())
        .await
        .expect("take the exclusive lock after the release");

    assert!(matches!(
        renewed,
        waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Lost
    ));
    assert!(matches!(
        exclusive,
        waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Taken
    ));

    drop_schema(schema).await;
}

#[tokio::test]
async fn two_connections_racing_for_the_exclusive_lock_give_it_to_one() {
    let schema = "migrations_engine_backend_lock_race";
    let mut one = set_up_backend(schema).await;
    let mut other = backend(schema).await;

    for round in 0..10_u128 {
        let one_holder = holder(round * 2);
        let other_holder = holder(round * 2 + 1);

        let (one_taking, other_taking) = tokio::join!(
            one.take_exclusive_migrations_lock(&one_holder, in_a_minute()),
            other.take_exclusive_migrations_lock(&other_holder, in_a_minute())
        );

        let one_taken = matches!(
            one_taking.expect("the first take returns"),
            waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Taken
        );
        let other_taken = matches!(
            other_taking.expect("the second take returns"),
            waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Taken
        );

        assert!(
            one_taken != other_taken,
            "exactly one of the two takes the lock"
        );

        one.release_migrations_lock(&one_holder)
            .await
            .expect("release the first holder's lock");
        one.release_migrations_lock(&other_holder)
            .await
            .expect("release the second holder's lock");
    }

    drop_schema(schema).await;
}

#[tokio::test]
async fn whole_run_transaction_keeps_what_is_committed_and_drops_what_is_rolled_back() {
    let schema = "migrations_engine_backend_transaction";
    let mut backend = set_up_backend(schema).await;

    backend
        .begin_whole_run_transaction()
        .await
        .expect("begin the transaction");
    sqlx::query(
        r#"
CREATE TABLE rolled_back (id INT)
        "#,
    )
    .execute(backend.migrations_connection())
    .await
    .expect("create a table inside the transaction");
    backend
        .rollback_whole_run_transaction()
        .await
        .expect("roll the transaction back");

    assert!(!table_exists(&mut backend, "rolled_back").await);

    backend
        .begin_whole_run_transaction()
        .await
        .expect("begin the transaction");
    sqlx::query(
        r#"
CREATE TABLE committed (id INT)
        "#,
    )
    .execute(backend.migrations_connection())
    .await
    .expect("create a table inside the transaction");
    backend
        .commit_whole_run_transaction()
        .await
        .expect("commit the transaction");

    assert!(table_exists(&mut backend, "committed").await);

    drop_schema(schema).await;
}

#[tokio::test]
async fn transaction_a_migration_begins_inside_the_whole_run_one_does_not_commit_it() {
    let schema = "migrations_engine_backend_savepoint";
    let mut backend = set_up_backend(schema).await;

    backend
        .begin_whole_run_transaction()
        .await
        .expect("begin the whole-run transaction");

    // What a migration with a transaction of its own does on the
    // connection.
    let mut inner = sqlx::Connection::begin(backend.migrations_connection())
        .await
        .expect("begin a transaction inside the whole-run one");
    sqlx::query(
        r#"
CREATE TABLE inside (id INT)
        "#,
    )
    .execute(&mut *inner)
    .await
    .expect("create a table inside the inner transaction");
    inner.commit().await.expect("commit the inner transaction");

    backend
        .rollback_whole_run_transaction()
        .await
        .expect("roll the whole-run transaction back");

    assert!(!table_exists(&mut backend, "inside").await);

    drop_schema(schema).await;
}

#[test]
fn remaining_microseconds_are_what_is_left_of_the_time_to_live() {
    let time_to_live = NonZeroDuration::from_secs(60).expect("a minute is not zero");

    let fresh = crate::locks::remaining_microseconds(&migrations_lock::Expiry {
        now: Instant::now(),
        time_to_live,
    });
    let passed = crate::locks::remaining_microseconds(&migrations_lock::Expiry {
        now: Instant::now()
            .checked_sub(Duration::from_secs(120))
            .expect("the clock is two minutes past its start"),
        time_to_live,
    });

    // A minute, less the little time between the two clock reads.
    assert!((59_000_000..=60_000_000).contains(&fresh));
    // A minute past: the count goes below zero, it does not stop at it.
    assert!((-61_000_000..=-60_000_000).contains(&passed));
}

#[tokio::test]
async fn sqlx_migrations_ledger_absent_holds_until_the_ledger_is_there() {
    use waymark_migrations_engine_assertion::Assertion as _;
    use waymark_migrations_engine_assertion::Outcome;

    let schema = "migrations_backend_test_sqlx_ledger_absent";
    fresh_schema(schema).await;
    let mut backend = backend(schema).await;

    let without_the_ledger = crate::SqlxMigrationsLedgerAbsentAssertion
        .check(backend.migrations_connection())
        .await
        .expect("the assertion is checked");

    sqlx::query(
        r#"
CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY)
        "#,
    )
    .execute(backend.migrations_connection())
    .await
    .expect("create a ledger as the SQLx migrator has");

    let with_the_ledger = crate::SqlxMigrationsLedgerAbsentAssertion
        .check(backend.migrations_connection())
        .await
        .expect("the assertion is checked");

    assert!(matches!(without_the_ledger, Outcome::Holds));
    assert!(matches!(
        with_the_ledger,
        Outcome::Violated { message }
            if message == "it has a `_sqlx_migrations` table left by the SQLx migrator"
    ));

    drop_schema(schema).await;
}
