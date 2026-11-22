//! Tests of the check flow, over a mocked backend.

use waymark_migrations_engine_backend::MigrationsLedger;
use waymark_migrations_engine_migration::MigrationVersion;

use super::{Error, check_migrations};
use crate::test_helpers::{
    MockBackendOperations, TestAssertion, TestBackend, TestMigration, ledger_of_finished,
    valid_migrations,
};

/// A backend whose ledger is read once, as `ledger` makes it.
fn backend_with_ledger<Ledger>(ledger: Ledger) -> TestBackend
where
    Ledger: Fn() -> MigrationsLedger + Send + 'static,
{
    let mut operations = MockBackendOperations::new();

    operations
        .expect_read_migrations_ledger()
        .times(1)
        .returning(move || Ok(ledger()));

    TestBackend::new(operations)
}

#[tokio::test]
async fn check_passes_when_every_migration_is_applied() {
    let mut backend = backend_with_ledger(|| ledger_of_finished(&[1, 2]));
    let migrations = [TestMigration::untouched(1), TestMigration::untouched(2)];

    let checked = check_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
    )
    .await;

    assert!(checked.is_ok());

    backend.operations.checkpoint();
}

#[tokio::test]
async fn check_of_a_database_not_set_up_is_not_migrated() {
    let mut backend = backend_with_ledger(|| MigrationsLedger::NotSetUp);
    let migrations = [TestMigration::untouched(1)];

    let checked = check_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
    )
    .await;

    assert!(matches!(checked, Err(Error::NotMigrated)));

    backend.operations.checkpoint();
}

#[tokio::test]
async fn check_with_a_migration_not_applied_is_pending_at_the_first_one() {
    let mut backend = backend_with_ledger(|| ledger_of_finished(&[1]));
    let migrations = [
        TestMigration::untouched(1),
        TestMigration::untouched(2),
        TestMigration::untouched(3),
    ];

    let checked = check_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
    )
    .await;

    let Err(Error::Pending { version }) = checked else {
        panic!("a migration is pending");
    };
    assert_eq!(version, MigrationVersion::from_raw(2));

    backend.operations.checkpoint();
}

#[tokio::test]
async fn check_of_a_conflicting_ledger_is_a_status_error() {
    let mut backend = backend_with_ledger(|| ledger_of_finished(&[1, 2]));
    let migrations = [TestMigration::untouched(1)];

    let checked = check_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
    )
    .await;

    let Err(error @ Error::Status(_)) = checked else {
        panic!("the ledger conflicts with the array");
    };
    assert_eq!(
        error.to_string(),
        "migrations status: migrations ledger: the database has migration 2 applied, which this \
         build does not have"
    );

    backend.operations.checkpoint();
}

#[test]
fn check_refusals_say_what_is_missing() {
    let not_migrated: Error<core::convert::Infallible, core::convert::Infallible> =
        Error::NotMigrated;
    let pending: Error<core::convert::Infallible, core::convert::Infallible> = Error::Pending {
        version: MigrationVersion::from_raw(12),
    };

    assert_eq!(
        not_migrated.to_string(),
        "the database has not been migrated"
    );
    assert_eq!(pending.to_string(), "migration 12 is not applied yet");
}
