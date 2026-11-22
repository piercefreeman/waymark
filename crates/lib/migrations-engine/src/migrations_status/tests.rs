//! Tests of the status flow, over a mocked backend.

use waymark_migrations_engine_assertion::Outcome;

use super::{Error, migrations_status};
use crate::MigrationsComparison;
use crate::migrations_comparison::ConflictError;
use crate::test_helpers::{
    MockBackendOperations, TestAssertion, TestBackend, TestBackendError, TestMigration,
    ledger_of_finished, valid_migrations,
};

/// A backend whose ledger is read once, as finished entries of `versions`.
fn backend_with_ledger_of(versions: &'static [u32]) -> TestBackend {
    let mut operations = MockBackendOperations::new();

    operations
        .expect_read_migrations_ledger()
        .times(1)
        .returning(|| Ok(ledger_of_finished(versions)));

    TestBackend::new(operations)
}

#[tokio::test]
async fn status_is_the_comparison_of_the_ledger_read_with_the_array() {
    let mut backend = backend_with_ledger_of(&[1]);
    let migrations = [TestMigration::untouched(1), TestMigration::untouched(2)];

    let status = migrations_status(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
    )
    .await;

    assert!(matches!(
        status,
        Ok(MigrationsComparison::InOrder { applied: 1 })
    ));

    backend.operations.checkpoint();
}

#[tokio::test]
async fn status_of_a_conflicting_ledger_is_a_conflict_error() {
    let mut backend = backend_with_ledger_of(&[1, 5]);
    let migrations = [TestMigration::untouched(1)];

    let status = migrations_status(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
    )
    .await;

    assert!(matches!(
        status,
        Err(Error::Conflict(ConflictError::UnknownApplied { .. }))
    ));

    backend.operations.checkpoint();
}

#[tokio::test]
async fn status_backend_error_is_a_read_ledger_error() {
    let mut operations = MockBackendOperations::new();
    operations
        .expect_read_migrations_ledger()
        .times(1)
        .returning(|| Err(TestBackendError));
    let mut backend = TestBackend::new(operations);
    let migrations = [TestMigration::untouched(1)];

    let status = migrations_status(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
    )
    .await;

    assert!(matches!(status, Err(Error::ReadLedger(TestBackendError))));

    backend.operations.checkpoint();
}

#[tokio::test]
async fn status_checks_the_assertions_before_it_reads_the_ledger() {
    let mut backend = backend_with_ledger_of(&[1]);
    let mut assertion = TestAssertion::checked_once("the database is ours", || Ok(Outcome::Holds));
    let migrations = [TestMigration::untouched(1)];

    let status = migrations_status(&mut backend, &assertion, &valid_migrations(&migrations)).await;

    assert!(matches!(
        status,
        Ok(MigrationsComparison::InOrder { applied: 1 })
    ));
    assert_eq!(
        backend.connection.events.so_far(),
        ["check the database is ours"]
    );

    backend.operations.checkpoint();
    assertion.operations.checkpoint();
}

#[tokio::test]
async fn status_of_a_database_that_does_not_meet_an_assertion_reads_nothing() {
    // No read is expected: the mock fails the test on any call.
    let mut backend = TestBackend::new(MockBackendOperations::new());
    let mut assertion = TestAssertion::checked_once("the database is ours", || {
        TestAssertion::violated("the database is theirs")
    });
    let migrations = [TestMigration::untouched(1)];

    let status = migrations_status(&mut backend, &assertion, &valid_migrations(&migrations)).await;

    assert!(matches!(
        status,
        Err(Error::Assertions(
            crate::migrations_engine_assertions::Error::Violated { .. }
        ))
    ));

    assertion.operations.checkpoint();
}
