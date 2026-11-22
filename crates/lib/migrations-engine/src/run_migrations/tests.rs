//! Tests of the run flow, over a mocked backend and mocked migrations.

use std::sync::Arc;

use waymark_migrations_engine_backend::MigrationsLedger;
use waymark_migrations_engine_migration::MigrationVersion;

use super::{Error, MigrationsRunTransactions, Outcome, run_migrations};
use crate::ExclusiveMigrationsLock;
use crate::migrations_comparison::ConflictError;
use crate::test_helpers::{
    Events, MockBackendOperations, MockMigrationOperations, TestAssertion, TestBackend,
    TestBackendError, TestMigration, TestMigrationError, ledger_of_finished, revision_of,
    valid_migrations,
};

/// A backend whose ledger is read once, as finished entries of `versions`,
/// with the events its operations and its connection record into.
fn backend_with_ledger_of(versions: &'static [u32]) -> (TestBackend, Arc<Events>) {
    let mut operations = MockBackendOperations::new();

    operations
        .expect_read_migrations_ledger()
        .times(1)
        .returning(|| Ok(ledger_of_finished(versions)));

    let backend = TestBackend::new(operations);
    let events = Arc::clone(&backend.connection.events);

    (backend, events)
}

/// Expect the whole-run transaction to begin once.
fn expect_begin(backend: &mut TestBackend) {
    let events = Arc::clone(&backend.connection.events);

    backend
        .operations
        .expect_begin_whole_run_transaction()
        .times(1)
        .returning(move || {
            events.push("begin");

            Ok(())
        });
}

/// Expect the whole-run transaction to be committed once.
fn expect_commit(backend: &mut TestBackend) {
    let events = Arc::clone(&backend.connection.events);

    backend
        .operations
        .expect_commit_whole_run_transaction()
        .times(1)
        .returning(move || {
            events.push("commit");

            Ok(())
        });
}

/// Expect the whole-run transaction to be rolled back once.
fn expect_rollback(backend: &mut TestBackend) {
    let events = Arc::clone(&backend.connection.events);

    backend
        .operations
        .expect_rollback_whole_run_transaction()
        .times(1)
        .returning(move || {
            events.push("rollback");

            Ok(())
        });
}

/// Expect `times` migrations to be marked as started, each with the
/// description, the revision and the source digest of the test migration
/// of its version.
fn expect_marks_as_started(backend: &mut TestBackend, times: usize) {
    let events = Arc::clone(&backend.connection.events);

    backend
        .operations
        .expect_mark_migration_started()
        .times(times)
        .withf(|version, description, revision, source_digest| {
            description == TestMigration::DESCRIPTION
                && revision == revision_of(version.to_raw()).as_raw()
                && source_digest == TestMigration::SOURCE_DIGEST
        })
        .returning(move |version, _, _, _| {
            events.push(format!("mark started {}", version.to_raw()));

            Ok(())
        });
}

/// Expect `times` migrations to be marked as finished.
fn expect_marks_as_finished(backend: &mut TestBackend, times: usize) {
    let events = Arc::clone(&backend.connection.events);

    backend
        .operations
        .expect_mark_migration_finished()
        .times(times)
        .returning(move |version| {
            events.push(format!("mark finished {}", version.to_raw()));

            Ok(())
        });
}

/// A test migration of `version` whose pre-validation succeeds once and
/// whose run fails once; its post-validation is not expected.
fn migration_failing_its_run(version: u32) -> TestMigration {
    let mut operations = MockMigrationOperations::new();

    operations
        .expect_pre_validate_migration()
        .times(1)
        .returning(move |connection| {
            connection.events.push(format!("pre-validate {version}"));

            Ok(())
        });
    operations
        .expect_run_migration()
        .times(1)
        .returning(move |connection| {
            connection.events.push(format!("run {version} fails"));

            Err(TestMigrationError)
        });

    TestMigration::new(version, operations)
}

/// Check the expectations of the backend and of every migration.
fn checkpoint(backend: &mut TestBackend, migrations: &mut [TestMigration]) {
    backend.operations.checkpoint();

    for migration in migrations {
        migration.operations.checkpoint();
    }
}

#[tokio::test]
async fn whole_run_applies_the_pending_migrations_in_order_inside_one_transaction() {
    let (mut backend, events) = backend_with_ledger_of(&[1]);
    expect_begin(&mut backend);
    expect_marks_as_started(&mut backend, 2);
    expect_marks_as_finished(&mut backend, 2);
    expect_commit(&mut backend);
    let mut migrations = [
        TestMigration::untouched(1),
        TestMigration::succeeding(2),
        TestMigration::succeeding(3),
    ];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    let Ok(Outcome::Applied {
        migrations: applied,
    }) = ran
    else {
        panic!("the run applies the pending migrations");
    };
    assert_eq!(applied.get(), 2);
    assert_eq!(
        events.so_far(),
        [
            "begin",
            "pre-validate 2",
            "mark started 2",
            "run 2",
            "post-validate 2",
            "mark finished 2",
            "pre-validate 3",
            "mark started 3",
            "run 3",
            "post-validate 3",
            "mark finished 3",
            "commit",
        ]
    );

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn run_with_nothing_pending_writes_nothing() {
    for transactions in [
        MigrationsRunTransactions::WholeRun,
        MigrationsRunTransactions::Unmanaged,
    ] {
        let (mut backend, events) = backend_with_ledger_of(&[1, 2]);
        let mut migrations = [TestMigration::untouched(1), TestMigration::untouched(2)];
        let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

        let ran = run_migrations(
            &mut backend,
            &TestAssertion::NONE,
            &valid_migrations(&migrations),
            &lock,
            transactions,
        )
        .await;

        assert!(matches!(ran, Ok(Outcome::NothingPending)));
        assert!(events.so_far().is_empty());

        checkpoint(&mut backend, &mut migrations);
    }
}

#[tokio::test]
async fn whole_run_refuses_a_pending_migration_that_cannot_run_inside_a_transaction() {
    let (mut backend, events) = backend_with_ledger_of(&[1]);
    let mut cannot = TestMigration::untouched(3);
    cannot.runs_inside_a_transaction = false;
    let mut migrations = [
        TestMigration::untouched(1),
        TestMigration::untouched(2),
        cannot,
    ];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    // Nothing began and nothing ran, not even the migration before it.
    let Err(Error::TransactionUnsupported { version }) = ran else {
        panic!("the run is refused");
    };
    assert_eq!(version, MigrationVersion::from_raw(3));
    assert!(events.so_far().is_empty());

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn whole_run_does_not_mind_an_applied_migration_that_cannot_run_inside_a_transaction() {
    let (mut backend, events) = backend_with_ledger_of(&[1]);
    expect_begin(&mut backend);
    expect_marks_as_started(&mut backend, 1);
    expect_marks_as_finished(&mut backend, 1);
    expect_commit(&mut backend);
    let mut applied_already = TestMigration::untouched(1);
    applied_already.runs_inside_a_transaction = false;
    let mut migrations = [applied_already, TestMigration::succeeding(2)];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    assert!(matches!(ran, Ok(Outcome::Applied { .. })));
    assert_eq!(events.so_far().first().map(String::as_str), Some("begin"));
    assert_eq!(events.so_far().last().map(String::as_str), Some("commit"));

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn whole_run_rolls_back_when_a_migration_fails() {
    let (mut backend, events) = backend_with_ledger_of(&[]);
    expect_begin(&mut backend);
    expect_marks_as_started(&mut backend, 2);
    expect_marks_as_finished(&mut backend, 1);
    expect_rollback(&mut backend);
    let mut migrations = [
        TestMigration::succeeding(1),
        migration_failing_its_run(2),
        TestMigration::untouched(3),
    ];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    let Err(error @ Error::Run { .. }) = ran else {
        panic!("the run fails at the second migration");
    };
    assert_eq!(
        error.to_string(),
        "running migration 2: test migration error"
    );
    assert_eq!(
        events.so_far(),
        [
            "begin",
            "pre-validate 1",
            "mark started 1",
            "run 1",
            "post-validate 1",
            "mark finished 1",
            "pre-validate 2",
            "mark started 2",
            "run 2 fails",
            "rollback",
        ]
    );

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn whole_run_returns_the_error_that_caused_a_rollback_that_fails_too() {
    let (mut backend, _events) = backend_with_ledger_of(&[]);
    expect_begin(&mut backend);
    expect_marks_as_started(&mut backend, 1);
    backend
        .operations
        .expect_rollback_whole_run_transaction()
        .times(1)
        .returning(|| Err(TestBackendError));
    let mut migrations = [migration_failing_its_run(1)];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    assert!(matches!(ran, Err(Error::Run { .. })));

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn whole_run_stops_before_the_next_migration_once_the_lock_is_fenced() {
    let (mut backend, events) = backend_with_ledger_of(&[]);
    expect_begin(&mut backend);
    expect_marks_as_started(&mut backend, 1);
    expect_rollback(&mut backend);
    let (lock, fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    // The first migration is marked as finished and the lock fenced right
    // then.
    let marked = Arc::clone(&events);
    backend
        .operations
        .expect_mark_migration_finished()
        .times(1)
        .returning(move |version| {
            marked.push(format!("mark finished {}", version.to_raw()));
            fence.cancel();

            Ok(())
        });
    let mut migrations = [TestMigration::succeeding(1), TestMigration::untouched(2)];

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    assert!(matches!(ran, Err(Error::LockFenced)));
    assert_eq!(
        events.so_far(),
        [
            "begin",
            "pre-validate 1",
            "mark started 1",
            "run 1",
            "post-validate 1",
            "mark finished 1",
            "rollback",
        ]
    );

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn whole_run_does_not_commit_once_the_lock_is_fenced() {
    let (mut backend, events) = backend_with_ledger_of(&[]);
    expect_begin(&mut backend);
    expect_marks_as_started(&mut backend, 1);
    expect_rollback(&mut backend);
    let (lock, fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    // The only pending migration is marked as finished and the lock
    // fenced right then: all that is left is the commit.
    backend
        .operations
        .expect_mark_migration_finished()
        .times(1)
        .returning(move |_| {
            fence.cancel();

            Ok(())
        });
    let mut migrations = [TestMigration::succeeding(1)];

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    assert!(matches!(ran, Err(Error::LockFenced)));
    assert_eq!(events.so_far().last().map(String::as_str), Some("rollback"));

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn unmanaged_marks_each_pending_migration_as_started_and_as_finished() {
    let (mut backend, events) = backend_with_ledger_of(&[1]);
    expect_marks_as_started(&mut backend, 2);
    expect_marks_as_finished(&mut backend, 2);
    let mut cannot = TestMigration::succeeding(3);
    cannot.runs_inside_a_transaction = false;
    let mut migrations = [
        TestMigration::untouched(1),
        TestMigration::succeeding(2),
        cannot,
    ];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::Unmanaged,
    )
    .await;

    // No transaction begins, and the migration that cannot run inside one
    // runs like the other.
    let Ok(Outcome::Applied {
        migrations: applied,
    }) = ran
    else {
        panic!("the run applies the pending migrations");
    };
    assert_eq!(applied.get(), 2);
    assert_eq!(
        events.so_far(),
        [
            "pre-validate 2",
            "mark started 2",
            "run 2",
            "post-validate 2",
            "mark finished 2",
            "pre-validate 3",
            "mark started 3",
            "run 3",
            "post-validate 3",
            "mark finished 3",
        ]
    );

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn unmanaged_leaves_a_migration_that_fails_marked_as_started() {
    let (mut backend, events) = backend_with_ledger_of(&[]);
    expect_marks_as_started(&mut backend, 2);
    expect_marks_as_finished(&mut backend, 1);
    let mut migrations = [
        TestMigration::succeeding(1),
        migration_failing_its_run(2),
        TestMigration::untouched(3),
    ];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::Unmanaged,
    )
    .await;

    assert!(matches!(ran, Err(Error::Run { .. })));
    assert_eq!(
        events.so_far(),
        [
            "pre-validate 1",
            "mark started 1",
            "run 1",
            "post-validate 1",
            "mark finished 1",
            "pre-validate 2",
            "mark started 2",
            "run 2 fails",
        ]
    );

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn unmanaged_marks_nothing_for_a_migration_whose_pre_validation_fails() {
    let (mut backend, events) = backend_with_ledger_of(&[]);
    let mut operations = MockMigrationOperations::new();
    operations
        .expect_pre_validate_migration()
        .times(1)
        .returning(|_| Err(TestMigrationError));
    let mut migrations = [TestMigration::new(1, operations)];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::Unmanaged,
    )
    .await;

    let Err(error @ Error::PreValidate { .. }) = ran else {
        panic!("the pre-validation fails");
    };
    assert_eq!(
        error.to_string(),
        "pre-validating migration 1: test migration error"
    );
    assert!(events.so_far().is_empty());

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn unmanaged_stops_before_the_next_migration_once_the_lock_is_fenced() {
    let (mut backend, events) = backend_with_ledger_of(&[]);
    expect_marks_as_started(&mut backend, 1);
    let (lock, fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    // The first migration is marked as finished and the lock fenced right
    // then.
    let marked = Arc::clone(&events);
    backend
        .operations
        .expect_mark_migration_finished()
        .times(1)
        .returning(move |version| {
            marked.push(format!("mark finished {}", version.to_raw()));
            fence.cancel();

            Ok(())
        });
    let mut migrations = [TestMigration::succeeding(1), TestMigration::untouched(2)];

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::Unmanaged,
    )
    .await;

    assert!(matches!(ran, Err(Error::LockFenced)));
    assert_eq!(
        events.so_far().last().map(String::as_str),
        Some("mark finished 1")
    );

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn run_on_a_database_not_set_up_is_refused() {
    let mut operations = MockBackendOperations::new();
    operations
        .expect_read_migrations_ledger()
        .times(1)
        .returning(|| Ok(MigrationsLedger::NotSetUp));
    let mut backend = TestBackend::new(operations);
    let mut migrations = [TestMigration::untouched(1)];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    assert!(matches!(ran, Err(Error::NotSetUp)));

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn run_on_a_conflicting_ledger_is_refused() {
    let (mut backend, events) = backend_with_ledger_of(&[1, 9]);
    let mut migrations = [TestMigration::untouched(1), TestMigration::untouched(2)];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &TestAssertion::NONE,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    assert!(matches!(
        ran,
        Err(Error::Conflict(ConflictError::UnknownApplied { .. }))
    ));
    assert!(events.so_far().is_empty());

    checkpoint(&mut backend, &mut migrations);
}

#[tokio::test]
async fn run_on_a_database_that_does_not_meet_an_assertion_reads_and_applies_nothing() {
    // No read is expected: the mock fails the test on any call.
    let mut backend = TestBackend::new(MockBackendOperations::new());
    let mut assertion = TestAssertion::checked_once("the database is ours", || {
        TestAssertion::violated("the database is theirs")
    });
    let mut migrations = [TestMigration::untouched(1)];
    let (lock, _fence) = ExclusiveMigrationsLock::with_fence_for_tests();

    let ran = run_migrations(
        &mut backend,
        &assertion,
        &valid_migrations(&migrations),
        &lock,
        MigrationsRunTransactions::WholeRun,
    )
    .await;

    let Err(error @ Error::Assertions(_)) = ran else {
        panic!("the run is refused");
    };
    assert_eq!(
        error.to_string(),
        "migrations engine assertions: the database does not meet an assertion: the database is \
         theirs"
    );

    checkpoint(&mut backend, &mut migrations);
    assertion.operations.checkpoint();
}
