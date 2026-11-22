use super::*;

/// `body` as the one line the tool writes for it.
fn line(command: &'static str, body: Body) -> String {
    serde_json::to_string(&Report {
        database: "main-database",
        command,
        body,
    })
    .expect("the report serializes")
}

/// The exit code `exit` stands for.
fn code(exit: Exit) -> u8 {
    exit.code()
}

#[test]
fn a_pending_status_lists_the_migrations_that_are_not_applied() {
    let line = line(
        "status",
        Body::Status(Status::Pending {
            applied: 26,
            pending: vec![PendingMigration {
                version: 27,
                description: "id map tables".to_owned(),
            }],
            migration_in_progress: false,
        }),
    );

    assert_eq!(
        line,
        r#"{"database":"main-database","command":"status","status":"pending","applied":26,"pending":[{"version":27,"description":"id map tables"}],"migration_in_progress":false}"#
    );
}

#[test]
fn a_run_reports_its_outcome_beside_the_database_and_the_command() {
    assert_eq!(
        line("run", Body::Run(Run::NothingPending)),
        r#"{"database":"main-database","command":"run","outcome":"nothing_pending"}"#
    );
    assert_eq!(
        line("run", Body::Run(Run::Applied { migrations: 2 })),
        r#"{"database":"main-database","command":"run","outcome":"applied","migrations":2}"#
    );
    assert_eq!(
        line(
            "run",
            Body::Run(Run::LockRefused {
                reason: LockRefusedReason::DatabaseInUse,
            })
        ),
        r#"{"database":"main-database","command":"run","outcome":"lock_refused","reason":"database_in_use"}"#
    );
}

#[test]
fn a_failed_command_reports_its_error() {
    assert_eq!(
        line(
            "check",
            Body::Error {
                error: "config: main database: database url: missing".to_owned(),
            }
        ),
        r#"{"database":"main-database","command":"check","error":"config: main database: database url: missing"}"#
    );
}

#[test]
fn check_exits_with_what_keeps_the_database_from_being_migrated_exactly() {
    let pending = || {
        vec![PendingMigration {
            version: 2,
            description: "two".to_owned(),
        }]
    };

    assert_eq!(
        code(check_exit(&Status::UpToDate {
            applied: 2,
            migration_in_progress: false,
        })),
        0
    );
    assert_eq!(
        code(check_exit(&Status::NotMigrated { pending: pending() })),
        3
    );
    assert_eq!(
        code(check_exit(&Status::Pending {
            applied: 1,
            pending: pending(),
            migration_in_progress: false,
        })),
        3
    );
    assert_eq!(
        code(check_exit(&Status::Pending {
            applied: 1,
            pending: pending(),
            migration_in_progress: true,
        })),
        4
    );
    assert_eq!(
        code(check_exit(&Status::UpToDate {
            applied: 2,
            migration_in_progress: true,
        })),
        4
    );
    // A run with unmanaged transactions shows as dirty while it is in
    // progress.
    assert_eq!(
        code(check_exit(&Status::Dirty {
            version: 2,
            migration_in_progress: true,
        })),
        4
    );
    assert_eq!(
        code(check_exit(&Status::Dirty {
            version: 2,
            migration_in_progress: false,
        })),
        5
    );
    assert_eq!(
        code(check_exit(&Status::LedgerMismatch {
            detail: "another revision".to_owned(),
            migration_in_progress: true,
        })),
        6
    );
}

#[test]
fn run_exits_with_what_kept_it_from_applying() {
    assert_eq!(code(run_exit(&Run::NothingPending)), 0);
    assert_eq!(code(run_exit(&Run::Applied { migrations: 1 })), 0);
    assert_eq!(
        code(run_exit(&Run::LockRefused {
            reason: LockRefusedReason::MigrationInProgress,
        })),
        4
    );
    assert_eq!(code(run_exit(&Run::Dirty { version: 2 })), 5);
    assert_eq!(
        code(run_exit(&Run::LedgerMismatch {
            detail: "another revision".to_owned(),
        })),
        6
    );
    assert_eq!(
        code(run_exit(&Run::AssertionViolated {
            message: "it is theirs".to_owned(),
        })),
        7
    );
}

#[test]
fn violated_assertion_is_reported_with_its_message() {
    let status = Status::AssertionViolated {
        message: "it is theirs".to_owned(),
    };

    assert_eq!(code(check_exit(&status)), 7);
    assert_eq!(
        line("check", Body::Status(status)),
        r#"{"database":"main-database","command":"check","status":"assertion_violated","message":"it is theirs"}"#
    );
    assert_eq!(
        line(
            "run",
            Body::Run(Run::AssertionViolated {
                message: "it is theirs".to_owned(),
            })
        ),
        r#"{"database":"main-database","command":"run","outcome":"assertion_violated","message":"it is theirs"}"#
    );
}
