//! Tests of the comparison of a migrations ledger with a migrations
//! array.

use waymark_migrations_engine_backend::{MigrationsLedger, migrations_ledger};
use waymark_migrations_engine_migration::{MigrationRevision, MigrationVersion};

use super::{
    ArrayOrderError, ConflictError, MigrationsComparison, ValidMigrationsArray, compare_migrations,
    validate_migrations_array,
};
use crate::test_helpers::{TestMigration, ledger_of_finished, revision_of};

/// Test migrations of `versions`, none of whose operations is expected to
/// be called.
fn migrations(versions: &[u32]) -> Vec<TestMigration> {
    versions
        .iter()
        .map(|version| TestMigration::untouched(*version))
        .collect()
}

/// The valid migrations array of the test migrations of `versions`.
fn valid_migrations(versions: &[u32]) -> ValidMigrationsArray<Vec<TestMigration>> {
    validate_migrations_array(migrations(versions))
        .expect("the versions are in the order of a migrations array")
}

#[test]
fn valid_array_gives_its_migrations_back_in_their_order() {
    let array = valid_migrations(&[1, 2, 10]);

    let versions: Vec<u32> = array
        .as_slice()
        .iter()
        .map(|migration| migration.version.to_raw())
        .collect();

    assert_eq!(versions, [1, 2, 10]);
}

#[test]
fn array_in_the_order_of_its_versions_is_valid() {
    assert!(validate_migrations_array(migrations(&[])).is_ok());
    assert!(validate_migrations_array(migrations(&[7])).is_ok());
    assert!(validate_migrations_array(migrations(&[1, 2, 10])).is_ok());
}

#[test]
fn array_with_a_version_twice_is_refused() {
    let refused = validate_migrations_array(migrations(&[1, 2, 2]));

    let Err(ArrayOrderError { version, previous }) = refused else {
        panic!("the array is refused");
    };
    assert_eq!(version, MigrationVersion::from_raw(2));
    assert_eq!(previous, MigrationVersion::from_raw(2));
}

#[test]
fn array_with_a_lower_version_after_a_higher_one_is_refused() {
    let refused = validate_migrations_array(migrations(&[1, 3, 2]));

    let Err(error) = refused else {
        panic!("the array is refused");
    };
    assert_eq!(error.version, MigrationVersion::from_raw(2));
    assert_eq!(error.previous, MigrationVersion::from_raw(3));
    assert_eq!(
        error.to_string(),
        "migration 2 does not come after migration 3 in the migrations array"
    );
}

#[test]
fn ledger_of_a_database_not_set_up_compares_as_not_set_up() {
    let comparison = compare_migrations(&MigrationsLedger::NotSetUp, &valid_migrations(&[1, 2]));

    assert!(matches!(comparison, Ok(MigrationsComparison::NotSetUp)));
}

#[test]
fn ledger_counts_the_leading_migrations_it_has() {
    let array = valid_migrations(&[1, 2, 3]);

    let none = compare_migrations(&ledger_of_finished(&[]), &array);
    let some = compare_migrations(&ledger_of_finished(&[1, 2]), &array);
    let all = compare_migrations(&ledger_of_finished(&[3, 1, 2]), &array);

    assert!(matches!(
        none,
        Ok(MigrationsComparison::InOrder { applied: 0 })
    ));
    assert!(matches!(
        some,
        Ok(MigrationsComparison::InOrder { applied: 2 })
    ));
    assert!(matches!(
        all,
        Ok(MigrationsComparison::InOrder { applied: 3 })
    ));
}

#[test]
fn started_entry_is_a_dirty_conflict() {
    let ledger = MigrationsLedger::SetUp(vec![
        migrations_ledger::Entry {
            version: MigrationVersion::from_raw(1),
            revision: revision_of(1),
            state: migrations_ledger::EntryState::Finished,
        },
        migrations_ledger::Entry {
            version: MigrationVersion::from_raw(2),
            revision: revision_of(2),
            state: migrations_ledger::EntryState::Started,
        },
    ]);

    let conflict = compare_migrations(&ledger, &valid_migrations(&[1, 2, 3]));

    let Err(ConflictError::Dirty { version }) = conflict else {
        panic!("the ledger is dirty");
    };
    assert_eq!(version, MigrationVersion::from_raw(2));
}

#[test]
fn entry_the_array_does_not_have_is_an_unknown_applied_conflict() {
    let conflict = compare_migrations(&ledger_of_finished(&[1, 2, 3]), &valid_migrations(&[1, 2]));

    let Err(ConflictError::UnknownApplied { version }) = conflict else {
        panic!("the ledger has a migration the array does not");
    };
    assert_eq!(version, MigrationVersion::from_raw(3));
}

#[test]
fn entry_with_another_revision_is_a_revision_mismatch_conflict() {
    let ledger = MigrationsLedger::SetUp(vec![migrations_ledger::Entry {
        version: MigrationVersion::from_raw(1),
        revision: MigrationRevision::from_static("another-revision-1"),
        state: migrations_ledger::EntryState::Finished,
    }]);

    let conflict = compare_migrations(&ledger, &valid_migrations(&[1, 2]));

    let Err(ConflictError::RevisionMismatch {
        version,
        applied,
        expected,
    }) = conflict
    else {
        panic!("the revisions differ");
    };
    assert_eq!(version, MigrationVersion::from_raw(1));
    assert_eq!(applied, "another-revision-1");
    assert_eq!(expected, "test-migration-1");
}

#[test]
fn entry_after_a_migration_the_ledger_lacks_is_an_out_of_order_conflict() {
    let conflict = compare_migrations(&ledger_of_finished(&[1, 3]), &valid_migrations(&[1, 2, 3]));

    let Err(ConflictError::AppliedOutOfOrder { pending, applied }) = conflict else {
        panic!("a migration is applied past one that is not");
    };
    assert_eq!(pending, MigrationVersion::from_raw(2));
    assert_eq!(applied, MigrationVersion::from_raw(3));
}

#[test]
fn started_entry_is_reported_before_an_entry_the_array_does_not_have() {
    let ledger = MigrationsLedger::SetUp(vec![
        migrations_ledger::Entry {
            version: MigrationVersion::from_raw(9),
            revision: revision_of(9),
            state: migrations_ledger::EntryState::Finished,
        },
        migrations_ledger::Entry {
            version: MigrationVersion::from_raw(2),
            revision: revision_of(2),
            state: migrations_ledger::EntryState::Started,
        },
    ]);

    let conflict = compare_migrations(&ledger, &valid_migrations(&[1, 2]));

    assert!(matches!(conflict, Err(ConflictError::Dirty { .. })));
}

#[test]
fn conflicts_show_the_versions_they_are_about() {
    let version = MigrationVersion::from_raw(4);
    let other = MigrationVersion::from_raw(5);

    assert_eq!(
        ConflictError::Dirty { version }.to_string(),
        "migration 4 was started and never finished"
    );
    assert_eq!(
        ConflictError::UnknownApplied { version }.to_string(),
        "the database has migration 4 applied, which this build does not have"
    );
    assert_eq!(
        ConflictError::RevisionMismatch {
            version,
            applied: "users-1".to_owned(),
            expected: "users-2".to_owned(),
        }
        .to_string(),
        "migration 4 was applied at revision users-1 and this build has revision users-2"
    );
    assert_eq!(
        ConflictError::AppliedOutOfOrder {
            pending: version,
            applied: other,
        }
        .to_string(),
        "migration 5 is applied while the earlier migration 4 is not"
    );
}
