//! Tests of the migration version and the migration revision.

use super::{MigrationRevision, MigrationVersion};

#[test]
fn migration_version_gives_back_its_number() {
    let version = MigrationVersion::from_raw(27);

    assert_eq!(version.to_raw(), 27);
}

#[test]
fn migration_versions_compare_and_order_by_their_numbers() {
    let earlier = MigrationVersion::from_raw(9);
    let later = MigrationVersion::from_raw(10);

    assert_eq!(earlier, MigrationVersion::from_raw(9));
    assert_ne!(earlier, later);
    assert!(earlier < later);
    assert_eq!(earlier.max(later), later);
}

#[test]
fn migration_revision_gives_back_its_text() {
    assert_eq!(MigrationRevision::from_static("init-1").as_raw(), "init-1");
    assert_eq!(
        MigrationRevision::from_raw("init-1".to_owned()).as_raw(),
        "init-1"
    );
}

#[test]
fn migration_revisions_are_equal_when_their_texts_are() {
    assert_eq!(
        MigrationRevision::from_static("init-1"),
        MigrationRevision::from_raw("init-1".to_owned())
    );
    assert_ne!(
        MigrationRevision::from_static("init-1"),
        MigrationRevision::from_static("init-2")
    );
}
