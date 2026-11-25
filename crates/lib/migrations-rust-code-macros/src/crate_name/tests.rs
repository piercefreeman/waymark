//! Tests of reading the name of a Rust-code migration's crate.

use super::{ParseError, parse};

#[test]
fn version_and_description_are_read_from_the_crate_name() {
    let parsed = parse("waymark-backend-postgres-migration-0027-id-map-tables")
        .expect("the crate name parses");

    assert_eq!(parsed.version, 27);
    assert_eq!(parsed.description, "id map tables");
}

#[test]
fn last_migration_part_is_the_one_that_counts() {
    let parsed = parse("waymark-migration-tools-migration-3-init").expect("the crate name parses");

    assert_eq!(parsed.version, 3);
    assert_eq!(parsed.description, "init");
}

#[test]
fn crate_name_without_a_migration_part_is_refused() {
    assert!(matches!(
        parse("waymark-backend-postgres-migrations"),
        Err(ParseError::NoMigrationPart)
    ));
}

#[test]
fn crate_name_without_a_description_is_refused() {
    assert!(matches!(
        parse("waymark-backend-postgres-migration-0027"),
        Err(ParseError::NoSeparator)
    ));
    assert!(matches!(
        parse("waymark-backend-postgres-migration-0027-"),
        Err(ParseError::EmptyDescription)
    ));
}

#[test]
fn version_that_is_not_digits_is_refused() {
    let Err(ParseError::VersionNotDigits { version }) =
        parse("waymark-backend-postgres-migration-v27-id-map-tables")
    else {
        panic!("the version is refused");
    };
    assert_eq!(version, "v27");

    assert!(matches!(
        parse("waymark-backend-postgres-migration--id-map-tables"),
        Err(ParseError::VersionNotDigits { .. })
    ));
}

#[test]
fn version_out_of_range_is_refused() {
    assert!(matches!(
        parse("waymark-backend-postgres-migration-99999999999-id-map-tables"),
        Err(ParseError::VersionOutOfRange { .. })
    ));
}
