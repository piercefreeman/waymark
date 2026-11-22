//! Tests of reading the file name of an SQL-file migration.

use super::{ParseError, parse};

#[test]
fn version_and_description_are_read_from_the_file_name() {
    let parsed = parse("0007_add_the_widgets_table.sql").expect("the file name parses");

    assert_eq!(parsed.version, 7);
    assert_eq!(parsed.description, "add the widgets table");
}

#[test]
fn version_without_leading_zeros_is_read_too() {
    let parsed = parse("120_init.sql").expect("the file name parses");

    assert_eq!(parsed.version, 120);
    assert_eq!(parsed.description, "init");
}

#[test]
fn file_name_with_another_extension_is_refused() {
    assert!(matches!(
        parse("0001_init.txt"),
        Err(ParseError::NotAnSqlFile)
    ));
    assert!(matches!(parse("0001_init"), Err(ParseError::NotAnSqlFile)));
}

#[test]
fn file_name_without_an_underscore_is_refused() {
    assert!(matches!(parse("0001.sql"), Err(ParseError::NoSeparator)));
}

#[test]
fn version_that_is_not_digits_alone_is_refused() {
    let letters = parse("one_init.sql");
    let empty = parse("_init.sql");
    let signed = parse("+1_init.sql");

    assert!(matches!(letters, Err(ParseError::VersionNotDigits { .. })));
    assert!(matches!(empty, Err(ParseError::VersionNotDigits { .. })));

    let Err(error @ ParseError::VersionNotDigits { .. }) = signed else {
        panic!("a signed version is refused");
    };
    assert_eq!(
        error.to_string(),
        "the version \"+1\" is not made of digits alone"
    );
}

#[test]
fn version_past_what_a_version_can_be_is_refused() {
    let too_big = parse("4294967296_init.sql");

    let Err(error @ ParseError::VersionOutOfRange { .. }) = too_big else {
        panic!("a version past the highest one is refused");
    };
    assert_eq!(
        error.to_string(),
        "the version \"4294967296\" is not a number a migration version can be: number too large \
         to fit in target type"
    );
}

#[test]
fn empty_description_is_refused() {
    assert!(matches!(
        parse("0001_.sql"),
        Err(ParseError::EmptyDescription)
    ));
}
