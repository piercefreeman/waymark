//! Tests of the check of an SQL file against the SQL files checksums of
//! its crate, over a scratch directory.

use std::path::PathBuf;

use waymark_migrations_sql_file_core::SQL_FILES_CHECKSUMS_FILE;

use super::{Difference, compare, watched_checksums};

/// The path the tests' SQL file has in the checksums.
const PATH: &str = "migrations/0001_init.sql";

/// A scratch directory of its own for the test `test`, emptied first.
fn scratch_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("waymark-migrations-sql-file-macros-tests")
        .join(test);

    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("empty the scratch directory");
    }
    std::fs::create_dir_all(&dir).expect("create the scratch directory");

    dir
}

/// Write the checksums of the crate at `dir`: `path` under `digest`.
fn write_checksums(dir: &std::path::Path, path: &str, digest: &str) {
    std::fs::write(
        dir.join(SQL_FILES_CHECKSUMS_FILE),
        format!("{{\"files\": {{\"{path}\": \"{digest}\"}}}}"),
    )
    .expect("write the checksums");
}

#[test]
fn file_the_checksums_have_with_its_digest_matches() {
    let dir = scratch_dir("matches");
    write_checksums(&dir, PATH, "sha256:aa");

    assert!(compare(&dir, PATH, "sha256:aa").is_none());
}

#[test]
fn crate_without_checksums_differs() {
    let dir = scratch_dir("no-checksums");

    let difference = compare(&dir, PATH, "sha256:aa").expect("the file differs");

    assert!(matches!(difference, Difference::NoChecksums));
    assert_eq!(
        difference.to_string(),
        "the crate has no migrations-engine-sql-files-checksums.json"
    );
}

#[test]
fn checksums_that_cannot_be_read_differ() {
    let dir = scratch_dir("unreadable-checksums");
    std::fs::write(dir.join(SQL_FILES_CHECKSUMS_FILE), "not json").expect("write the checksums");

    let difference = compare(&dir, PATH, "sha256:aa").expect("the file differs");

    assert!(matches!(difference, Difference::UnreadableChecksums { .. }));
}

#[test]
fn file_the_checksums_do_not_have_differs() {
    let dir = scratch_dir("not-listed");
    write_checksums(&dir, "migrations/0002_other.sql", "sha256:aa");

    let difference = compare(&dir, PATH, "sha256:aa").expect("the file differs");

    assert!(matches!(difference, Difference::NotListed));
    assert_eq!(
        difference.to_string(),
        "migrations-engine-sql-files-checksums.json does not have the file"
    );
}

#[test]
fn file_with_another_digest_than_the_checksums_have_differs() {
    let dir = scratch_dir("changed");
    write_checksums(&dir, PATH, "sha256:aa");

    let difference = compare(&dir, PATH, "sha256:bb").expect("the file differs");

    let Difference::Changed { expected, actual } = &difference else {
        panic!("the file is changed");
    };
    assert_eq!(expected, "sha256:aa");
    assert_eq!(actual, "sha256:bb");
    assert_eq!(difference.to_string(), "expected sha256:aa, got sha256:bb");
}

#[test]
fn checksums_are_watched_when_they_are_there() {
    let dir = scratch_dir("watched");

    assert!(watched_checksums(&dir).is_none());

    write_checksums(&dir, PATH, "sha256:00");

    assert_eq!(
        watched_checksums(&dir),
        Some(dir.join(SQL_FILES_CHECKSUMS_FILE))
    );
}
