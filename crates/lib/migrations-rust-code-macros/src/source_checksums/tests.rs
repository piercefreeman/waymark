//! Tests of the check of a migration's source against its crate capture,
//! over a scratch directory.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use waymark_migrations_rust_code_core::CRATE_CAPTURE_FILE;
use waymark_migrations_source_checksums_core::source_digest;

use super::{Compared, Difference, compare, watched_files};

/// A scratch directory of its own for the test `test`, emptied first.
fn scratch_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("waymark-migrations-rust-code-macros-tests")
        .join(test);

    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("empty the scratch directory");
    }
    std::fs::create_dir_all(dir.join("src")).expect("create the scratch directory");

    dir
}

/// The text of a capture with `files` and a dependency the check does not
/// look at.
fn capture_text(files: &[(&str, &str)]) -> String {
    let entries: Vec<String> = files
        .iter()
        .map(|(path, digest)| format!("{path:?}: {digest:?}"))
        .collect();

    format!(
        "{{\"files\": {{{}}}, \"dependencies\": [\"rmpv 1.3.1\"]}}",
        entries.join(", ")
    )
}

#[test]
fn source_that_is_what_the_capture_has_matches() {
    let dir = scratch_dir("matches");
    std::fs::write(dir.join("src").join("lib.rs"), "the code").expect("write a file");
    std::fs::write(dir.join("not-captured.txt"), "not in the capture").expect("write a file");
    std::fs::write(
        dir.join(CRATE_CAPTURE_FILE),
        capture_text(&[("src/lib.rs", &source_digest(b"the code"))]),
    )
    .expect("write the capture");

    let Compared {
        differences,
        as_the_files_make_it,
    } = compare(&dir);

    assert!(differences.is_empty());
    assert_eq!(
        as_the_files_make_it.files,
        BTreeMap::from([("src/lib.rs".to_owned(), source_digest(b"the code"))])
    );
    assert_eq!(
        as_the_files_make_it.dependencies,
        BTreeSet::from(["rmpv 1.3.1".to_owned()])
    );
}

#[test]
fn crate_without_a_capture_differs() {
    let dir = scratch_dir("no-capture");

    let Compared {
        differences,
        as_the_files_make_it,
    } = compare(&dir);

    assert!(matches!(differences.as_slice(), [Difference::NoCapture]));
    assert!(as_the_files_make_it.files.is_empty());
    assert!(as_the_files_make_it.dependencies.is_empty());
}

#[test]
fn changed_and_missing_files_are_each_a_difference() {
    let dir = scratch_dir("changed-and-missing");
    std::fs::write(dir.join("src").join("lib.rs"), "the code, edited").expect("write a file");
    std::fs::write(
        dir.join(CRATE_CAPTURE_FILE),
        capture_text(&[
            ("src/gone.rs", "sha256:00"),
            ("src/lib.rs", &source_digest(b"the code")),
        ]),
    )
    .expect("write the capture");

    let Compared {
        differences,
        as_the_files_make_it,
    } = compare(&dir);

    let [
        Difference::UnreadableFile { path: gone, .. },
        Difference::Changed {
            path,
            expected,
            actual,
        },
    ] = differences.as_slice()
    else {
        panic!("one file is gone and one is changed");
    };
    assert_eq!(gone, "src/gone.rs");
    assert_eq!(path, "src/lib.rs");
    assert_eq!(expected, &source_digest(b"the code"));
    assert_eq!(actual, &source_digest(b"the code, edited"));

    // The file that is gone has no digest now; the changed one has its
    // new one.
    assert_eq!(
        as_the_files_make_it.files,
        BTreeMap::from([("src/lib.rs".to_owned(), source_digest(b"the code, edited"))])
    );
}

#[test]
fn watched_files_are_the_capture_and_the_captured_files_that_are_there() {
    let dir = scratch_dir("watched");
    std::fs::write(dir.join("src").join("lib.rs"), "the code").expect("write a file");
    std::fs::write(dir.join("not-captured.txt"), "not in the capture").expect("write a file");
    std::fs::write(
        dir.join(CRATE_CAPTURE_FILE),
        capture_text(&[("src/gone.rs", "sha256:00"), ("src/lib.rs", "sha256:11")]),
    )
    .expect("write the capture");

    assert_eq!(
        watched_files(&dir),
        [dir.join(CRATE_CAPTURE_FILE), dir.join("src").join("lib.rs")]
    );
}

#[test]
fn crate_without_a_capture_has_nothing_to_watch() {
    let dir = scratch_dir("nothing-to-watch");

    assert!(watched_files(&dir).is_empty());
}

#[test]
fn capture_that_cannot_be_read_differs() {
    let dir = scratch_dir("unreadable-capture");
    std::fs::write(dir.join(CRATE_CAPTURE_FILE), "not json").expect("write the capture");

    let Compared { differences, .. } = compare(&dir);

    assert!(matches!(
        differences.as_slice(),
        [Difference::UnreadableCapture { .. }]
    ));
}
