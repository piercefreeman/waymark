//! Tests of how a crate capture differs from what its crate makes it.

use std::collections::{BTreeMap, BTreeSet};

use waymark_migrations_rust_code_core::CrateCapture;

use super::{CheckOrUpdateError, Difference, differences};

/// A capture with `files` and `dependencies`.
fn capture(files: &[(&str, &str)], dependencies: &[&str]) -> CrateCapture {
    CrateCapture {
        files: files
            .iter()
            .map(|(path, digest)| ((*path).to_owned(), (*digest).to_owned()))
            .collect::<BTreeMap<String, String>>(),
        dependencies: dependencies
            .iter()
            .map(|dependency| (*dependency).to_owned())
            .collect::<BTreeSet<String>>(),
    }
}

/// `differences`, each as its line.
fn lines(differences: &[Difference]) -> Vec<String> {
    differences.iter().map(ToString::to_string).collect()
}

#[test]
fn same_captures_do_not_differ() {
    let stored = capture(&[("src/lib.rs", "sha256:aa")], &["rmpv 1.3.1"]);
    let computed = capture(&[("src/lib.rs", "sha256:aa")], &["rmpv 1.3.1"]);

    assert!(differences(&stored, &computed).is_empty());
}

#[test]
fn files_added_changed_and_removed_are_each_a_difference() {
    let stored = capture(
        &[("src/gone.rs", "sha256:aa"), ("src/lib.rs", "sha256:bb")],
        &[],
    );
    let computed = capture(
        &[("src/lib.rs", "sha256:cc"), ("src/new.rs", "sha256:dd")],
        &[],
    );

    assert_eq!(
        lines(&differences(&stored, &computed)),
        [
            "file changed: src/lib.rs",
            "file added: src/new.rs",
            "file removed: src/gone.rs",
        ]
    );
}

#[test]
fn dependency_at_another_version_is_one_added_and_one_removed() {
    let stored = capture(&[], &["rmpv 1.3.1", "uuid 1.23.0"]);
    let computed = capture(&[], &["rmpv 1.3.2", "uuid 1.23.0"]);

    assert_eq!(
        lines(&differences(&stored, &computed)),
        [
            "dependency added: rmpv 1.3.2",
            "dependency removed: rmpv 1.3.1",
        ]
    );
}

#[test]
fn out_of_date_error_lists_the_differences() {
    let error = CheckOrUpdateError::OutOfDate {
        differences: vec![
            Difference::FileChanged("src/lib.rs".to_owned()),
            Difference::DependencyAdded("rmpv 1.3.2".to_owned()),
        ],
    };

    assert_eq!(
        error.to_string(),
        "migrations-engine-crate-capture.json is not what the crate makes it:\n    file changed: \
         src/lib.rs\n    dependency added: rmpv 1.3.2"
    );
}
