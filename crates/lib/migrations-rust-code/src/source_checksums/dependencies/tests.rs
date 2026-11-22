//! Tests of the captured dependencies of a migration and of the walk over
//! its dependency graph.

use std::collections::BTreeMap;

use super::{
    CapturedDependency, CapturedDependencyError, OfPackageError, RefusedPackage,
    transitive_dependencies,
};

/// The source of a package from crates.io.
const CRATES_IO: &str = "registry+https://github.com/rust-lang/crates.io-index";

#[test]
fn crates_io_package_is_its_name_and_version() {
    let captured = CapturedDependency::of("rmpv", "1.3.1", Some(CRATES_IO))
        .expect("the package can be captured");

    assert!(matches!(captured, CapturedDependency::CratesIo { .. }));
    assert_eq!(captured.to_string(), "rmpv 1.3.1");
}

#[test]
fn git_package_has_its_source_with_the_commit() {
    let source = "git+https://github.com/transact-rs/sqlx?rev=5d3787e#5d3787ef9d69947065085f46b58767a50b3d8e25";

    let captured =
        CapturedDependency::of("sqlx", "0.9.0", Some(source)).expect("the package can be captured");

    assert!(matches!(captured, CapturedDependency::Git { .. }));
    assert_eq!(captured.to_string(), format!("sqlx 0.9.0 ({source})"));
}

#[test]
fn git_package_without_its_commit_is_refused() {
    for source in [
        "git+https://github.com/transact-rs/sqlx?branch=main",
        "git+https://github.com/transact-rs/sqlx?branch=main#",
    ] {
        assert!(
            matches!(
                CapturedDependency::of("sqlx", "0.9.0", Some(source)),
                Err(CapturedDependencyError::GitWithoutCommit { .. })
            ),
            "{source} is refused"
        );
    }
}

#[test]
fn package_from_another_registry_is_refused() {
    assert!(matches!(
        CapturedDependency::of(
            "rmpv",
            "1.3.1",
            Some("registry+https://example.com/another-index")
        ),
        Err(CapturedDependencyError::OtherSource { .. })
    ));
}

#[test]
fn migrations_engine_crate_is_its_name_alone() {
    let captured = CapturedDependency::of("waymark-migrations-rust-code", "0.1.0", None)
        .expect("the package can be captured");

    assert!(matches!(captured, CapturedDependency::EngineCrate { .. }));
    assert_eq!(captured.to_string(), "waymark-migrations-rust-code (path)");
}

#[test]
fn any_other_path_dependency_is_refused() {
    assert!(matches!(
        CapturedDependency::of("waymark-vm-runtime", "0.1.0", None),
        Err(CapturedDependencyError::PathDependency)
    ));
}

#[test]
fn transitive_dependencies_are_what_the_root_depends_on_through_others_too() {
    let dependencies = BTreeMap::from([
        ("migration", vec!["rmpv", "sqlx"]),
        ("rmpv", vec!["rmp"]),
        ("rmp", vec![]),
        ("sqlx", vec!["rmp"]),
        ("not-depended-on", vec!["rmpv"]),
    ]);

    let depended_on: Vec<&str> = transitive_dependencies("migration", &dependencies)
        .expect("the graph is whole")
        .into_iter()
        .collect();

    assert_eq!(depended_on, ["rmp", "rmpv", "sqlx"]);
}

#[test]
fn transitive_dependencies_of_a_graph_that_is_not_whole_are_an_error() {
    let dependencies = BTreeMap::from([("migration", vec!["rmpv"])]);

    let error =
        transitive_dependencies("migration", &dependencies).expect_err("the graph has no rmpv");

    assert_eq!(error.id, "rmpv");
}

#[test]
fn refused_packages_are_listed_one_to_a_line() {
    let error = OfPackageError::Refused(vec![
        RefusedPackage {
            name: "waymark-vm-runtime".to_owned(),
            version: "0.1.0".to_owned(),
            error: CapturedDependencyError::PathDependency,
        },
        RefusedPackage {
            name: "rmpv".to_owned(),
            version: "1.3.1".to_owned(),
            error: CapturedDependencyError::OtherSource {
                source_text: "registry+https://example.com/another-index".to_owned(),
            },
        },
    ]);

    assert_eq!(
        error.to_string(),
        "the migration cannot depend on:\n    waymark-vm-runtime 0.1.0: a path dependency \
         outside the migrations engine\n    rmpv 1.3.1: a dependency from neither crates.io nor \
         a git repository: registry+https://example.com/another-index"
    );
}
