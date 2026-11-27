//! Tests of what the proc macros say and emit about a source and its
//! checksums.

use quote::quote;

use super::{
    BuildFlags, mismatch_help, mismatch_revision, read_update_at_run_time, release_guards,
    update_command, watched_variables,
};

#[test]
fn mismatch_builds_with_either_flag_on() {
    for (allow_mismatch, update, builds) in [
        (false, false, false),
        (true, false, true),
        (false, true, true),
        (true, true, true),
    ] {
        let flags = BuildFlags {
            allow_mismatch,
            update,
        };

        assert_eq!(flags.mismatch_builds(), builds);
    }
}

#[test]
fn mismatch_revision_is_the_declared_one_with_the_mismatch_after_it() {
    assert_eq!(
        mismatch_revision("id-map-tables-1"),
        "id-map-tables-1 (source checksums mismatch)"
    );
}

#[test]
fn update_command_runs_the_tests_of_the_crate_with_the_update_variable_on() {
    assert_eq!(
        update_command("waymark-test-migration-0001-init"),
        "WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS=true cargo test -p \
         waymark-test-migration-0001-init"
    );
}

#[test]
fn mismatch_help_gives_the_variable_to_build_with_and_the_command_to_approve_with() {
    assert_eq!(
        mismatch_help("waymark-test-migration-0001-init"),
        "to build it anyway while developing:\n    \
         WAYMARK_MIGRATIONS_ALLOW_SOURCE_CHECKSUMS_MISMATCH=true\n\nto approve the source as it \
         is, after verifying the migration:\n    \
         WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS=true cargo test -p \
         waymark-test-migration-0001-init\n"
    );
}

#[test]
fn release_guards_are_one_for_each_flag_that_is_on() {
    let none = release_guards(&BuildFlags {
        allow_mismatch: false,
        update: false,
    })
    .to_string();
    let both = release_guards(&BuildFlags {
        allow_mismatch: true,
        update: true,
    })
    .to_string();

    assert!(none.is_empty());
    assert_eq!(both.matches("compile_error").count(), 2);
    assert!(both.contains("WAYMARK_MIGRATIONS_ALLOW_SOURCE_CHECKSUMS_MISMATCH is `true`"));
    assert!(both.contains("WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS is `true`"));
}

#[test]
fn watched_variables_are_the_two_a_build_reads() {
    let watched = watched_variables().to_string();

    assert_eq!(watched.matches("option_env").count(), 2);
    assert!(watched.contains("\"WAYMARK_MIGRATIONS_ALLOW_SOURCE_CHECKSUMS_MISMATCH\""));
    assert!(watched.contains("\"WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS\""));
    assert!(!watched.contains("TESTS_WRITE"));
}

#[test]
fn update_is_read_at_run_time_from_the_update_variable_and_the_tests_write_one() {
    let read =
        read_update_at_run_time(&quote! { ::some_crate::macro_support::envfury }).to_string();

    assert_eq!(
        read.matches(":: some_crate :: macro_support :: envfury :: or_parse")
            .count(),
        2
    );
    assert!(read.contains("\"WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS\""));
    assert!(read.contains("\"WAYMARK_MIGRATIONS_TESTS_WRITE_SOURCE_CHECKSUMS\""));
    assert!(!read.contains("ALLOW"));
}
