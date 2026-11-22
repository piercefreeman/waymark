//! Tests of computing, writing and comparing the SQL files checksums,
//! over a scratch directory.

use std::collections::BTreeMap;
use std::path::PathBuf;

use waymark_migrations_source_checksums_core::source_digest;
use waymark_migrations_sql_file_core::{
    SQL_FILES_CHECKSUMS_FILE, SqlFilesChecksums, read_sql_files_checksums,
};

use super::{
    CheckOrUpdateError, ComputeSqlFilesChecksumsError, Difference, compute_sql_files_checksums,
    differences, write_sql_files_checksums,
};

/// A scratch directory of its own for the test `test`, emptied first.
fn scratch_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("waymark-migrations-sql-file-tests")
        .join(test);

    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("empty the scratch directory");
    }
    std::fs::create_dir_all(&dir).expect("create the scratch directory");

    dir
}

/// Checksums with `files`.
fn checksums(files: &[(&str, &str)]) -> SqlFilesChecksums {
    SqlFilesChecksums {
        files: files
            .iter()
            .map(|(path, digest)| ((*path).to_owned(), (*digest).to_owned()))
            .collect::<BTreeMap<String, String>>(),
    }
}

/// `differences`, each as its line.
fn lines(differences: &[Difference]) -> Vec<String> {
    differences.iter().map(ToString::to_string).collect()
}

#[test]
fn sql_files_under_the_directory_are_computed_at_any_depth() {
    let dir = scratch_dir("any-depth");
    let migrations = dir.join("migrations");
    std::fs::create_dir_all(migrations.join("older")).expect("create the directories");
    std::fs::write(migrations.join("0002_second.sql"), "SELECT 2;").expect("write a file");
    std::fs::write(migrations.join("older").join("0001_first.sql"), "SELECT 1;")
        .expect("write a file");

    let computed = compute_sql_files_checksums(&dir, "migrations").expect("the files are read");

    assert_eq!(
        computed.files,
        BTreeMap::from([
            (
                "migrations/0002_second.sql".to_owned(),
                source_digest(b"SELECT 2;")
            ),
            (
                "migrations/older/0001_first.sql".to_owned(),
                source_digest(b"SELECT 1;")
            ),
        ])
    );
}

#[test]
fn files_that_are_not_sql_files_and_files_outside_the_directory_are_left_out() {
    let dir = scratch_dir("left-out");
    let migrations = dir.join("migrations");
    std::fs::create_dir_all(&migrations).expect("create the directory");
    std::fs::create_dir_all(dir.join("transitions")).expect("create the directory");
    std::fs::write(migrations.join("0001_first.sql"), "SELECT 1;").expect("write a file");
    std::fs::write(migrations.join("README.md"), "not SQL").expect("write a file");
    std::fs::write(dir.join("transitions").join("ledger.sql"), "SELECT 0;").expect("write a file");

    let computed = compute_sql_files_checksums(&dir, "migrations").expect("the files are read");

    assert_eq!(
        computed
            .files
            .keys()
            .map(String::as_str)
            .collect::<Vec<&str>>(),
        ["migrations/0001_first.sql"]
    );
}

#[test]
fn directory_that_is_not_there_is_an_error() {
    let dir = scratch_dir("not-there");

    let Err(error @ ComputeSqlFilesChecksumsError::ListDirectory { .. }) =
        compute_sql_files_checksums(&dir, "migrations")
    else {
        panic!("the directory cannot be listed");
    };

    assert!(
        error.to_string().starts_with(&format!(
            "listing the directory {}",
            dir.join("migrations").display()
        )),
        "{error}"
    );
}

#[test]
fn written_checksums_are_one_file_to_a_line_and_read_back_the_same() {
    let dir = scratch_dir("written");
    let written = checksums(&[
        ("migrations/0002_second.sql", "sha256:bb"),
        ("migrations/0001_first.sql", "sha256:aa"),
    ]);

    write_sql_files_checksums(&dir, &written).expect("the checksums are written");

    assert_eq!(
        std::fs::read_to_string(dir.join(SQL_FILES_CHECKSUMS_FILE)).expect("read the file"),
        "{\n  \"files\": {\n    \"migrations/0001_first.sql\": \"sha256:aa\",\n    \
         \"migrations/0002_second.sql\": \"sha256:bb\"\n  }\n}\n"
    );
    assert_eq!(
        read_sql_files_checksums(&dir)
            .expect("the checksums are read")
            .files,
        written.files
    );
}

#[test]
fn same_checksums_do_not_differ() {
    let stored = checksums(&[("migrations/0001_first.sql", "sha256:aa")]);
    let computed = checksums(&[("migrations/0001_first.sql", "sha256:aa")]);

    assert!(differences(&stored, &computed).is_empty());
}

#[test]
fn files_added_changed_and_removed_are_each_a_difference() {
    let stored = checksums(&[
        ("migrations/0001_gone.sql", "sha256:aa"),
        ("migrations/0002_kept.sql", "sha256:bb"),
    ]);
    let computed = checksums(&[
        ("migrations/0002_kept.sql", "sha256:cc"),
        ("migrations/0003_new.sql", "sha256:dd"),
    ]);

    assert_eq!(
        lines(&differences(&stored, &computed)),
        [
            "file changed: migrations/0002_kept.sql",
            "file added: migrations/0003_new.sql",
            "file removed: migrations/0001_gone.sql",
        ]
    );
}

#[test]
fn out_of_date_error_lists_the_differences() {
    let error = CheckOrUpdateError::OutOfDate {
        differences: vec![
            Difference::NoChecksums,
            Difference::FileChanged("migrations/0002_kept.sql".to_owned()),
        ],
    };

    assert_eq!(
        error.to_string(),
        "migrations-engine-sql-files-checksums.json is not what the SQL files make it:\n    the \
         crate has no migrations-engine-sql-files-checksums.json\n    file changed: \
         migrations/0002_kept.sql"
    );
}
