//! Tests of reading the SQL files checksums, over a scratch directory.

use std::collections::BTreeMap;
use std::path::PathBuf;

use super::{ReadSqlFilesChecksumsError, SQL_FILES_CHECKSUMS_FILE, read_sql_files_checksums};

/// A scratch directory of its own for the test `test`, emptied first.
fn scratch_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("waymark-migrations-sql-file-core-tests")
        .join(test);

    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("empty the scratch directory");
    }
    std::fs::create_dir_all(&dir).expect("create the scratch directory");

    dir
}

#[test]
fn checksums_are_read_out_of_their_file() {
    let dir = scratch_dir("read");
    std::fs::write(
        dir.join(SQL_FILES_CHECKSUMS_FILE),
        "{\"files\": {\"migrations/0001_init.sql\": \"sha256:aa\"}}",
    )
    .expect("write the file");

    let checksums = read_sql_files_checksums(&dir).expect("the checksums are read");

    assert_eq!(
        checksums.files,
        BTreeMap::from([(
            "migrations/0001_init.sql".to_owned(),
            "sha256:aa".to_owned()
        )])
    );
}

#[test]
fn file_that_is_not_there_is_told_apart() {
    let dir = scratch_dir("not-found");

    let Err(error @ ReadSqlFilesChecksumsError::NotFound { .. }) = read_sql_files_checksums(&dir)
    else {
        panic!("the file is not there");
    };

    assert_eq!(
        error.to_string(),
        format!(
            "{} is not there",
            dir.join(SQL_FILES_CHECKSUMS_FILE).display()
        )
    );
}

#[test]
fn text_that_is_not_checksums_is_refused() {
    let dir = scratch_dir("parse");

    for text in [
        "not json",
        "{}",
        "{\"files\": {\"migrations/0001_init.sql\": 7}}",
        "{\"files\": {}, \"dependencies\": []}",
    ] {
        std::fs::write(dir.join(SQL_FILES_CHECKSUMS_FILE), text).expect("write the file");

        assert!(
            matches!(
                read_sql_files_checksums(&dir),
                Err(ReadSqlFilesChecksumsError::Parse { .. })
            ),
            "{text} is refused"
        );
    }
}
