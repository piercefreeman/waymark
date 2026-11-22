//! The check of an SQL file against the SQL files checksums of its crate,
//! done when the crate is compiled.
//!
//! The checksums are written by the test the crate has for them; see
//! `waymark_migrations_sql_file::source_checksums`.

use std::path::{Path, PathBuf};

use waymark_migrations_sql_file_core::{
    ReadSqlFilesChecksumsError, SQL_FILES_CHECKSUMS_FILE, read_sql_files_checksums,
};

/// The way an SQL file does not match the SQL files checksums of its
/// crate. It shows as a line of the compiler's message.
#[derive(Debug)]
pub enum Difference {
    /// The crate has no checksums.
    NoChecksums,

    /// The checksums cannot be read.
    UnreadableChecksums {
        /// Why they cannot be read.
        error: String,
    },

    /// The checksums do not have the file.
    NotListed,

    /// The file has another digest than the checksums have for it.
    Changed {
        /// The digest the checksums have.
        expected: String,

        /// The digest of the file as it is.
        actual: String,
    },
}

impl std::fmt::Display for Difference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoChecksums => write!(formatter, "the crate has no {SQL_FILES_CHECKSUMS_FILE}"),
            Self::UnreadableChecksums { error } => {
                write!(
                    formatter,
                    "{SQL_FILES_CHECKSUMS_FILE} cannot be read: {error}"
                )
            }
            Self::NotListed => {
                write!(
                    formatter,
                    "{SQL_FILES_CHECKSUMS_FILE} does not have the file"
                )
            }
            Self::Changed { expected, actual } => {
                write!(formatter, "expected {expected}, got {actual}")
            }
        }
    }
}

/// Compare the SQL file with the digest `digest`, which the checksums
/// have under `path`, with the SQL files checksums of the crate at
/// `manifest_dir`: nothing when they have the file with that digest.
pub fn compare(manifest_dir: &Path, path: &str, digest: &str) -> Option<Difference> {
    let checksums = match read_sql_files_checksums(manifest_dir) {
        Ok(checksums) => checksums,
        Err(ReadSqlFilesChecksumsError::NotFound { path: _ }) => {
            return Some(Difference::NoChecksums);
        }
        Err(error) => {
            return Some(Difference::UnreadableChecksums {
                error: error.to_string(),
            });
        }
    };

    let Some(expected) = checksums.files.get(path) else {
        return Some(Difference::NotListed);
    };

    if digest != expected {
        return Some(Difference::Changed {
            expected: expected.clone(),
            actual: digest.to_owned(),
        });
    }

    None
}

/// The file of the SQL files checksums of the crate at `manifest_dir`,
/// when it is there: the compiler has to watch it for the check to run
/// again when it changes.
pub fn watched_checksums(manifest_dir: &Path) -> Option<PathBuf> {
    let checksums_path = manifest_dir.join(SQL_FILES_CHECKSUMS_FILE);

    if !checksums_path.is_file() {
        return None;
    }

    Some(checksums_path)
}

#[cfg(test)]
mod tests;
