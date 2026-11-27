//! The SQL files checksums of a crate with SQL-file migrations: the digest
//! each of its SQL files had when the migrations were approved.
//!
//! The proc macro of the SQL-file migrations and the check their crates'
//! tests run both read them.

#![warn(missing_docs)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The file of a crate's SQL files checksums, beside the crate's manifest.
pub const SQL_FILES_CHECKSUMS_FILE: &str = "migrations-engine-sql-files-checksums.json";

/// The SQL files checksums of a crate.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlFilesChecksums {
    /// The digest of each SQL file, by the file's path from the manifest's
    /// directory with `/` between its parts.
    pub files: BTreeMap<String, String>,
}

/// Error returned by [`read_sql_files_checksums`].
#[derive(Debug, thiserror::Error)]
pub enum ReadSqlFilesChecksumsError {
    /// The file is not there.
    #[error("{} is not there", .path.display())]
    NotFound {
        /// The file.
        path: PathBuf,
    },

    /// The file could not be read.
    #[error("reading {}: {error}", .path.display())]
    Read {
        /// The file.
        path: PathBuf,

        /// Why it could not be read.
        #[source]
        error: std::io::Error,
    },

    /// The file is not SQL files checksums.
    #[error("{} is not SQL files checksums: {error}", .path.display())]
    Parse {
        /// The file.
        path: PathBuf,

        /// Why it is not.
        #[source]
        error: serde_json::Error,
    },
}

/// Read the SQL files checksums of the crate whose manifest is in
/// `manifest_dir`.
pub fn read_sql_files_checksums(
    manifest_dir: &Path,
) -> Result<SqlFilesChecksums, ReadSqlFilesChecksumsError> {
    let path = manifest_dir.join(SQL_FILES_CHECKSUMS_FILE);

    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ReadSqlFilesChecksumsError::NotFound { path });
        }
        Err(error) => return Err(ReadSqlFilesChecksumsError::Read { path, error }),
    };

    serde_json::from_str(&text).map_err(|error| ReadSqlFilesChecksumsError::Parse { path, error })
}

#[cfg(test)]
mod tests;
