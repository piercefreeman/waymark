//! The check and the update of the SQL files checksums of a crate with
//! SQL-file migrations, run by the test [`crate::source_checksums_test!`]
//! adds to the crate.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use waymark_migrations_source_checksums_core::source_digest;
use waymark_migrations_sql_file_core::{
    ReadSqlFilesChecksumsError, SQL_FILES_CHECKSUMS_FILE, SqlFilesChecksums,
    read_sql_files_checksums,
};

/// The extension of an SQL-file migration's file.
const SQL_EXTENSION: &str = ".sql";

/// The place of the file or the directory at `path`, a path from
/// `manifest_dir` with `/` between its parts.
fn place_from(manifest_dir: &Path, path: &str) -> PathBuf {
    let mut place = manifest_dir.to_path_buf();

    for part in path.split('/') {
        place.push(part);
    }

    place
}

/// Error returned by [`compute_sql_files_checksums`].
#[derive(Debug, thiserror::Error)]
pub enum ComputeSqlFilesChecksumsError {
    /// A directory could not be listed.
    #[error("listing the directory {}: {error}", .path.display())]
    ListDirectory {
        /// The directory.
        path: PathBuf,

        /// Why it could not be listed.
        #[source]
        error: std::io::Error,
    },

    /// A directory has an entry whose file name is not Unicode.
    #[error(
        "the directory {} has an entry whose file name is not valid UTF-8",
        .directory.display()
    )]
    FileNameNotUtf8 {
        /// The directory.
        directory: PathBuf,
    },

    /// An SQL file could not be read.
    #[error("reading {}: {error}", .path.display())]
    ReadFile {
        /// The file.
        path: PathBuf,

        /// Why it could not be read.
        #[source]
        error: std::io::Error,
    },
}

/// Compute the SQL files checksums the SQL files under `migrations_dir`
/// make: read every SQL file under it, at any depth, and give the digest
/// of each by the file's path from `manifest_dir`.
///
/// `migrations_dir` is a directory from `manifest_dir`, with `/` between
/// its parts. Files that are not SQL files are left out, a directory
/// under it that is a symbolic link is not gone into, and a directory
/// that is not there is an error. Nothing is written.
pub fn compute_sql_files_checksums(
    manifest_dir: &Path,
    migrations_dir: &str,
) -> Result<SqlFilesChecksums, ComputeSqlFilesChecksumsError> {
    let mut files = BTreeMap::new();

    // The directories still to list, each by its path from the manifest's
    // directory.
    let mut pending = vec![migrations_dir.to_owned()];

    while let Some(listed_directory) = pending.pop() {
        let directory = place_from(manifest_dir, &listed_directory);

        let entries = std::fs::read_dir(&directory).map_err(|error| {
            ComputeSqlFilesChecksumsError::ListDirectory {
                path: directory.clone(),
                error,
            }
        })?;

        for entry in entries {
            let entry = entry.map_err(|error| ComputeSqlFilesChecksumsError::ListDirectory {
                path: directory.clone(),
                error,
            })?;

            let Ok(name) = entry.file_name().into_string() else {
                return Err(ComputeSqlFilesChecksumsError::FileNameNotUtf8 { directory });
            };
            let listed_path = format!("{listed_directory}/{name}");

            let file_type = entry.file_type().map_err(|error| {
                ComputeSqlFilesChecksumsError::ListDirectory {
                    path: directory.clone(),
                    error,
                }
            })?;

            if file_type.is_dir() {
                pending.push(listed_path);

                continue;
            }

            if !name.ends_with(SQL_EXTENSION) {
                continue;
            }

            let path = entry.path();
            let bytes = std::fs::read(&path)
                .map_err(|error| ComputeSqlFilesChecksumsError::ReadFile { path, error })?;

            files.insert(listed_path, source_digest(&bytes));
        }
    }

    Ok(SqlFilesChecksums { files })
}

/// Error returned by [`write_sql_files_checksums`].
#[derive(Debug, thiserror::Error)]
pub enum WriteSqlFilesChecksumsError {
    /// The checksums could not be put into the text of their file.
    #[error("rendering the checksums: {0}")]
    Render(#[source] serde_json::Error),

    /// The file could not be written.
    #[error("writing {}: {error}", .path.display())]
    Write {
        /// The file.
        path: PathBuf,

        /// Why it could not be written.
        #[source]
        error: std::io::Error,
    },
}

/// Write `checksums` as the SQL files checksums of the crate whose
/// manifest is in `manifest_dir`: the files' paths in their order, one to
/// a line.
pub fn write_sql_files_checksums(
    manifest_dir: &Path,
    checksums: &SqlFilesChecksums,
) -> Result<(), WriteSqlFilesChecksumsError> {
    let mut text =
        serde_json::to_string_pretty(checksums).map_err(WriteSqlFilesChecksumsError::Render)?;
    text.push('\n');

    let path = manifest_dir.join(SQL_FILES_CHECKSUMS_FILE);

    std::fs::write(&path, text).map_err(|error| WriteSqlFilesChecksumsError::Write { path, error })
}

/// One way the SQL files checksums of a crate are not what its SQL files
/// make them.
#[derive(Debug)]
pub enum Difference {
    /// The crate has no checksums.
    NoChecksums,

    /// The file of the checksums is not SQL files checksums.
    UnreadableChecksums(serde_json::Error),

    /// The crate has an SQL file the checksums do not.
    FileAdded(String),

    /// The checksums have an SQL file the crate does not.
    FileRemoved(String),

    /// An SQL file has another digest than the checksums have for it.
    FileChanged(String),
}

impl std::fmt::Display for Difference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoChecksums => write!(formatter, "the crate has no {SQL_FILES_CHECKSUMS_FILE}"),
            Self::UnreadableChecksums(error) => {
                write!(
                    formatter,
                    "{SQL_FILES_CHECKSUMS_FILE} cannot be read: {error}"
                )
            }
            Self::FileAdded(path) => write!(formatter, "file added: {path}"),
            Self::FileRemoved(path) => write!(formatter, "file removed: {path}"),
            Self::FileChanged(path) => write!(formatter, "file changed: {path}"),
        }
    }
}

/// The lines of `differences`, one difference to a line.
fn difference_lines(differences: &[Difference]) -> String {
    let mut lines = String::new();

    for difference in differences {
        lines.push_str(&format!("\n    {difference}"));
    }

    lines
}

/// How `stored`, the checksums as their file has them, differ from
/// `computed`, the checksums the SQL files make: nothing when they are
/// the same.
fn differences(stored: &SqlFilesChecksums, computed: &SqlFilesChecksums) -> Vec<Difference> {
    let mut differences = Vec::new();

    for (path, digest) in &computed.files {
        match stored.files.get(path) {
            None => differences.push(Difference::FileAdded(path.clone())),
            Some(stored_digest) if stored_digest != digest => {
                differences.push(Difference::FileChanged(path.clone()));
            }
            Some(_) => {}
        }
    }

    for path in stored.files.keys() {
        if !computed.files.contains_key(path) {
            differences.push(Difference::FileRemoved(path.clone()));
        }
    }

    differences
}

/// Error returned by [`check_or_update`].
#[derive(Debug, thiserror::Error)]
pub enum CheckOrUpdateError {
    /// The checksums the SQL files make could not be computed.
    #[error("computing the SQL files checksums: {0}")]
    ComputeSqlFilesChecksums(#[source] ComputeSqlFilesChecksumsError),

    /// The checksums could not be read.
    #[error("reading the SQL files checksums: {0}")]
    ReadSqlFilesChecksums(#[source] ReadSqlFilesChecksumsError),

    /// The checksums could not be written.
    #[error("writing the SQL files checksums: {0}")]
    WriteSqlFilesChecksums(#[source] WriteSqlFilesChecksumsError),

    /// The checksums are not what the SQL files make them.
    #[error(
        "{SQL_FILES_CHECKSUMS_FILE} is not what the SQL files make it:{}",
        difference_lines(.differences)
    )]
    OutOfDate {
        /// How the checksums differ.
        differences: Vec<Difference>,
    },
}

/// Check the SQL files checksums of the crate whose manifest is in
/// `manifest_dir` against the SQL files under `migrations_dir`. When they
/// differ and `update` is `true`, write the checksums the SQL files make
/// instead.
///
/// `migrations_dir` is the directory of the crate's SQL-file migrations,
/// from the manifest's directory, with `/` between its parts.
pub fn check_or_update(
    manifest_dir: &Path,
    migrations_dir: &str,
    update: bool,
) -> Result<(), CheckOrUpdateError> {
    let computed = compute_sql_files_checksums(manifest_dir, migrations_dir)
        .map_err(CheckOrUpdateError::ComputeSqlFilesChecksums)?;

    // Checksums that are not there or are not checksums are differences:
    // writing them anew is what mends both.
    let differences = match read_sql_files_checksums(manifest_dir) {
        Ok(stored) => differences(&stored, &computed),
        Err(ReadSqlFilesChecksumsError::NotFound { path: _ }) => vec![Difference::NoChecksums],
        Err(ReadSqlFilesChecksumsError::Parse { path: _, error }) => {
            vec![Difference::UnreadableChecksums(error)]
        }
        Err(error @ ReadSqlFilesChecksumsError::Read { .. }) => {
            return Err(CheckOrUpdateError::ReadSqlFilesChecksums(error));
        }
    };

    if differences.is_empty() {
        return Ok(());
    }

    if !update {
        return Err(CheckOrUpdateError::OutOfDate { differences });
    }

    write_sql_files_checksums(manifest_dir, &computed)
        .map_err(CheckOrUpdateError::WriteSqlFilesChecksums)
}

/// Add to a crate with SQL-file migrations the test of its SQL files
/// checksums.
///
/// ```ignore
/// waymark_migrations_sql_file::source_checksums_test!("migrations");
/// ```
///
/// The argument is the directory of the crate's SQL-file migrations, from
/// the directory of the crate's manifest, with `/` between its parts.
///
/// The test checks `migrations-engine-sql-files-checksums.json`, the file
/// beside the crate's manifest, against the SQL files under that
/// directory. When the check fails, the test says how to write the file
/// anew.
///
/// The test writes the file anew itself when, as it runs,
/// `WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS` is `true`, or
/// `WAYMARK_MIGRATIONS_TESTS_WRITE_SOURCE_CHECKSUMS` is. The second is
/// read by the tests alone: turning it on or off compiles nothing again.
#[macro_export]
macro_rules! source_checksums_test {
    ($migrations_dir:literal $(,)?) => {
        $crate::macro_support::source_checksums_test_at!($crate, $migrations_dir);
    };
}

#[cfg(test)]
mod tests;
