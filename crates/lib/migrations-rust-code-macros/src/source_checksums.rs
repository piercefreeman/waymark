//! The check of a migration's source against its crate capture, done when
//! the migration's crate is compiled.
//!
//! The capture is written by the test the macros add to the crate; see
//! `waymark_migrations_rust_code::source_checksums`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use waymark_migrations_rust_code_core::{
    CRATE_CAPTURE_FILE, CrateCapture, ReadCrateCaptureError, read_crate_capture,
};
use waymark_migrations_source_checksums_core::source_digest;

/// The place of the file the capture has under `path`, from the
/// manifest's directory.
fn place_from(manifest_dir: &Path, path: &str) -> PathBuf {
    let mut place = manifest_dir.to_path_buf();

    for part in path.split('/') {
        place.push(part);
    }

    place
}

/// How a migration's source stands against its crate capture.
#[derive(Debug)]
pub struct Compared {
    /// The ways the source does not match the capture: none when every
    /// file the capture has is there, with the digest the capture has.
    pub differences: Vec<Difference>,

    /// The capture with each file under the digest it has now: the files
    /// that could be read, and the dependencies as the capture has them.
    /// Empty when there is no capture to read.
    pub as_the_files_make_it: CrateCapture,
}

impl Compared {
    /// What comparing gives when there is no capture to read.
    fn without_a_capture(difference: Difference) -> Self {
        Self {
            differences: vec![difference],
            as_the_files_make_it: CrateCapture {
                files: BTreeMap::new(),
                dependencies: BTreeSet::new(),
            },
        }
    }
}

/// One way a migration's source does not match its crate capture. It
/// shows as a line of the compiler's message.
#[derive(Debug)]
pub enum Difference {
    /// The crate has no capture.
    NoCapture,

    /// The capture cannot be read.
    UnreadableCapture {
        /// Why it cannot be read.
        error: String,
    },

    /// A file the capture has cannot be read.
    UnreadableFile {
        /// The file's path, as the capture has it.
        path: String,

        /// Why it cannot be read.
        error: String,
    },

    /// A file has another digest than the capture has for it.
    Changed {
        /// The file's path, as the capture has it.
        path: String,

        /// The digest the capture has.
        expected: String,

        /// The digest of the file as it is.
        actual: String,
    },
}

impl std::fmt::Display for Difference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCapture => write!(formatter, "the crate has no {CRATE_CAPTURE_FILE}"),
            Self::UnreadableCapture { error } => {
                write!(formatter, "{CRATE_CAPTURE_FILE} cannot be read: {error}")
            }
            Self::UnreadableFile { path, error } => {
                write!(formatter, "{path}: the file cannot be read: {error}")
            }
            Self::Changed {
                path,
                expected,
                actual,
            } => write!(formatter, "{path}: expected {expected}, got {actual}"),
        }
    }
}

/// Compare the files of the crate at `manifest_dir` with its crate
/// capture: exactly the files the capture has are read.
pub fn compare(manifest_dir: &Path) -> Compared {
    let capture = match read_crate_capture(manifest_dir) {
        Ok(capture) => capture,
        Err(ReadCrateCaptureError::NotFound { path: _ }) => {
            return Compared::without_a_capture(Difference::NoCapture);
        }
        Err(error) => {
            return Compared::without_a_capture(Difference::UnreadableCapture {
                error: error.to_string(),
            });
        }
    };

    let mut differences = Vec::new();
    let mut files = BTreeMap::new();

    for (path, expected) in &capture.files {
        let bytes = match std::fs::read(place_from(manifest_dir, path)) {
            Ok(bytes) => bytes,
            Err(error) => {
                differences.push(Difference::UnreadableFile {
                    path: path.clone(),
                    error: error.to_string(),
                });

                continue;
            }
        };

        let actual = source_digest(&bytes);

        if &actual != expected {
            differences.push(Difference::Changed {
                path: path.clone(),
                expected: expected.clone(),
                actual: actual.clone(),
            });
        }

        files.insert(path.clone(), actual);
    }

    Compared {
        differences,
        as_the_files_make_it: CrateCapture {
            files,
            dependencies: capture.dependencies,
        },
    }
}

/// The files the compiler has to watch for the check to run again when
/// one changes: the capture, and the files it has that are there.
pub fn watched_files(manifest_dir: &Path) -> Vec<PathBuf> {
    let capture_path = manifest_dir.join(CRATE_CAPTURE_FILE);

    if !capture_path.is_file() {
        return Vec::new();
    }

    let mut watched = vec![capture_path];

    let Ok(capture) = read_crate_capture(manifest_dir) else {
        return watched;
    };

    for path in capture.files.keys() {
        let full_path = place_from(manifest_dir, path);

        if full_path.is_file() {
            watched.push(full_path);
        }
    }

    watched
}

#[cfg(test)]
mod tests;
