//! The crate capture of a Rust-code migration: what the files of its
//! crate and the packages it depends on were when the migration was
//! approved.
//!
//! The proc macros of the Rust-code migrations and the check their
//! crates' tests run both read it.

#![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The file of a migration's crate capture, beside the crate's manifest.
pub const CRATE_CAPTURE_FILE: &str = "migrations-engine-crate-capture.json";

/// The crate capture of a Rust-code migration.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrateCapture {
    /// The digest of each file of the crate, by the file's path from the
    /// manifest's directory with `/` between its parts. The capture's own
    /// file is not among them.
    pub files: BTreeMap<String, String>,

    /// The packages the crate depends on, each as one entry.
    pub dependencies: BTreeSet<String>,
}

/// Error returned by [`read_crate_capture`].
#[derive(Debug, thiserror::Error)]
pub enum ReadCrateCaptureError {
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

    /// The file is not a crate capture.
    #[error("{} is not a crate capture: {error}", .path.display())]
    Parse {
        /// The file.
        path: PathBuf,

        /// Why it is not.
        #[source]
        error: serde_json::Error,
    },
}

/// Read the crate capture of the crate whose manifest is in
/// `manifest_dir`.
pub fn read_crate_capture(manifest_dir: &Path) -> Result<CrateCapture, ReadCrateCaptureError> {
    let path = manifest_dir.join(CRATE_CAPTURE_FILE);

    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ReadCrateCaptureError::NotFound { path });
        }
        Err(error) => return Err(ReadCrateCaptureError::Read { path, error }),
    };

    serde_json::from_str(&text).map_err(|error| ReadCrateCaptureError::Parse { path, error })
}

/// The text of the file of `capture`: the files' paths in their order,
/// then the dependencies in theirs, one to a line.
pub fn render_crate_capture(capture: &CrateCapture) -> Result<String, serde_json::Error> {
    let mut text = serde_json::to_string_pretty(capture)?;

    text.push('\n');

    Ok(text)
}

#[cfg(test)]
mod tests;
