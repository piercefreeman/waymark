//! The check and the update of a Rust-code migration's crate capture, run
//! by the test [`crate::migration_identity!`] adds to the migration's
//! crate.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use waymark_migrations_rust_code_core::{
    CRATE_CAPTURE_FILE, CrateCapture, ReadCrateCaptureError, read_crate_capture,
    render_crate_capture,
};
use waymark_migrations_source_checksums_core::source_digest;

pub mod dependencies;

/// The entries of cargo's package list that cargo makes when it packages
/// a crate: they are not files of the crate's directory, or are another
/// crate's to keep.
const GENERATED_FILES: [&str; 3] = [".cargo_vcs_info.json", "Cargo.lock", "Cargo.toml.orig"];

/// The place of the file at `path`, a path from `manifest_dir` with `/`
/// between its parts.
fn place_from(manifest_dir: &Path, path: &str) -> PathBuf {
    let mut place = manifest_dir.to_path_buf();

    for part in path.split('/') {
        place.push(part);
    }

    place
}

/// Error returned by [`compute_crate_capture`].
#[derive(Debug, thiserror::Error)]
pub enum ComputeCrateCaptureError {
    /// Cargo could not be run.
    #[error("running cargo for the files of the package: {0}")]
    RunCargo(#[source] std::io::Error),

    /// Cargo did not list the files of the package.
    #[error("cargo did not list the files of the package ({status}): {stderr}")]
    ListPackageFiles {
        /// How cargo exited.
        status: std::process::ExitStatus,

        /// What cargo wrote to its standard error.
        stderr: String,
    },

    /// Cargo listed a file whose path is not Unicode.
    #[error("cargo listed a file whose path is not valid UTF-8: {0}")]
    PackageFileNotUtf8(#[source] std::string::FromUtf8Error),

    /// A file of the package could not be read.
    #[error("reading {}: {error}", .path.display())]
    ReadFile {
        /// The file.
        path: PathBuf,

        /// Why it could not be read.
        #[source]
        error: std::io::Error,
    },

    /// The dependencies of the package could not be told.
    #[error("dependencies: {0}")]
    Dependencies(#[source] dependencies::OfPackageError),
}

/// Compute the crate capture the crate `package`, whose manifest is in
/// `manifest_dir`, makes: the digest of each file cargo counts as the
/// crate's package, and the line of each package the crate depends on.
///
/// A file is under its path from `manifest_dir`, with `/` between its
/// parts. The files cargo makes itself and the capture's own file are not
/// among them. A dependency whose content nothing pins is an error.
/// Nothing is written.
pub fn compute_crate_capture(
    manifest_dir: &Path,
    package: &str,
) -> Result<CrateCapture, ComputeCrateCaptureError> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());

    let listed = std::process::Command::new(cargo)
        .args([
            "package",
            "--list",
            "--allow-dirty",
            "--offline",
            "--package",
        ])
        .arg(package)
        .current_dir(manifest_dir)
        .output()
        .map_err(ComputeCrateCaptureError::RunCargo)?;

    if !listed.status.success() {
        return Err(ComputeCrateCaptureError::ListPackageFiles {
            status: listed.status,
            stderr: String::from_utf8_lossy(&listed.stderr).into_owned(),
        });
    }

    let listed =
        String::from_utf8(listed.stdout).map_err(ComputeCrateCaptureError::PackageFileNotUtf8)?;

    let paths = listed
        .lines()
        .map(|line| line.replace('\\', "/"))
        .filter(|path| !path.is_empty())
        .filter(|path| !GENERATED_FILES.contains(&path.as_str()))
        .filter(|path| path != CRATE_CAPTURE_FILE);

    let mut files = BTreeMap::new();

    for path in paths {
        let place = place_from(manifest_dir, &path);

        let bytes = std::fs::read(&place)
            .map_err(|error| ComputeCrateCaptureError::ReadFile { path: place, error })?;

        files.insert(path, source_digest(&bytes));
    }

    let dependencies =
        dependencies::of_package(manifest_dir).map_err(ComputeCrateCaptureError::Dependencies)?;

    Ok(CrateCapture {
        files,
        dependencies,
    })
}

/// Error returned by [`write_crate_capture`].
#[derive(Debug, thiserror::Error)]
pub enum WriteCrateCaptureError {
    /// The capture could not be put into the text of its file.
    #[error("rendering the capture: {0}")]
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

/// Write `capture` as the crate capture of the crate whose manifest is in
/// `manifest_dir`.
pub fn write_crate_capture(
    manifest_dir: &Path,
    capture: &CrateCapture,
) -> Result<(), WriteCrateCaptureError> {
    let text = render_crate_capture(capture).map_err(WriteCrateCaptureError::Render)?;

    let path = manifest_dir.join(CRATE_CAPTURE_FILE);

    std::fs::write(&path, text).map_err(|error| WriteCrateCaptureError::Write { path, error })
}

/// One way the crate capture of a migration is not what its crate makes
/// it.
#[derive(Debug)]
pub enum Difference {
    /// The crate has no capture.
    NoCapture,

    /// The file of the capture is not a crate capture.
    UnreadableCapture(serde_json::Error),

    /// The crate has a file the capture does not.
    FileAdded(String),

    /// The capture has a file the crate does not.
    FileRemoved(String),

    /// A file has another digest than the capture has for it.
    FileChanged(String),

    /// The crate depends on a package the capture does not have.
    DependencyAdded(String),

    /// The capture has a package the crate does not depend on.
    DependencyRemoved(String),
}

impl std::fmt::Display for Difference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCapture => write!(formatter, "the crate has no {CRATE_CAPTURE_FILE}"),
            Self::UnreadableCapture(error) => {
                write!(formatter, "{CRATE_CAPTURE_FILE} cannot be read: {error}")
            }
            Self::FileAdded(path) => write!(formatter, "file added: {path}"),
            Self::FileRemoved(path) => write!(formatter, "file removed: {path}"),
            Self::FileChanged(path) => write!(formatter, "file changed: {path}"),
            Self::DependencyAdded(dependency) => {
                write!(formatter, "dependency added: {dependency}")
            }
            Self::DependencyRemoved(dependency) => {
                write!(formatter, "dependency removed: {dependency}")
            }
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

/// How `stored`, the capture as its file has it, differs from `computed`,
/// the capture the crate makes: nothing when they are the same.
fn differences(stored: &CrateCapture, computed: &CrateCapture) -> Vec<Difference> {
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

    for dependency in computed.dependencies.difference(&stored.dependencies) {
        differences.push(Difference::DependencyAdded(dependency.clone()));
    }

    for dependency in stored.dependencies.difference(&computed.dependencies) {
        differences.push(Difference::DependencyRemoved(dependency.clone()));
    }

    differences
}

/// Error returned by [`check_or_update`].
#[derive(Debug, thiserror::Error)]
pub enum CheckOrUpdateError {
    /// The capture the crate makes could not be computed.
    #[error("computing the crate capture: {0}")]
    ComputeCrateCapture(#[source] ComputeCrateCaptureError),

    /// The capture could not be read.
    #[error("reading the crate capture: {0}")]
    ReadCrateCapture(#[source] ReadCrateCaptureError),

    /// The capture could not be written.
    #[error("writing the crate capture: {0}")]
    WriteCrateCapture(#[source] WriteCrateCaptureError),

    /// The capture is not what the crate makes it.
    #[error(
        "{CRATE_CAPTURE_FILE} is not what the crate makes it:{}",
        difference_lines(.differences)
    )]
    OutOfDate {
        /// How the capture differs.
        differences: Vec<Difference>,
    },
}

/// Check the crate capture of the crate `package`, whose manifest is in
/// `manifest_dir`, against the files cargo counts as the crate's package
/// and the packages the crate depends on. When they differ and `update` is
/// `true`, write the capture the crate makes instead.
///
/// A dependency whose content nothing pins is an error either way.
pub fn check_or_update(
    manifest_dir: &Path,
    package: &str,
    update: bool,
) -> Result<(), CheckOrUpdateError> {
    let computed = compute_crate_capture(manifest_dir, package)
        .map_err(CheckOrUpdateError::ComputeCrateCapture)?;

    // A capture that is not there or is not a capture is a difference:
    // writing it anew is what mends both.
    let differences = match read_crate_capture(manifest_dir) {
        Ok(stored) => differences(&stored, &computed),
        Err(ReadCrateCaptureError::NotFound { path: _ }) => vec![Difference::NoCapture],
        Err(ReadCrateCaptureError::Parse { path: _, error }) => {
            vec![Difference::UnreadableCapture(error)]
        }
        Err(error @ ReadCrateCaptureError::Read { .. }) => {
            return Err(CheckOrUpdateError::ReadCrateCapture(error));
        }
    };

    if differences.is_empty() {
        return Ok(());
    }

    if !update {
        return Err(CheckOrUpdateError::OutOfDate { differences });
    }

    write_crate_capture(manifest_dir, &computed).map_err(CheckOrUpdateError::WriteCrateCapture)
}

#[cfg(test)]
mod tests;
