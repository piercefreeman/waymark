use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

use crate::Runner;

/// Find the default Python runner.
///
/// Prefers a `waymark-worker` in the directory of the current executable;
/// then one on PATH; otherwise `uv run`. Whether a sibling exists is the
/// installer's doing: `scripts/build_wheel.py` places the
/// `waymark-start-workers` binary and the worker entry point in one scripts
/// directory, while a source build has no sibling and falls through.
pub fn detect() -> Runner {
    match std::env::current_exe() {
        Ok(current_exe) => {
            if let Some(script_path) = find_beside(&current_exe, Path::new("waymark-worker")) {
                tracing::debug!(
                    ?script_path,
                    "found waymark-worker beside the current executable"
                );
                return Runner {
                    script_path,
                    script_args: Vec::new(),
                };
            }
        }
        Err(error) => {
            tracing::debug!(%error, "cannot locate the current executable");
        }
    }

    if let Some(script_path) = find_executable("waymark-worker") {
        tracing::debug!(?script_path, "found waymark-worker in PATH");
        return Runner {
            script_path,
            script_args: Vec::new(),
        };
    }

    tracing::debug!("waymark-worker not found in PATH, falling back to uv");
    Runner {
        script_path: PathBuf::from("uv"),
        script_args: vec![
            "run".to_string(),
            "python".to_string(),
            "-m".to_string(),
            "waymark.worker".to_string(),
        ],
    }
}

/// The executable `bin` in the directory of the executable at `exe`, after
/// resolving `exe` through its symlinks, so a linked launcher is looked up
/// beside its target rather than beside the link.
fn find_beside(exe: &Path, bin: &Path) -> Option<PathBuf> {
    let exe = match std::fs::canonicalize(exe) {
        Ok(exe) => exe,
        Err(error) => {
            tracing::debug!(?exe, %error, "cannot resolve the executable's path");
            return None;
        }
    };

    let candidate = exe.parent()?.join(executable_name(bin));
    if !is_executable_file(&candidate) {
        tracing::debug!(?candidate, "not an executable file");
        return None;
    }

    Some(candidate)
}

/// The file name `bin` runs under. On Windows only a file with an
/// executable extension runs; an extensionless file of the same stem is
/// not a candidate.
fn executable_name(bin: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        bin.with_added_extension("exe")
    }

    #[cfg(not(windows))]
    {
        bin.to_path_buf()
    }
}

/// Search PATH for an executable.
fn find_executable(bin: impl AsRef<Path>) -> Option<PathBuf> {
    let Some(path_var) = std::env::var_os("PATH") else {
        tracing::debug!("PATH is not set");
        return None;
    };

    find_executable_in(bin.as_ref(), &path_var)
}

/// Search the given PATH value for an executable.
fn find_executable_in(bin: &Path, path_var: &OsStr) -> Option<PathBuf> {
    let bin = executable_name(bin);

    for dir in std::env::split_paths(path_var) {
        // An empty entry usually comes from a stray separator rather than
        // a wish to search the current directory.
        if dir.as_os_str().is_empty() {
            tracing::debug!("skipping an empty PATH entry");
            continue;
        }

        // A relative entry resolves against the current directory, here
        // and now, so the spawn runs the file checked here even if the
        // current directory changes later.
        let candidate = match std::path::absolute(dir.join(&bin)) {
            Ok(candidate) => candidate,
            Err(error) => {
                tracing::debug!(?dir, %error, "skipping a PATH entry that cannot be made absolute");
                continue;
            }
        };

        if !is_executable_file(&candidate) {
            tracing::debug!(?candidate, "not an executable file");
            continue;
        }

        return Some(candidate);
    }

    None
}

/// Whether the path is an executable file; symlinks are followed.
fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };

    if !metadata.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        metadata.permissions().mode() & 0o111 != 0
    }

    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(test)]
mod tests;
