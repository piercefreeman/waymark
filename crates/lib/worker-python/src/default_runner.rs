use std::path::{Path, PathBuf};

use crate::Runner;

/// Find the default Python runner.
/// Prefers `waymark-worker` if in PATH, otherwise uses `uv run`.
pub fn detect() -> Runner {
    if let Some(script_path) = find_executable("waymark-worker") {
        return Runner {
            script_path,
            script_args: Vec::new(),
        };
    }
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

/// Search PATH for an executable.
fn find_executable(bin: impl AsRef<Path>) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(bin.as_ref());
        // BUG: this code doesn't allow symlinks/junctions.
        // TODO: rewrite this to do it correctly
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let exe_candidate = dir.join(bin.as_ref().with_added_extension("exe"));
            if exe_candidate.is_file() {
                return Some(exe_candidate);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests;
