use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

use crate::Runner;

/// Find the default Python runner.
/// Prefers `waymark-worker` if in PATH, otherwise uses `uv run`.
pub fn detect() -> Runner {
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
    // On Windows only a file with an executable extension runs; an
    // extensionless file of the same stem is not a candidate.
    #[cfg(windows)]
    let bin = bin.with_added_extension("exe");
    #[cfg(windows)]
    let bin = bin.as_path();

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
        let candidate = match std::path::absolute(dir.join(bin)) {
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
mod tests {
    use super::*;

    #[test]
    fn test_default_runner_detection() {
        // Should return uv as fallback if waymark-worker not in PATH
        let runner = detect();
        // Either waymark-worker was found, or we get uv with args
        if runner.script_args.is_empty() {
            assert!(
                runner
                    .script_path
                    .to_string_lossy()
                    .contains("waymark-worker")
            );
        } else {
            assert_eq!(runner.script_path, PathBuf::from("uv"));
            assert_eq!(
                runner.script_args,
                vec!["run", "python", "-m", "waymark.worker"]
            );
        }
    }

    /// A fresh empty directory under the system temp directory.
    #[cfg(unix)]
    fn fresh_temp_dir(test_name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "waymark-worker-python-{test_name}-{}",
            std::process::id()
        ));
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[cfg(unix)]
    fn write_file_with_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::write(path, "").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn test_find_executable_skips_non_executable_file() {
        let root = fresh_temp_dir("skips-non-executable");
        let first_dir = root.join("first");
        let second_dir = root.join("second");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        write_file_with_mode(&first_dir.join("tool"), 0o644);
        write_file_with_mode(&second_dir.join("tool"), 0o755);

        let path_var = std::env::join_paths([&first_dir, &second_dir]).unwrap();
        let found = find_executable_in(Path::new("tool"), &path_var);

        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(found, Some(second_dir.join("tool")));
    }

    #[cfg(unix)]
    #[test]
    fn test_find_executable_follows_symlink() {
        let root = fresh_temp_dir("follows-symlink");
        let target_dir = root.join("target");
        let link_dir = root.join("link");
        std::fs::create_dir_all(&target_dir).unwrap();
        std::fs::create_dir_all(&link_dir).unwrap();
        write_file_with_mode(&target_dir.join("tool"), 0o755);
        std::os::unix::fs::symlink(target_dir.join("tool"), link_dir.join("tool")).unwrap();

        let path_var = std::env::join_paths([&link_dir]).unwrap();
        let found = find_executable_in(Path::new("tool"), &path_var);

        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(found, Some(link_dir.join("tool")));
    }

    #[cfg(unix)]
    #[test]
    fn test_find_executable_resolves_relative_entry() {
        let root = fresh_temp_dir("resolves-relative");
        write_file_with_mode(&root.join("tool"), 0o755);

        // The same directory, reached from the current directory: one `..`
        // per component below the root, then the path without its root.
        let current_dir = std::env::current_dir().unwrap();
        let mut relative_dir = PathBuf::new();
        for _ in current_dir.components().skip(1) {
            relative_dir.push("..");
        }
        relative_dir.push(root.strip_prefix("/").unwrap());

        let path_var = std::env::join_paths([&relative_dir]).unwrap();
        let found = find_executable_in(Path::new("tool"), &path_var);

        let found = found.unwrap();
        let found_canonical = std::fs::canonicalize(&found).unwrap();
        let expected_canonical = std::fs::canonicalize(root.join("tool")).unwrap();

        std::fs::remove_dir_all(&root).unwrap();
        assert!(found.is_absolute());
        assert_eq!(found_canonical, expected_canonical);
    }
}
