use std::path::{Path, PathBuf};
use waymark_worker_process::Timeouts;

/// Configuration for spawning Python workers.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// Explicit path to the script/executable to run (e.g. "uv" or
    /// "waymark-worker"). `None` means auto-detect during `Config::prepare`.
    pub script_path: Option<PathBuf>,

    /// Arguments to pass before the worker-specific args.
    pub script_args: Vec<String>,

    /// Python module(s) to preload (contains @action definitions).
    pub user_modules: Vec<String>,

    /// Additional paths to add to PYTHONPATH.
    pub extra_python_paths: Vec<PathBuf>,

    /// Startup/shutdown lifecycle timeouts for the worker process.
    pub timeouts: Timeouts,
}

impl Config {
    /// Create a new config with default runner detection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the user module to preload.
    pub fn with_user_module(mut self, module: &str) -> Self {
        self.user_modules = vec![module.to_string()];
        self
    }

    /// Set multiple user modules to preload.
    pub fn with_user_modules(mut self, modules: Vec<String>) -> Self {
        self.user_modules = modules;
        self
    }

    /// Add extra paths to PYTHONPATH.
    pub fn with_python_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.extra_python_paths = paths;
        self
    }

    /// Set an explicit script/executable and its leading arguments, bypassing
    /// auto-detection.
    pub fn with_script(mut self, script_path: PathBuf, script_args: Vec<String>) -> Self {
        self.script_path = Some(script_path);
        self.script_args = script_args;
        self
    }
}

/// Find the default Python runner.
/// Prefers `waymark-worker` if in PATH, otherwise uses `uv run`.
///
/// Performs blocking filesystem lookups; only call from a blocking context
/// (invoked from `Config::prepare` inside `spawn_blocking`).
pub(crate) fn default_runner() -> (PathBuf, Vec<String>) {
    if let Some(path) = find_executable("waymark-worker") {
        return (path, Vec::new());
    }
    (
        PathBuf::from("uv"),
        vec![
            "run".to_string(),
            "python".to_string(),
            "-m".to_string(),
            "waymark.worker".to_string(),
        ],
    )
}

/// Search PATH for an executable file.
///
/// Follows symlinks (so a symlinked binary on PATH is found) and, on unix,
/// requires the execute bit to be set so we don't return a non-executable
/// file that merely shares the name.
fn find_executable(bin: impl AsRef<Path>) -> Option<PathBuf> {
    let bin = bin.as_ref();
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(bin);
        if is_executable_file(&candidate) {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let exe_candidate = dir.join(bin.with_added_extension("exe"));
            if is_executable_file(&exe_candidate) {
                return Some(exe_candidate);
            }
        }
    }
    None
}

/// Returns true if `path` resolves (following symlinks) to a regular file that
/// is executable.
fn is_executable_file(path: &Path) -> bool {
    // `std::fs::metadata` follows symlinks (unlike `symlink_metadata`).
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
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
    fn test_config_builder() {
        let config = Config::new()
            .with_user_module("my_module")
            .with_python_paths(vec![PathBuf::from("/extra/path")]);

        assert_eq!(config.user_modules, vec!["my_module".to_string()]);
        assert_eq!(
            config.extra_python_paths,
            vec![PathBuf::from("/extra/path")]
        );
    }

    #[test]
    fn test_config_with_multiple_modules() {
        let config =
            Config::new().with_user_modules(vec!["module1".to_string(), "module2".to_string()]);

        assert_eq!(config.user_modules, vec!["module1", "module2"]);
    }

    #[cfg(unix)]
    #[test]
    fn is_executable_file_accepts_real_executable_via_symlink() {
        // /bin/sh is an executable file and is a symlink on many systems,
        // which exercises the symlink-following path.
        assert!(is_executable_file(Path::new("/bin/sh")));
    }

    #[cfg(unix)]
    #[test]
    fn is_executable_file_rejects_missing_path() {
        assert!(!is_executable_file(Path::new(
            "/definitely/not/a/real/binary/xyzzy"
        )));
    }

    #[cfg(unix)]
    #[test]
    fn find_executable_locates_sh_on_path() {
        assert!(find_executable("sh").is_some());
    }

    #[test]
    fn default_runner_detection() {
        let (path, args) = default_runner();
        if args.is_empty() {
            assert!(path.to_string_lossy().contains("waymark-worker"));
        } else {
            assert_eq!(path, PathBuf::from("uv"));
            assert_eq!(args, vec!["run", "python", "-m", "waymark.worker"]);
        }
    }
}
