//! Resolved, ready-to-launch Python worker configuration.
//!
//! [`Config`] is declarative — what the caller *asks for*. [`PreparedConfig`]
//! is the *materialized* result of resolving that against the environment
//! (executable lookup, working directory, `PYTHONPATH`). All fallible and
//! blocking work happens once, here, in [`Config::prepare`].

use std::path::{Path, PathBuf};

use super::Config;

/// A fully resolved worker configuration, ready to assemble launch commands
/// from without any further I/O.
///
/// The environment (`PYTHONPATH`, current working directory, and the
/// `<crate>/python` package-root probe) is snapshotted once at
/// [`Config::prepare`] time and frozen here; it is *not* re-read when
/// `prepare_spawn_params` runs on a later worker recycle.
#[derive(Clone, Debug)]
pub struct PreparedConfig {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<String>,
    pub(crate) working_dir: PathBuf,
    pub(crate) python_path: String,
    pub(crate) user_modules: Vec<String>,
}

/// Errors that can occur while resolving a [`Config`] into a [`PreparedConfig`].
#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    /// The current working directory could not be resolved.
    #[error("resolve current working directory: {0}")]
    CurrentDir(#[source] std::io::Error),

    /// The blocking preparation task panicked or was cancelled.
    #[error("config preparation task failed: {0}")]
    Join(#[source] tokio::task::JoinError),
}

impl Config {
    /// Resolve this declarative config into a [`PreparedConfig`].
    ///
    /// Runs all filesystem probing on a blocking thread (via
    /// [`tokio::task::spawn_blocking`]) so the async runtime is never blocked,
    /// and returns errors instead of panicking.
    pub async fn prepare(self) -> Result<PreparedConfig, PrepareError> {
        // A panic inside `prepare_blocking` is surfaced as `PrepareError::Join`
        // (via `JoinError`) rather than resumed: config resolution holds no
        // partial state to corrupt, so a clean error is preferable to aborting.
        // (Contrast worker-process, which `resume_unwind`s associated-task panics.)
        tokio::task::spawn_blocking(move || self.prepare_blocking())
            .await
            .map_err(PrepareError::Join)?
    }

    /// The blocking half of [`Config::prepare`]. Must only be called from a
    /// blocking context (e.g. inside `spawn_blocking`).
    fn prepare_blocking(self) -> Result<PreparedConfig, PrepareError> {
        let (program, args) = match self.script_path {
            Some(path) => (path, self.script_args),
            None => default_runner(),
        };

        // The python package ships next to this crate at `<crate>/python`. When
        // present we run from there and add it (plus its `src`/`proto`
        // subdirs) to PYTHONPATH; otherwise we fall back to the current dir.
        let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");
        let package_root_is_dir = package_root.is_dir();

        let working_dir = if package_root_is_dir {
            package_root.clone()
        } else {
            std::env::current_dir().map_err(PrepareError::CurrentDir)?
        };

        let mut module_paths: Vec<PathBuf> = Vec::new();
        if package_root_is_dir {
            module_paths.push(package_root.clone());
            let src_dir = package_root.join("src");
            if src_dir.exists() {
                module_paths.push(src_dir);
            }
            let proto_dir = package_root.join("proto");
            if proto_dir.exists() {
                module_paths.push(proto_dir);
            }
        }
        module_paths.extend(self.extra_python_paths);

        let joined_python_path = module_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(":");

        let python_path = match std::env::var("PYTHONPATH") {
            Ok(existing) if !existing.is_empty() => format!("{existing}:{joined_python_path}"),
            _ => joined_python_path,
        };

        // Logged once, here, at resolution time (not per spawn): records the
        // effective PYTHONPATH and whether we ran from the bundled package root
        // or fell back to the cwd — the usual culprits when a worker can't
        // import its user modules.
        tracing::info!(
            working_dir = %working_dir.display(),
            python_path = %python_path,
            package_root_used = package_root_is_dir,
            "resolved python worker environment"
        );

        Ok(PreparedConfig {
            program,
            args,
            working_dir,
            python_path,
            user_modules: self.user_modules,
        })
    }
}

/// Find the default Python runner.
/// Prefers `waymark-worker` if in PATH, otherwise uses `uv run`.
///
/// Performs blocking filesystem lookups; only call from a blocking context
/// (invoked from `Config::prepare` inside `spawn_blocking`).
fn default_runner() -> (PathBuf, Vec<String>) {
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
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => is_executable(&metadata),
        _ => false,
    }
}

/// Whether `metadata` grants execute permission to anyone (owner/group/other).
///
/// `std` has no `is_executable`, so on unix we test the mode bits directly; on
/// other platforms executability is governed by file extension/ACLs rather than
/// a permission bit, so any regular file is treated as runnable.
#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    // Execute bits for owner, group, and other (`--x--x--x`).
    const EXECUTABLE_BITS: u32 = 0o111;
    metadata.permissions().mode() & EXECUTABLE_BITS != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn prepare_resolves_an_explicit_script() {
        let prepared = Config::new()
            .with_script(PathBuf::from("/usr/bin/python3"), vec!["-u".to_string()])
            .with_user_module("my_module")
            .prepare()
            .await
            .expect("prepare should succeed for an explicit script");

        assert_eq!(prepared.program, PathBuf::from("/usr/bin/python3"));
        assert_eq!(prepared.args, vec!["-u".to_string()]);
        assert_eq!(prepared.user_modules, vec!["my_module".to_string()]);
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
    fn is_executable_file_rejects_non_executable_file() {
        // `main` accepted any `is_file()`; this crate adds the 0o111 exec-bit
        // gate. The crate's own Cargo.toml sits next to it and is not executable.
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        assert!(!is_executable_file(&manifest));
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
