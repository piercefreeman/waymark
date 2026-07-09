//! Resolve ['Config'] into a ['SpecFactory'].

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::Config;

/// A fully resolved worker configuration, ready to mint a [`crate::Spec`]
/// without any further I/O.
#[derive(Clone, Debug)]
pub struct SpecFactory {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<String>,
    pub(crate) working_dir: PathBuf,
    pub(crate) python_path: String,
    pub(crate) user_modules: Vec<String>,
}

/// Errors that can occur while resolving a [`Config`] into a [`SpecFactory`].
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// The current working directory could not be resolved.
    #[error("resolve current working directory: {0}")]
    CurrentDir(#[source] std::io::Error),

    /// The blocking resolution task panicked or was cancelled.
    #[error("config resolution task failed: {0}")]
    Join(#[source] tokio::task::JoinError),
}

/// Resolve a declarative [`Config`] into a [`SpecFactory`] without blocking the runtime.
pub async fn resolve(config: Config) -> Result<SpecFactory, ResolveError> {
    tokio::task::spawn_blocking(move || resolve_blocking(config))
        .await
        .map_err(ResolveError::Join)?
}

/// Perform blocking work to resolve a config.
fn resolve_blocking(config: Config) -> Result<SpecFactory, ResolveError> {
    let (program, args) = match config.script_path {
        Some(path) => (path, config.script_args),
        None => default_runner(),
    };

    let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");
    let package_root_is_dir = package_root.is_dir();

    let working_dir = if package_root_is_dir {
        package_root.clone()
    } else {
        std::env::current_dir().map_err(ResolveError::CurrentDir)?
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
    module_paths.extend(config.extra_python_paths);

    let joined_python_path = module_paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(":");

    let python_path = match std::env::var("PYTHONPATH") {
        Ok(existing) if !existing.is_empty() => format!("{existing}:{joined_python_path}"),
        _ => joined_python_path,
    };

    tracing::info!(
        working_dir = %working_dir.display(),
        python_path = %python_path,
        package_root_used = package_root_is_dir,
        "resolved python worker environment"
    );

    Ok(SpecFactory {
        program,
        args,
        working_dir,
        python_path,
        user_modules: config.user_modules,
    })
}

/// Find the default Python runner.
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

/// Search PATH for an executable file, following symlinks.
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

/// Return true if `path` resolves (following symlinks) to a regular file that
/// is executable.
fn is_executable_file(path: &Path) -> bool {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => is_executable(&metadata),
        _ => false,
    }
}

/// Return whether `metadata` grants execute permission to anyone (owner/group/other).
#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    // owner, group, and other (`--x--x--x`).
    const EXECUTABLE_BITS: u32 = 0o111;
    metadata.permissions().mode() & EXECUTABLE_BITS != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    true
}

/// Assemble the launch command.
pub(crate) fn build_command(
    factory: &SpecFactory,
    bridge_server_addr: SocketAddr,
    reservation_id: waymark_worker_reservation::Id,
) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(&factory.program);
    command.args(&factory.args);
    command
        .arg("--bridge")
        .arg(bridge_server_addr.to_string())
        .arg("--worker-id")
        .arg(reservation_id.to_string());

    for module in &factory.user_modules {
        command.arg("--user-module").arg(module);
    }

    command.env("PYTHONPATH", &factory.python_path);
    command.current_dir(&factory.working_dir);

    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_resolves_an_explicit_script() {
        let factory = resolve(
            Config::new()
                .with_script(PathBuf::from("/usr/bin/python3"), vec!["-u".to_string()])
                .with_user_module("my_module"),
        )
        .await
        .expect("resolve should succeed for an explicit script");

        assert_eq!(factory.program, PathBuf::from("/usr/bin/python3"));
        assert_eq!(factory.args, vec!["-u".to_string()]);
        assert_eq!(factory.user_modules, vec!["my_module".to_string()]);
    }

    #[cfg(unix)]
    #[test]
    fn is_executable_file_accepts_real_executable_via_symlink() {
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
