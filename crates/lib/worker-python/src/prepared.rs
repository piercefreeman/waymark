//! Resolved, ready-to-launch Python worker configuration.
//!
//! [`Config`] is declarative — what the caller *asks for*. [`PreparedConfig`]
//! is the *materialized* result of resolving that against the environment
//! (executable lookup, working directory, `PYTHONPATH`). All fallible and
//! blocking work happens once, here, in [`Config::prepare`].

use std::path::PathBuf;

use crate::config::{Config, default_runner};

/// A fully resolved worker configuration, ready to assemble launch commands
/// from without any further I/O.
pub struct PreparedConfig {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<String>,
    pub(crate) working_dir: PathBuf,
    pub(crate) python_path: String,
    pub(crate) user_modules: Vec<String>,
    pub(crate) timeouts: waymark_worker_process::Timeouts,
}

/// Errors that can occur while resolving a [`Config`] into a [`PreparedConfig`].
#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    /// The current working directory could not be resolved.
    #[error("resolve current working directory")]
    CurrentDir(#[source] std::io::Error),

    /// The blocking preparation task panicked or was cancelled.
    #[error("config preparation task failed")]
    Join(#[source] tokio::task::JoinError),
}

impl Config {
    /// Resolve this declarative config into a [`PreparedConfig`].
    ///
    /// Runs all filesystem probing on a blocking thread (via
    /// [`tokio::task::spawn_blocking`]) so the async runtime is never blocked,
    /// and returns errors instead of panicking.
    pub async fn prepare(self) -> Result<PreparedConfig, PrepareError> {
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

        Ok(PreparedConfig {
            program,
            args,
            working_dir,
            python_path,
            user_modules: self.user_modules,
            timeouts: self.timeouts,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::Config;
    use std::path::PathBuf;

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
}
