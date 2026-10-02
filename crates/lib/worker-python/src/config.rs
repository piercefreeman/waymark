use std::path::PathBuf;

/// Configuration for spawning Python workers.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// The runner to start the worker with; detected when `None`.
    pub runner: Option<Runner>,

    /// Python module(s) to preload (contains @action definitions)
    pub user_modules: Vec<String>,

    /// Additional paths to add to PYTHONPATH
    pub extra_python_paths: Vec<PathBuf>,
}

/// The command that starts a Python worker.
#[derive(Clone, Debug)]
pub struct Runner {
    /// Path to the script/executable to run (e.g., "uv" or "waymark-worker")
    pub script_path: PathBuf,

    /// Arguments to pass before the worker-specific args
    pub script_args: Vec<String>,
}

impl Config {
    /// Create a new config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the runner to start the worker with.
    pub fn with_runner(mut self, runner: Runner) -> Self {
        self.runner = Some(runner);
        self
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
}
