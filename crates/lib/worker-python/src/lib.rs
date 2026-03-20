//! The Python worker process spec.

#![warn(missing_docs)]

use std::{path::PathBuf, time::Duration};

mod config;

pub use config::Config;

/// Python worker process spec.
// TODO: rewrite to fully cache effective values, like workdir, as constructor.
pub struct Spec {
    /// The address of the bridge server to connect the worker to.
    pub bridge_server_addr: std::net::SocketAddr,

    /// The worker config.
    pub config: Config,
}

impl waymark_worker_process_spec::Spec for Spec {
    fn prepare_spawn_params(
        &self,
        reservation_id: waymark_worker_reservation::Id,
    ) -> waymark_worker_process::SpawnParams {
        // Determine working directory and module paths
        let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");
        let working_dir = if package_root.is_dir() {
            Some(package_root.clone())
        } else {
            None
        };

        // Build PYTHONPATH with all necessary directories
        let mut module_paths = Vec::new();
        if let Some(root) = working_dir.as_ref() {
            module_paths.push(root.clone());
            let src_dir = root.join("src");
            if src_dir.exists() {
                module_paths.push(src_dir);
            }
            let proto_dir = root.join("proto");
            if proto_dir.exists() {
                module_paths.push(proto_dir);
            }
        }
        module_paths.extend(self.config.extra_python_paths.clone());

        let joined_python_path = module_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(":");

        let python_path = match std::env::var("PYTHONPATH") {
            Ok(existing) if !existing.is_empty() => format!("{existing}:{joined_python_path}"),
            _ => joined_python_path,
        };

        tracing::info!(python_path = %python_path, ?reservation_id, "configured python path for worker");

        // Build the command
        let mut command = tokio::process::Command::new(&self.config.script_path);
        command.args(&self.config.script_args);
        command
            .arg("--bridge")
            .arg(self.bridge_server_addr.to_string())
            .arg("--worker-id")
            .arg(reservation_id.to_string());

        // Add user modules
        for module in &self.config.user_modules {
            command.arg("--user-module").arg(module);
        }

        command.env("PYTHONPATH", python_path);

        if let Some(dir) = working_dir {
            tracing::info!(?dir, "using package root for worker process");
            command.current_dir(dir);
        } else {
            // TODO: move this fallible initialization outside of this impl.
            let cwd = std::env::current_dir().expect("failed to resolve current directory");
            tracing::info!(
                ?cwd,
                "package root missing, using current directory for worker process"
            );
            command.current_dir(cwd);
        }

        waymark_worker_process::SpawnParams {
            command,
            // TODO: move to config
            wait_for_playload_timeout: Duration::from_secs(15),
            shutdown_params: waymark_worker_process::ShutdownParams {
                tasks_graceful_shutdown_timeout: Duration::from_secs(5),
                process_graceful_shutdown_timeout: Duration::from_secs(5),
                process_kill_timeout: Duration::from_secs(10),
            },
        }
    }
}
