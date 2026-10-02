//! The Python worker process spec.

#![warn(missing_docs)]

use std::{sync::Arc, time::Duration};

mod config;
mod default_runner;

pub use config::{Config, Runner};

/// Prepare the Python worker process spec from the config, detecting the
/// runner if the config has none.
pub async fn prepare(config: Config) -> PreparedSpec {
    let Config {
        runner,
        user_modules,
        extra_python_paths,
    } = config;

    let runner = match runner {
        Some(runner) => runner,
        None => {
            let detected_runner = tokio::task::spawn_blocking(default_runner::detect).await;
            // Nothing here cancels the task, so a join error is only ever
            // a panic.
            match detected_runner {
                Ok(runner) => runner,
                Err(join_error) => std::panic::resume_unwind(join_error.into_panic()),
            }
        }
    };

    let joined_python_path = extra_python_paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(":");

    let python_path = match std::env::var("PYTHONPATH") {
        Ok(existing) if !existing.is_empty() => format!("{existing}:{joined_python_path}"),
        _ => joined_python_path,
    };

    tracing::info!(
        script_path = ?runner.script_path,
        script_args = ?runner.script_args,
        python_path = %python_path,
        "prepared python worker spec"
    );

    PreparedSpec {
        runner,
        user_modules,
        python_path,
    }
}

/// Python worker process spec, prepared and not yet bound to a bridge server.
pub struct PreparedSpec {
    runner: Runner,

    user_modules: Vec<String>,

    python_path: String,
}

impl PreparedSpec {
    /// Bind the prepared spec to the bridge server the workers connect to.
    pub fn bind(self: Arc<Self>, bridge_server_addr: std::net::SocketAddr) -> Spec {
        Spec {
            bridge_server_addr,
            prepared_spec: self,
        }
    }

    /// Turn the prepared spec into a fn that binds it to a bridge server.
    pub fn into_binder(self) -> impl Fn(std::net::SocketAddr) -> Spec {
        let prepared_spec = Arc::new(self);
        move |bridge_server_addr| Arc::clone(&prepared_spec).bind(bridge_server_addr)
    }
}

/// Python worker process spec.
pub struct Spec {
    bridge_server_addr: std::net::SocketAddr,

    prepared_spec: Arc<PreparedSpec>,
}

impl waymark_worker_process_spec::Spec for Spec {
    fn prepare_spawn_params(
        &self,
        reservation_id: waymark_worker_reservation::Id,
    ) -> waymark_worker_process::SpawnParams {
        let mut command = tokio::process::Command::new(&self.prepared_spec.runner.script_path);
        command.args(&self.prepared_spec.runner.script_args);
        command
            .arg("--bridge")
            .arg(self.bridge_server_addr.to_string())
            .arg("--worker-id")
            .arg(reservation_id.to_string());

        for module in &self.prepared_spec.user_modules {
            command.arg("--user-module").arg(module);
        }

        command.env("PYTHONPATH", &self.prepared_spec.python_path);

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
