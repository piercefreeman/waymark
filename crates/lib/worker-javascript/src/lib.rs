//! JavaScript worker process specification.

#![warn(missing_docs)]

use std::{path::PathBuf, time::Duration};

/// Configuration for a JavaScript action worker.
#[derive(Clone, Debug)]
pub struct Config {
    /// Installed `@waymark/nextjs` worker command.
    pub command: PathBuf,

    /// Generated ESM action bundle loaded by the worker.
    pub action_bundle: PathBuf,
}

impl Config {
    /// Configure a worker using the installed `waymark-worker-node` command.
    pub fn new(action_bundle: PathBuf) -> Self {
        Self {
            command: PathBuf::from("waymark-worker-node"),
            action_bundle,
        }
    }

    /// Override the worker command.
    pub fn with_command(mut self, command: PathBuf) -> Self {
        self.command = command;
        self
    }
}

/// JavaScript worker process specification.
pub struct Spec {
    /// Address of the worker bridge.
    pub bridge_server_addr: std::net::SocketAddr,

    /// Worker configuration.
    pub config: Config,
}

impl waymark_worker_process_spec::Spec for Spec {
    fn action_runtime() -> waymark_action_core::ActionRuntime {
        waymark_action_core::ActionRuntime::JavaScript
    }

    fn prepare_spawn_params(
        &self,
        reservation_id: waymark_worker_reservation::Id,
    ) -> waymark_worker_process::SpawnParams {
        let mut command = tokio::process::Command::new(&self.config.command);
        command
            .arg("--bridge")
            .arg(self.bridge_server_addr.to_string())
            .arg("--worker-id")
            .arg(reservation_id.to_string())
            .arg("--bundle")
            .arg(&self.config.action_bundle);

        waymark_worker_process::SpawnParams {
            command,
            wait_for_playload_timeout: Duration::from_secs(15),
            shutdown_params: waymark_worker_process::ShutdownParams {
                tasks_graceful_shutdown_timeout: Duration::from_secs(5),
                process_graceful_shutdown_timeout: Duration::from_secs(5),
                process_kill_timeout: Duration::from_secs(10),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use waymark_worker_process_spec::Spec as _;

    use super::*;

    #[test]
    fn spawn_command_carries_bridge_identity_and_bundle() {
        let spec = Spec {
            bridge_server_addr: "127.0.0.1:24119".parse().unwrap(),
            config: Config::new(PathBuf::from("/app/actions.mjs"))
                .with_command(PathBuf::from("node-worker")),
        };

        let reservation_id = waymark_worker_reservation::Id::default();
        let params = spec.prepare_spawn_params(reservation_id);
        let command = params.command.as_std();
        let arguments = command
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(command.get_program(), "node-worker");
        assert_eq!(
            arguments,
            vec![
                "--bridge".to_owned(),
                "127.0.0.1:24119".to_owned(),
                "--worker-id".to_owned(),
                reservation_id.to_string(),
                "--bundle".to_owned(),
                "/app/actions.mjs".to_owned(),
            ]
        );
        assert_eq!(
            Spec::action_runtime(),
            waymark_action_core::ActionRuntime::JavaScript
        );
    }
}
