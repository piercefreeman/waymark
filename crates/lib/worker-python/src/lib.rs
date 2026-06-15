//! The Python worker process spec.

#![warn(missing_docs)]

use std::time::Duration;

mod config;
mod resolve;

pub use config::Config;
pub use resolve::{ResolveError, SpecFactory, resolve};

/// Python worker process spec.
///
/// Holds a resolved [`SpecFactory`] plus the late-bound bridge address.
#[derive(Clone, Debug)]
pub struct Spec {
    bridge_server_addr: std::net::SocketAddr,
    factory: SpecFactory,
}

impl Spec {
    pub(crate) fn new(factory: SpecFactory, bridge_server_addr: std::net::SocketAddr) -> Self {
        Self {
            bridge_server_addr,
            factory,
        }
    }
}

impl waymark_worker_process_spec::Spec for Spec {
    fn prepare_spawn_params(
        &self,
        reservation_id: waymark_worker_reservation::Id,
    ) -> waymark_worker_process::SpawnParams {
        let factory = &self.factory;

        let mut command = tokio::process::Command::new(&factory.program);
        command.args(&factory.args);
        command
            .arg("--bridge")
            .arg(self.bridge_server_addr.to_string())
            .arg("--worker-id")
            .arg(reservation_id.to_string());

        for module in &factory.user_modules {
            command.arg("--user-module").arg(module);
        }

        command.env("PYTHONPATH", &factory.python_path);
        command.current_dir(&factory.working_dir);

        tracing::info!(
            ?reservation_id,
            working_dir = %factory.working_dir.display(),
            python_path = %factory.python_path,
            "prepared python worker spawn params"
        );

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use waymark_worker_process_spec::Spec as _;

    #[tokio::test]
    async fn spawn_params_assembles_expected_command() {
        let config = Config::new()
            .with_script(PathBuf::from("python3"), vec![])
            .with_user_module("mod_a");
        let factory = crate::resolve(config)
            .await
            .expect("resolve should succeed");
        let spec = factory.build("127.0.0.1:9000".parse().expect("addr"));

        let params = spec.prepare_spawn_params(waymark_worker_reservation::Id::from(42));
        let std_command = params.command.as_std();

        assert_eq!(std_command.get_program(), "python3");

        let args: Vec<String> = std_command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert!(args.contains(&"--bridge".to_string()));
        assert!(args.contains(&"127.0.0.1:9000".to_string()));
        assert!(args.contains(&"--user-module".to_string()));
        assert!(args.contains(&"mod_a".to_string()));

        // Default lifecycle timeouts flow through unchanged.
        assert_eq!(params.wait_for_playload_timeout, Duration::from_secs(15));
    }
}
