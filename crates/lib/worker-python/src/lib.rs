//! The Python worker process spec.

#![warn(missing_docs)]

mod config;
mod prepared;

pub use config::Config;
pub use prepared::{PrepareError, PreparedConfig};

/// Python worker process spec.
///
/// Holds a fully [`PreparedConfig`] plus the late-bound bridge address. All
/// fallible/blocking setup already happened in [`Config::prepare`], so
/// `prepare_spawn_params` is pure: it only assembles a command from cached
/// values and never performs I/O or panics.
pub struct Spec {
    /// The address of the bridge server to connect the worker to.
    pub bridge_server_addr: std::net::SocketAddr,

    /// The resolved worker configuration.
    pub prepared: PreparedConfig,
}

impl waymark_worker_process_spec::Spec for Spec {
    fn prepare_spawn_params(
        &self,
        reservation_id: waymark_worker_reservation::Id,
    ) -> waymark_worker_process::SpawnParams {
        let prepared = &self.prepared;

        let mut command = tokio::process::Command::new(&prepared.program);
        command.args(&prepared.args);
        command
            .arg("--bridge")
            .arg(self.bridge_server_addr.to_string())
            .arg("--worker-id")
            .arg(reservation_id.to_string());

        for module in &prepared.user_modules {
            command.arg("--user-module").arg(module);
        }

        command.env("PYTHONPATH", &prepared.python_path);
        command.current_dir(&prepared.working_dir);

        tracing::info!(
            ?reservation_id,
            working_dir = %prepared.working_dir.display(),
            "prepared python worker spawn params"
        );

        waymark_worker_process::SpawnParams {
            command,
            timeouts: prepared.timeouts,
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
        let prepared = Config::new()
            .with_script(PathBuf::from("python3"), vec![])
            .with_user_module("mod_a")
            .prepare()
            .await
            .expect("prepare should succeed");

        let spec = Spec {
            bridge_server_addr: "127.0.0.1:9000".parse().expect("valid addr"),
            prepared,
        };

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
        assert_eq!(
            params.timeouts.wait_for_payload,
            std::time::Duration::from_secs(15)
        );
    }
}
