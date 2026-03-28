pub type Reservation =
    waymark_worker_reservation::Reservation<waymark_worker_message_protocol::Channels>;

pub async fn spawn(
    reservation: Reservation,
    params: waymark_reserved_process::SpawnParams,
) -> Result<(Handle, waymark_worker_message_protocol::Sender), waymark_reserved_process::SpawnError>
{
    let shutdown_params = ShutdownParams {
        graceful_shutdown_timeout: params.graceful_shutdown_timeout,
        kill_timeout: params.kill_timeout,
    };

    let (child, channels) = waymark_reserved_process::spawn(reservation, params).await?;

    let mut tasks = tokio::task::JoinSet::new();

    let (sender, fut) = waymark_worker_message_protocol::setup(channels);

    tasks.spawn(fut);

    let handle = Handle {
        child: Some(child),
        tasks,
        shutdown_params,
    };

    Ok((handle, sender))
}

struct ShutdownParams {
    pub graceful_shutdown_timeout: std::time::Duration,
    pub kill_timeout: std::time::Duration,
}

pub struct Handle {
    /// The managed child process.
    child: Option<waymark_managed_process::Child>,

    /// All of the async tasks managed by this process.
    tasks: tokio::task::JoinSet<()>,

    /// The params used for shutdown.
    shutdown_params: ShutdownParams,
}

impl Handle {
    /// Gracefully shut down the worker process.
    pub async fn shutdown(mut self) -> Result<(), waymark_managed_process::ShutdownError> {
        tracing::info!("shutting down worker");

        // Abort the tasks.
        self.tasks.shutdown().await;

        // Shutdown the managed process gracefully.
        if let Some(child) = self.child.take() {
            let exit_status = child
                .shutdown(
                    self.shutdown_params.graceful_shutdown_timeout,
                    self.shutdown_params.kill_timeout,
                )
                .await?;
            tracing::debug!(?exit_status, "worker child process exited");
        }

        tracing::info!("worker shutdown complete");
        Ok(())
    }
}
