//! Worker process lifecycle utilities.
//!
//! This crate coordinates three parts of a worker's lifecycle:
//! - process spawning,
//! - message protocol channel attachment via a reservation,
//! - and graceful shutdown for both async tasks and the child process.

#![warn(missing_docs)]

use std::time::Duration;

/// Worker channel reservation for the message protocol.
pub type Reservation =
    waymark_worker_reservation::Reservation<waymark_worker_message_protocol::Channels>;

/// Parameters used to spawn and initialize a worker process.
pub struct SpawnParams {
    /// Process command used to spawn the worker binary.
    pub command: tokio::process::Command,

    /// Maximum time to wait for the worker to attach to the reservation.
    pub wait_for_playload_timeout: Duration,

    /// Shutdown behavior used when startup or later teardown occurs.
    pub shutdown_params: ShutdownParams,
}

/// Errors returned by [`spawn`].
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    /// Failed to spawn the worker child process.
    #[error("spawn: {0}")]
    Spawn(std::io::Error),

    /// Timed out while waiting for the worker to attach.
    #[error("timed out after waiting for worker to attach")]
    ReservationWaitTimeout {
        /// Configured wait duration that elapsed.
        timeout: Duration,
    },

    /// Reservation wait was cancelled or otherwise failed.
    #[error("reservation wait: {0}")]
    ReservationWaitError(waymark_worker_reservation::ReservationCancelledError),
}

/// Spawn a worker process and wait for it to attach to the reservation.
///
/// This function:
/// - starts the configured child process,
/// - waits for message channels from `reservation` up to
///   [`SpawnParams::wait_for_playload_timeout`],
/// - starts the worker message protocol loop as an associated task,
/// - and returns a [`Handle`] plus a [`waymark_worker_message_protocol::Sender`].
///
/// If reservation attachment fails or times out, the child process is shut
/// down before returning a [`SpawnError`].
pub async fn spawn(
    reservation: Reservation,
    params: SpawnParams,
) -> Result<(Handle, waymark_worker_message_protocol::Sender), SpawnError> {
    let SpawnParams {
        wait_for_playload_timeout,
        command,
        shutdown_params,
    } = params;

    // Spawn the process.
    let child = waymark_managed_process::spawn(command).map_err(SpawnError::Spawn)?;

    // Wait for the worker to connect (with timeout).
    let result = tokio::time::timeout(wait_for_playload_timeout, reservation.wait()).await;
    let result = match result {
        Ok(Ok(channels)) => Ok(channels),
        Ok(Err(err)) => Err(SpawnError::ReservationWaitError(err)),
        Err(tokio::time::error::Elapsed { .. }) => Err(SpawnError::ReservationWaitTimeout {
            timeout: wait_for_playload_timeout,
        }),
    };

    // Pass the payload or terminate the process gracefully.
    let channels = match result {
        Ok(channels) => channels,
        Err(error) => {
            let shutdown_result = shutdown_params.shutdown_child_process(child).await;
            tracing::debug!(
                ?error,
                ?shutdown_result,
                "worker process shut down after failed payload wait"
            );
            return Err(error);
        }
    };

    tracing::info!("worker process connected");

    // Prepare the join set for all worker-associated tasks and a cancellation
    // token for graceful task termination.
    let mut tasks = tokio::task::JoinSet::new();
    let task_shutdown_token = tokio_util::sync::CancellationToken::new();

    // Set up the message protocol.
    let (sender, fut) = waymark_worker_message_protocol::setup(channels);
    let fut = {
        let task_shutdown_token = task_shutdown_token.clone();
        async move {
            let output = task_shutdown_token
                .clone()
                .run_until_cancelled_owned(fut)
                .await;
            if output.is_none() {
                tracing::debug!(
                    message =
                        "worker message protocol loop has just shut down via future cancellation"
                );
            }
        }
    };

    // Spawn the message protocol loop into the worker-associated tasks set.
    tasks.spawn(fut);

    let task_shutdown_guard = task_shutdown_token.clone().drop_guard();
    let handle = Handle {
        child: Some(child),
        tasks,
        task_shutdown_token,
        task_shutdown_guard,
        shutdown_params,
    };

    Ok((handle, sender))
}

/// Parameters controlling graceful worker shutdown.
pub struct ShutdownParams {
    /// Maximum time to wait for spawned worker tasks to finish gracefully.
    pub tasks_graceful_shutdown_timeout: Duration,

    /// Grace period to allow the process to exit after termination is requested.
    pub process_graceful_shutdown_timeout: Duration,

    /// Maximum time to wait after sending a force-kill signal.
    pub process_kill_timeout: Duration,
}

/// Handle to a spawned worker process and its associated tasks.
pub struct Handle {
    /// The managed child process.
    child: Option<waymark_managed_process::Child>,

    /// All of the async tasks managed by this process.
    tasks: tokio::task::JoinSet<()>,

    /// The task graceful shutdown token.
    task_shutdown_token: tokio_util::sync::CancellationToken,

    /// The task shutdown drop guard.
    task_shutdown_guard: tokio_util::sync::DropGuard,

    /// The params used for shutdown.
    shutdown_params: ShutdownParams,
}

impl Handle {
    /// Gracefully shut down the worker process.
    ///
    /// Shutdown ordering:
    /// - cancel associated tasks via the shared cancellation token,
    /// - wait for those tasks to finish (or force-abort on timeout),
    /// - then shut down the child process.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying managed process fails to terminate
    /// within configured limits.
    pub async fn shutdown(mut self) -> Result<(), waymark_managed_process::ShutdownError> {
        tracing::info!("shutting down worker");

        // Start graceful termination of the tasks.
        drop(self.task_shutdown_guard);

        // Wait for graceful shutdown of the tasks.
        self.shutdown_params.shutdown_tasks(&mut self.tasks).await;

        // Shutdown the managed process gracefully.
        if let Some(child) = self.child.take() {
            let exit_status = self.shutdown_params.shutdown_child_process(child).await?;
            tracing::debug!(?exit_status, "worker child process exited");
        }

        tracing::info!("worker shutdown complete");
        Ok(())
    }

    /// Get a cancellation future that resolves when worker shutdown begins.
    ///
    /// This is useful for tasks that need to cooperatively stop when
    /// [`Handle::shutdown`] is called.
    pub fn task_shutdown_signal(&self) -> tokio_util::sync::WaitForCancellationFutureOwned {
        self.task_shutdown_token.clone().cancelled_owned()
    }

    /// Spawn an async task that is owned by this worker handle.
    ///
    /// The task is tracked in the handle's internal join set, so it
    /// participates in the same shutdown lifecycle as other worker-associated
    /// tasks.
    ///
    /// Returns a task abort handle that can be used for early cancellation.
    pub fn spawn_associated_task<F>(&mut self, f: F) -> tokio::task::AbortHandle
    where
        F: Future<Output = ()>,
        F: Send + 'static,
    {
        self.tasks.spawn(f)
    }
}

impl ShutdownParams {
    async fn shutdown_child_process(
        &self,
        child: waymark_managed_process::Child,
    ) -> Result<std::process::ExitStatus, waymark_managed_process::ShutdownError> {
        child
            .shutdown(
                self.process_graceful_shutdown_timeout,
                self.process_kill_timeout,
            )
            .await
    }

    async fn shutdown_tasks(&self, mut tasks: &mut tokio::task::JoinSet<()>) {
        // Prepare a future to wait for all the tasks to join.
        let gracefully_terminate_tasks_fut = {
            let tasks = &mut tasks;
            async move {
                while let Some(join_result) = tasks.join_next().await {
                    match join_result {
                        Ok(()) => {}
                        Err(error) if error.is_cancelled() => {}
                        Err(error) => std::panic::resume_unwind(error.into_panic()),
                    }
                }
            }
        };

        // Attempt to wait for graceful task shutdown first.
        let task_shutdown_result = tokio::time::timeout(
            self.tasks_graceful_shutdown_timeout,
            gracefully_terminate_tasks_fut,
        )
        .await;

        // Abort the tasks forcefully if they didn't finish in time.
        if task_shutdown_result.is_err() {
            tracing::debug!(
                tasks_graceful_shutdown_timeout = ?self.tasks_graceful_shutdown_timeout,
                "worker tasks didn't shut down gracefully in time"
            );
            tasks.shutdown().await;
        }
    }
}
