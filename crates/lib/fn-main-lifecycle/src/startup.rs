//! The startup phase: the startup function run to its end, stopped at its
//! checkpoints on `stop_startup_token`.

/// The startup stopped at a checkpoint: the startup stop was requested
/// when the checkpoint was reached. The startup function returns it, as
/// its own error type, instead of going on; see [`Error`].
#[derive(Debug, thiserror::Error)]
#[error("startup stopped at a checkpoint")]
pub struct Stopped;

/// The startup function's error type: a [`Stopped`] converts into it,
/// and it tells a [`Stopped`] apart from a failure afterwards.
///
/// Do not wrap a checkpoint's error with context: return it as it is.
/// Whether a wrapped [`Stopped`] is still told apart is up to the
/// implementation.
pub trait Error: From<Stopped> {
    /// The [`Stopped`] this error is, if it is one.
    fn as_stopped(&self) -> Option<&Stopped>;
}

/// An error seen as some error tells a [`Stopped`] apart by looking for
/// it in what it is seen as: a report holding one, for instance. An error
/// that holds a [`Stopped`] as one of its variants is not seen as some
/// error; it implements [`Error`] itself.
impl<StartupError> Error for StartupError
where
    StartupError: From<Stopped>,
    StartupError: waymark_error_coercion::AsDynError,
{
    fn as_stopped(&self) -> Option<&Stopped> {
        self.as_dyn_error().downcast_ref::<Stopped>()
    }
}

/// The startup function's checkpoint, where the startup stop request is
/// observed: [`Stopped`] once it was made, for the startup function to
/// return instead of going on. The shutdown is requested right there,
/// before the startup function returns and drops what it holds.
pub type CheckpointFn<'a> = &'a dyn Fn() -> Result<(), Stopped>;

/// How the startup ended.
#[derive(Debug)]
pub enum Outcome<StartupOk, StartupError> {
    /// The startup function returned its value.
    Success(StartupOk),

    /// The startup function stopped at a checkpoint: `stop_startup_token`
    /// was cancelled.
    Stopped,

    /// The startup function returned an error (i.e. one of its steps
    /// failed).
    Failed(StartupError),
}

/// Run the startup function `startup_fn` under `supervisor` to its end,
/// with its checkpoints over `stop_startup_token`; the checkpoint that
/// stops the startup requests the shutdown by cancelling `shutdown_token`.
///
/// The future is never dropped, raced or timed out: whatever `startup_fn`
/// is in the middle of always finishes. The stop is observed only where
/// `startup_fn` asks for it, through its [`CheckpointFn`].
pub(crate) async fn run<TaskError, StartupOk, StartupError>(
    supervisor: &mut waymark_task_supervisor::Supervisor<TaskError>,
    stop_startup_token: &tokio_util::sync::CancellationToken,
    shutdown_token: &tokio_util::sync::CancellationToken,
    startup_fn: impl AsyncFnOnce(
        &mut waymark_task_supervisor::Supervisor<TaskError>,
        CheckpointFn<'_>,
    ) -> Result<StartupOk, StartupError>,
) -> Outcome<StartupOk, StartupError>
where
    StartupError: Error,
{
    let checkpoint = || {
        if stop_startup_token.is_cancelled() {
            // Requested here, while `startup_fn` still holds everything
            // it set up: a task that ends once `startup_fn` drops its
            // handle then ends after the request, never before it.
            shutdown_token.cancel();

            return Err(Stopped);
        }

        Ok(())
    };
    match startup_fn(supervisor, &checkpoint).await {
        Ok(ok) => Outcome::Success(ok),
        Err(error) if error.as_stopped().is_some() => Outcome::Stopped,
        Err(error) => Outcome::Failed(error),
    }
}

#[cfg(test)]
mod tests;
