//! The startup phase: the startup function run to its end, stopped at its
//! checkpoints on `stop_startup_token`.

/// The startup stopped at a checkpoint: the startup stop was requested
/// when the checkpoint was reached. The startup function returns it, as
/// its own error type, instead of going on; see [`Error`].
#[derive(Debug, thiserror::Error)]
#[error("startup stopped at a checkpoint")]
pub struct Stopped;

/// The startup function's error type: any error with an `Error` form,
/// as long as a [`Stopped`] converts into it and is told apart from a
/// failure afterwards.
///
/// Do not wrap a checkpoint's error with context: return it as it is.
/// Whether a wrapped [`Stopped`] is still told apart depends on the
/// `Error` form's implementation.
pub trait Error: From<Stopped> + waymark_into_error::IntoError {
    /// The [`Stopped`] this error is, if it is one.
    fn as_stopped(&self) -> Option<&Stopped>;
}

impl<StartupError> Error for StartupError
where
    StartupError: From<Stopped> + waymark_into_error::IntoError,
{
    fn as_stopped(&self) -> Option<&Stopped> {
        self.as_dyn_error().downcast_ref::<Stopped>()
    }
}

/// The startup function's checkpoint, where the startup stop request is
/// observed: [`Stopped`] once it was made, for the startup function to
/// return instead of going on.
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
/// with its checkpoints over `stop_startup_token`.
///
/// The future is never dropped, raced or timed out: whatever `startup_fn`
/// is in the middle of always finishes. The stop is observed only where
/// `startup_fn` asks for it, through its [`CheckpointFn`].
pub(crate) async fn run<TaskError, StartupOk, StartupError>(
    supervisor: &mut waymark_task_supervisor::Supervisor<TaskError>,
    stop_startup_token: &tokio_util::sync::CancellationToken,
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
