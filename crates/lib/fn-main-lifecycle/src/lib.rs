//! The body of a binary's `main`: the startup under its task supervisor,
//! then the drain and the verdict.
//!
//! # Contract
//!
//! - `startup_fn`'s future is run to its end. It is never dropped, raced
//!   or timed out. It stops early only at the checkpoints it places
//!   itself.
//!
//! - Cancelling the stop-startup-then-shutdown token during the startup
//!   stops the startup at its next checkpoint, or lets it reach its end
//!   when no checkpoint comes first, and then requests the shutdown.
//!   Cancelling it once the startup has ended requests the shutdown right
//!   away. Until the startup has ended it reaches nothing but the
//!   checkpoints, so whatever `startup_fn` is in the middle of is
//!   undisturbed by it. That is what the two tokens are for: what
//!   `startup_fn` is in the middle of tends to depend on tasks the
//!   shutdown would end, and would then fail through no fault of its own.
//!
//! - Cancelling the shutdown token ends the tasks. The checkpoints do not
//!   observe it, so `startup_fn` runs to its end regardless.
//!
//! - An error returned by `startup_fn` fails the startup: the shutdown
//!   token is cancelled, and the error is the result. A cancelled
//!   stop-startup-then-shutdown token changes nothing about that: it
//!   reaches `startup_fn` at its checkpoints only, so the error came from
//!   `startup_fn`, not from the token.
//!
//! - Whatever the startup did, every task is drained afterwards, and a task
//!   that ended before the shutdown was requested is the result over
//!   anything the startup returned: it is the root cause of what the
//!   startup saw.

#![warn(missing_docs)]

mod cancellation_utils;
pub mod error;
pub mod startup;

pub use waymark_task_supervisor as supervisor;

pub use self::error::Error;

use self::cancellation_utils::*;

/// The lifecycle's end when nothing failed: the shutdown drained
/// with no task ending early. When the shutdown came is the variant.
#[derive(Debug)]
pub enum CleanShutdown<StartupOk> {
    /// After the full lifecycle: the startup ran to its end, and the
    /// process ran on from it until the shutdown.
    AfterFullLifecycle(StartupOk),

    /// While the startup ran: the stop-startup-then-shutdown token was
    /// cancelled, and the startup stopped at its next checkpoint.
    DuringStartup,
}

impl<StartupOk> std::process::Termination for CleanShutdown<StartupOk>
where
    StartupOk: std::process::Termination,
{
    /// A shutdown during the startup exits with success; one after the
    /// full lifecycle exits with what the startup's value reports.
    fn report(self) -> std::process::ExitCode {
        match self {
            CleanShutdown::AfterFullLifecycle(ok) => ok.report(),
            CleanShutdown::DuringStartup => std::process::ExitCode::SUCCESS,
        }
    }
}

/// What [`run`] runs the lifecycle with.
#[derive(Debug)]
pub struct Params<TaskError> {
    /// The supervisor the startup's tasks go under, and the one drained at
    /// the end; started on `shutdown_token`.
    pub supervisor: waymark_task_supervisor::Supervisor<TaskError>,

    /// The token that makes `startup_fn` stop early, at its explicit
    /// checkpoints rather than by future cancellation, and then triggers
    /// the shutdown by cancelling `shutdown_token`.
    ///
    /// After the startup has ended, cancelling it cancels `shutdown_token`
    /// right away.
    pub stop_startup_then_shutdown_token: tokio_util::sync::CancellationToken,

    /// The token the startup's tasks end on; the supervisor runs on it.
    /// The lifecycle cancels it after the stop-startup-then-shutdown
    /// token, and when the startup fails.
    pub shutdown_token: tokio_util::sync::CancellationToken,
}

/// Run the startup `startup_fn` under the supervisor, then drain the
/// supervisor. See the crate docs for the contract.
pub async fn run<TaskError, StartupOk, StartupError>(
    params: Params<TaskError>,
    startup_fn: impl AsyncFnOnce(
        &mut waymark_task_supervisor::Supervisor<TaskError>,
        startup::CheckpointFn<'_>,
    ) -> Result<StartupOk, StartupError>,
) -> Result<CleanShutdown<StartupOk>, Error<StartupOk, StartupError::Error, TaskError>>
where
    TaskError: std::fmt::Display,
    StartupError: startup::Error + std::fmt::Display,
{
    let Params {
        mut supervisor,
        stop_startup_then_shutdown_token,
        shutdown_token,
    } = params;

    let startup_outcome = {
        // The shutdown follows every end of the startup but the
        // successful one: the guard fires at the end of this block unless
        // it is disarmed.
        let non_success_startup_guard = shutdown_token.clone().drop_guard();

        let startup_outcome = startup::run(
            &mut supervisor,
            &stop_startup_then_shutdown_token,
            startup_fn,
        )
        .await;

        match &startup_outcome {
            startup::Outcome::Success(_) => {
                let _ = non_success_startup_guard.disarm();
            }
            startup::Outcome::Stopped => {
                tracing::info!("startup stopped at a checkpoint; shutting down");
            }
            startup::Outcome::Failed(error) => {
                tracing::error!(%error, "startup failed; shutting down");
            }
        }
        startup_outcome
    };

    // The startup is over: from here on, cancelling the
    // stop-startup-then-shutdown token cancels the shutdown token.
    let propagate_startup_stop_to_shutdown =
        cancel(&shutdown_token).after(stop_startup_then_shutdown_token.cancelled());
    let (report, ()) = futures_util::join!(supervisor.drain(), propagate_startup_stop_to_shutdown);

    if report.any_before_shutdown() {
        tracing::error!(%report, "shutdown complete");
        let startup_outcome = match startup_outcome {
            startup::Outcome::Success(ok) => startup::Outcome::Success(ok),
            startup::Outcome::Stopped => startup::Outcome::Stopped,
            startup::Outcome::Failed(error) => startup::Outcome::Failed(error.into_error()),
        };
        return Err(Error::Task {
            supervisor_report: report,
            startup_outcome,
        });
    }
    tracing::info!(%report, "shutdown complete");

    match startup_outcome {
        startup::Outcome::Success(ok) => Ok(CleanShutdown::AfterFullLifecycle(ok)),
        startup::Outcome::Stopped => Ok(CleanShutdown::DuringStartup),
        startup::Outcome::Failed(error) => Err(Error::Startup {
            error: error.into_error(),
            supervisor_report: report,
        }),
    }
}

#[cfg(test)]
mod tests;
