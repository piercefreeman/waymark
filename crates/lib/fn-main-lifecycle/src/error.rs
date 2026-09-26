//! Why the lifecycle did not end cleanly.

/// Why the lifecycle did not end cleanly. `StartupError` is the startup's
/// error in its `Error` form.
#[derive(Debug, thiserror::Error)]
pub enum Error<StartupOk, StartupError, TaskError> {
    /// The startup failed. The tasks were shut down and drained in
    /// response; the report says how.
    #[error("startup failed")]
    Startup {
        /// The startup's error.
        #[source]
        error: StartupError,

        /// The drain that followed.
        supervisor_report: waymark_task_supervisor::Report<TaskError>,
    },

    /// A task ended before the shutdown was requested, during the startup
    /// or after it. That is the root cause over whatever the startup did
    /// meanwhile; the report names the task.
    #[error("a task ended before the shutdown was requested")]
    Task {
        /// The drain, with the early end in it.
        #[source]
        supervisor_report: waymark_task_supervisor::Report<TaskError>,

        /// How the startup ended meanwhile.
        startup_outcome: crate::startup::Outcome<StartupOk, StartupError>,
    },
}
