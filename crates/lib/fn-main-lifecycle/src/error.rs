//! Why the lifecycle did not finish cleanly.

/// Why the lifecycle did not finish cleanly. `StartupError` is the
/// startup's error in its `Error` form.
#[derive(Debug, thiserror::Error)]
pub enum Error<StartupOk, StartupError, TaskError> {
    /// The startup failed. The shutdown was requested, by the failure
    /// unless an early task end had requested it already, and the tasks
    /// were drained; the report says how, with every task that ended
    /// before the request, one whose end may have caused the failure
    /// included. The message counts those, so what `main` prints points
    /// at the report's log.
    #[error("startup failed: {error}{}", early_ends_hint(.supervisor_report))]
    Startup {
        /// The startup's error.
        #[source]
        error: StartupError,

        /// The drain that followed.
        supervisor_report: waymark_task_supervisor::Report<TaskError>,
    },

    /// A task ended before the shutdown was requested, during a startup
    /// that then succeeded or stopped, or after it. That is the root cause
    /// over that completion; the report says which task.
    #[error("a task ended before the shutdown was requested: {supervisor_report}")]
    Task {
        /// The drain, with the early end in it.
        #[source]
        supervisor_report: waymark_task_supervisor::Report<TaskError>,

        /// How the startup function returned meanwhile.
        startup_completion: crate::startup::Completion<StartupOk>,
    },
}

/// The suffix of [`Error::Startup`]'s message: how many tasks the report
/// lists as ended before the shutdown was requested, or nothing.
fn early_ends_hint<TaskError>(report: &waymark_task_supervisor::Report<TaskError>) -> String {
    let early_ends = report
        .ended
        .iter()
        .filter(|ended| ended.before_shutdown)
        .count();
    match early_ends {
        0 => String::new(),
        1 => " (1 task ended before the shutdown was requested)".to_owned(),
        early_ends => format!(" ({early_ends} tasks ended before the shutdown was requested)"),
    }
}
