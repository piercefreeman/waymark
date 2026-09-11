//! The supervisor's report: every task's end.

/// What ended a task that did not return.
#[derive(Debug, thiserror::Error)]
pub enum Cause<TaskError> {
    /// The task ended with its own error.
    #[error(transparent)]
    Error(TaskError),

    /// The task did not get to end on its own: it panicked, and the join
    /// error carries the panic.
    #[error(transparent)]
    Join(tokio::task::JoinError),
}

/// One task's end, as recorded by the supervisor.
#[derive(Debug)]
pub struct End<TaskError> {
    /// The name the task was spawned under.
    pub name: &'static str,

    /// How the task ended: `Ok` when it returned, else what ended it.
    pub result: Result<(), Cause<TaskError>>,

    /// Whether the task ended before the shutdown token was cancelled.
    ///
    /// `true` is the early exit: the supervisor cancelled the token in
    /// response. `false` is an end after the shutdown was requested.
    pub before_shutdown: bool,
}

/// Every task's end, in the order the supervisor observed them.
#[derive(Debug)]
pub struct Report<TaskError> {
    /// The recorded ends.
    pub ended: Vec<End<TaskError>>,
}

impl<TaskError> Report<TaskError> {
    /// Whether any task ended before the shutdown was requested.
    pub fn any_before_shutdown(&self) -> bool {
        self.ended.iter().any(|ended| ended.before_shutdown)
    }

    /// The report as a result: `Err` when any task ended before the
    /// shutdown was requested, `Ok` otherwise. The report is the same
    /// either way, so the ends after the shutdown stay available for
    /// logging on the `Ok` path.
    pub fn into_result(self) -> Result<Self, Self> {
        if self.any_before_shutdown() {
            Err(self)
        } else {
            Ok(self)
        }
    }
}

impl<TaskError> std::fmt::Display for Report<TaskError>
where
    TaskError: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let early = self
            .ended
            .iter()
            .filter(|ended| ended.before_shutdown)
            .count();

        writeln!(
            f,
            "{} tasks ended, {} of them before shutdown was requested",
            self.ended.len(),
            early
        )?;

        for ended in &self.ended {
            let timing = if ended.before_shutdown {
                "before shutdown"
            } else {
                "after shutdown"
            };
            match &ended.result {
                Ok(()) => writeln!(f, "  {} ({}): returned", ended.name, timing)?,
                Err(cause) => writeln!(f, "  {} ({}): {}", ended.name, timing, cause)?,
            }
        }

        Ok(())
    }
}

impl<TaskError> std::error::Error for Report<TaskError> where TaskError: std::error::Error {}

#[cfg(test)]
mod tests;
