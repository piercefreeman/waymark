//! Supervision of a process's lifetime tasks.
//!
//! # Contract
//!
//! - Tasks are awaited to completion and never aborted or abandoned. There
//!   is no timeout and no abort. The awaiting is done by the supervising
//!   task [`start`] runs, so it does not depend on the owner polling
//!   anything: a task that ends is observed the moment it does, before or
//!   after the owner reaches [`Supervisor::drain`]. Draining consumes the
//!   supervisor, which closes the intake, so the supervising task finishes
//!   once every task has ended; dropping the supervisor without draining
//!   keeps every task supervised to completion and loses only the
//!   [`Report`].
//! - Any bound a task wants on its own shutdown is the task's internals.
//! - The shutdown token is cancelled by the supervising task at the moment
//!   of an early end, and nowhere else: [`Supervisor::drain`] only
//!   collects the report, it never requests a shutdown. Requesting one for
//!   any other reason is the owner's job, through the same token.

#![warn(missing_docs)]

mod managed_spawner;
mod spawn;

pub mod report;
pub mod task_join_handle;

pub use self::report::Report;
pub use self::task_join_handle::TaskJoinHandle;

/// A task error with its type erased, for a manager supervising tasks of
/// different error types: every `std::error::Error + Send + Sync + 'static`
/// converts into it, so [`Supervisor::spawn`] takes them all.
pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A task under supervision, as handed to the supervising task.
#[derive(Debug)]
struct SupervisedTask<TaskError> {
    name: &'static str,

    task: TaskJoinHandle<TaskError>,

    shutdown_token: tokio_util::sync::CancellationToken,

    /// Cancelling when the task ends is always right: either the token is
    /// already cancelled and this is a no-op, or the task ended early and
    /// this is the reaction. Held as a guard from the moment of tracking,
    /// so it also fires if the supervising task is lost.
    cancel_on_end: tokio_util::sync::DropGuard,
}

/// The handle to a running supervisor of a process's lifetime tasks.
///
/// Obtained from [`start`]. See the crate docs for the contract.
#[derive(Debug)]
pub struct Supervisor<TaskError> {
    shutdown_token: tokio_util::sync::CancellationToken,

    /// Intake of the supervising task; closing it is what lets the
    /// supervising task finish once every task has ended.
    supervised_task_tx: tokio::sync::mpsc::UnboundedSender<SupervisedTask<TaskError>>,

    task: tokio::task::JoinHandle<Report<TaskError>>,
}

/// Start the supervising task on the current runtime and return the
/// handle to it.
///
/// `shutdown_token` is the token every task ends on and the one the
/// supervisor cancels when a task ends early.
pub fn start<TaskError>(
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Supervisor<TaskError>
where
    TaskError: std::fmt::Display + Send + 'static,
{
    let (supervised_task_tx, supervised_task_rx) = tokio::sync::mpsc::unbounded_channel();

    let task = spawn::named("task supervisor", |builder| {
        builder.spawn(run_supervisor(supervised_task_rx))
    });

    Supervisor {
        shutdown_token,
        supervised_task_tx,
        task,
    }
}

impl<TaskError> Supervisor<TaskError> {
    /// Supervise the already spawned `task` under `name`.
    ///
    /// The supervising task awaits it to completion and records the
    /// end. A panic in the task reaches the supervising task as a join
    /// error the moment it happens, so it is recorded under the task's
    /// name and, before the shutdown, cancels the token as promptly as any
    /// other early end.
    pub fn track<IntoTaskJoinHandle>(&mut self, name: &'static str, task: IntoTaskJoinHandle)
    where
        IntoTaskJoinHandle: Into<TaskJoinHandle<TaskError>>,
    {
        let supervised_task = SupervisedTask {
            name,
            task: task.into(),
            shutdown_token: self.shutdown_token.clone(),
            cancel_on_end: self.shutdown_token.clone().drop_guard(),
        };

        if self.supervised_task_tx.send(supervised_task).is_err() {
            // The supervising task is gone, which only a panic in it can
            // cause. The task runs on unsupervised; nothing else to do here.
            // (The returned `SupervisedTask` drops, and its guard requests
            // the shutdown.)
            tracing::error!(name, "task supervisor is gone; task runs unsupervised");
        }
    }

    /// Close the intake, await every task to completion and report how
    /// each ended.
    ///
    /// Consuming the supervisor closes the intake, so this completes once
    /// every task has ended. The report carries every end; whether any
    /// came before the shutdown was requested is
    /// [`Report::any_before_shutdown`], the owner's to act on.
    pub async fn drain(self) -> Report<TaskError> {
        let Self {
            shutdown_token,
            supervised_task_tx,
            task,
        } = self;

        drop(supervised_task_tx);

        match task.await {
            Ok(report) => report,
            Err(join_error) => {
                // The supervising task panicked. It does nothing that can
                // panic, so this is not expected to happen; recorded rather
                // than hidden. The ends it had collected are lost with it.
                // Recorded only: requesting the shutdown is the supervising
                // task's reaction at the moment of an end, never this
                // observer's.
                let before_shutdown = !shutdown_token.is_cancelled();

                tracing::error!(error = %join_error, before_shutdown, "task supervisor ended");

                Report {
                    ended: vec![report::End {
                        name: "task supervisor",
                        result: Err(report::Cause::Join(join_error)),
                        before_shutdown,
                    }],
                }
            }
        }
    }
}

/// Supervise one task: await it to its end and record the end.
async fn supervise<TaskError>(supervised_task: SupervisedTask<TaskError>) -> report::End<TaskError>
where
    TaskError: std::fmt::Display,
{
    let SupervisedTask {
        name,
        task,
        shutdown_token,
        cancel_on_end,
    } = supervised_task;

    let result = match task {
        TaskJoinHandle::Unit(task) => task.await,
        TaskJoinHandle::Infallible(task) => {
            task.await.map(|result| result.map(|never| match never {}))
        }
        TaskJoinHandle::BareUnit(task) => task.await.map(Ok),
        TaskJoinHandle::BareInfallible(task) => task.await.map(|never| match never {}),
    };

    let result = match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(report::Cause::Error(error)),
        Err(join_error) => Err(report::Cause::Join(join_error)),
    };

    let before_shutdown = !shutdown_token.is_cancelled();

    match (&result, before_shutdown) {
        (Ok(()), true) => tracing::error!(
            name,
            "task returned before shutdown was requested; requesting shutdown"
        ),
        (Err(cause), true) => tracing::error!(
            name,
            %cause,
            "task ended before shutdown was requested; requesting shutdown"
        ),
        (Ok(()), false) => tracing::debug!(name, "task returned"),
        (Err(cause), false) => tracing::debug!(name, %cause, "task ended"),
    }

    drop(cancel_on_end);

    report::End {
        name,
        result,
        before_shutdown,
    }
}

/// The supervising task: takes in supervised tasks for as long as the
/// intake is open, supervises each, collects the ends, and finishes once
/// the intake is closed and every end is in.
async fn run_supervisor<TaskError>(
    mut supervised_task_rx: tokio::sync::mpsc::UnboundedReceiver<SupervisedTask<TaskError>>,
) -> Report<TaskError>
where
    TaskError: std::fmt::Display,
{
    use futures_util::StreamExt as _;

    let mut ends = futures_util::stream::FuturesUnordered::new();
    let mut ended = Vec::new();
    let mut intake_open = true;

    loop {
        tokio::select! {
            received = supervised_task_rx.recv(), if intake_open => match received {
                Some(supervised_task) => ends.push(supervise(supervised_task)),
                None => intake_open = false,
            },
            // An empty set yields `None` at once, so it is polled only
            // while it holds something; with the intake closed as well,
            // there is nothing left to wait for.
            Some(end) = ends.next(), if !ends.is_empty() => ended.push(end),
            else => break,
        }
    }

    Report { ended }
}

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;
