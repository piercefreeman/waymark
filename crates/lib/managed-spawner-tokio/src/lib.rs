//! [`Spawner`](waymark_managed_spawner::Spawner)s over tokio runtimes:
//! named, detached tasks.

#![warn(missing_docs)]

/// Spawns on the runtime current at the call.
///
/// A task's error is traced, then dropped: nothing else observes how a
/// task ends.
#[derive(Debug)]
pub struct CurrentRuntime;

impl<Ok> waymark_managed_spawner::Spawn<Ok> for CurrentRuntime
where
    Ok: waymark_managed_spawner::AllowedOkShape,
{
    /// # Panics
    ///
    /// Panics outside a tokio runtime, as [`tokio::spawn`] does, and when
    /// the builder reports a failure to spawn, which tokio's never does:
    /// its result is reserved surface.
    fn spawn<Fut, TaskOutput>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = TaskOutput> + Send + 'static,
        TaskOutput: waymark_managed_spawner::TaskOutput<Ok = Ok>,
        TaskOutput::Error: core::fmt::Debug,
    {
        let _detached = tokio::task::Builder::new()
            .name(name)
            .spawn(async move {
                if let Err(error) =
                    waymark_managed_spawner::TaskOutput::into_task_result(future.await)
                {
                    trace(name, error);
                }
            })
            .expect("tokio's task builder always spawns; its result is reserved surface");
    }
}

/// Spawns on the runtime behind a handle.
///
/// A task's error is traced, then dropped: nothing else observes how a
/// task ends.
#[derive(Debug)]
pub struct Handle(pub tokio::runtime::Handle);

impl<Ok> waymark_managed_spawner::Spawn<Ok> for Handle
where
    Ok: waymark_managed_spawner::AllowedOkShape,
{
    /// # Panics
    ///
    /// Panics when the builder reports a failure to spawn, which tokio's
    /// never does: its result is reserved surface.
    fn spawn<Fut, TaskOutput>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = TaskOutput> + Send + 'static,
        TaskOutput: waymark_managed_spawner::TaskOutput<Ok = Ok>,
        TaskOutput::Error: core::fmt::Debug,
    {
        let _detached = tokio::task::Builder::new()
            .name(name)
            .spawn_on(
                async move {
                    if let Err(error) =
                        waymark_managed_spawner::TaskOutput::into_task_result(future.await)
                    {
                        trace(name, error);
                    }
                },
                &self.0,
            )
            .expect("tokio's task builder always spawns; its result is reserved surface");
    }
}

/// Trace a task's error under its name.
fn trace(name: &'static str, error: impl std::fmt::Debug) {
    tracing::error!(name, ?error, "task ended with an error");
}

#[cfg(test)]
mod tests;
