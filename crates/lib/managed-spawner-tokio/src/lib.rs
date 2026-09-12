//! [`Spawner`](waymark_managed_spawner::Spawner)s over tokio runtimes:
//! named, detached tasks.

#![warn(missing_docs)]

/// A task's error as the spawners here take it: anything `Debug`, kept
/// only to be traced.
///
/// Deliberately not `Debug` itself: the blanket conversion from every
/// `Debug` error is coherent only because this type is not one.
pub struct TracedError(Box<dyn std::fmt::Debug + Send + 'static>);

impl<Error> From<Error> for TracedError
where
    Error: std::fmt::Debug + Send + 'static,
{
    fn from(error: Error) -> Self {
        Self(Box::new(error))
    }
}

/// Spawns on the runtime current at the call.
///
/// A task's error is traced, then dropped: nothing else observes how a
/// task ends.
#[derive(Debug)]
pub struct CurrentRuntime;

impl waymark_managed_spawner::SpawnsTasksWith for CurrentRuntime {
    type TaskError = TracedError;
}

impl<Ok> waymark_managed_spawner::Spawn<Ok> for CurrentRuntime
where
    Ok: waymark_managed_spawner::AllowedOkShape,
{
    /// # Panics
    ///
    /// Panics outside a tokio runtime, as [`tokio::spawn`] does, and when
    /// the builder reports a failure to spawn, which tokio's never does:
    /// its result is reserved surface.
    fn spawn<Fut, IntoError>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = Result<Ok, IntoError>> + Send + 'static,
        IntoError: Into<Self::TaskError>,
    {
        let _detached = tokio::task::Builder::new()
            .name(name)
            .spawn(async move {
                if let Err(error) = future.await {
                    trace(name, error.into());
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

impl waymark_managed_spawner::SpawnsTasksWith for Handle {
    type TaskError = TracedError;
}

impl<Ok> waymark_managed_spawner::Spawn<Ok> for Handle
where
    Ok: waymark_managed_spawner::AllowedOkShape,
{
    /// # Panics
    ///
    /// Panics when the builder reports a failure to spawn, which tokio's
    /// never does: its result is reserved surface.
    fn spawn<Fut, IntoError>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = Result<Ok, IntoError>> + Send + 'static,
        IntoError: Into<Self::TaskError>,
    {
        let _detached = tokio::task::Builder::new()
            .name(name)
            .spawn_on(
                async move {
                    if let Err(error) = future.await {
                        trace(name, error.into());
                    }
                },
                &self.0,
            )
            .expect("tokio's task builder always spawns; its result is reserved surface");
    }
}

/// Trace a task's error under its name.
fn trace(name: &'static str, TracedError(error): TracedError) {
    tracing::error!(name, ?error, "task ended with an error");
}

#[cfg(test)]
mod tests;
