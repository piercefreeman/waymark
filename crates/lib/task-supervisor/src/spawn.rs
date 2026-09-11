//! Spawning supervised tasks: every [`tokio::task::Builder`] variant,
//! each naming the task in tokio's eyes and handing it to
//! [`track`](crate::Supervisor::track).

impl<TaskError> crate::Supervisor<TaskError>
where
    TaskError: Send + 'static,
{
    /// Spawn `future` on the current runtime as a task named `name`, in
    /// tokio's eyes too, and supervise it ([`track`](Self::track)),
    /// converting its error into the manager's.
    ///
    /// The variants below mirror [`tokio::task::Builder`]: `_on` takes the
    /// runtime or local set to spawn on instead of the current one,
    /// `_blocking` runs a closure on the blocking pool, `_local` spawns a
    /// `!Send` future on the current local set.
    pub fn spawn<TaskFuture, Ok, IntoTaskError>(&mut self, name: &'static str, future: TaskFuture)
    where
        TaskFuture: Future<Output = Result<Ok, IntoTaskError>> + Send + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = named(name, |builder| {
            builder.spawn(async move { future.await.map_err(Into::into) })
        });

        self.track(name, task);
    }

    /// [`spawn`](Self::spawn) on the runtime behind `handle`.
    pub fn spawn_on<TaskFuture, Ok, IntoTaskError>(
        &mut self,
        name: &'static str,
        future: TaskFuture,
        handle: &tokio::runtime::Handle,
    ) where
        TaskFuture: Future<Output = Result<Ok, IntoTaskError>> + Send + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = named(name, |builder| {
            builder.spawn_on(async move { future.await.map_err(Into::into) }, handle)
        });

        self.track(name, task);
    }

    /// [`spawn`](Self::spawn) for a blocking `function`, on the current
    /// runtime's blocking pool.
    pub fn spawn_blocking<Function, Ok, IntoTaskError>(
        &mut self,
        name: &'static str,
        function: Function,
    ) where
        Function: FnOnce() -> Result<Ok, IntoTaskError> + Send + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = named(name, |builder| {
            builder.spawn_blocking(move || function().map_err(Into::into))
        });

        self.track(name, task);
    }

    /// [`spawn_blocking`](Self::spawn_blocking) on the runtime behind
    /// `handle`.
    pub fn spawn_blocking_on<Function, Ok, IntoTaskError>(
        &mut self,
        name: &'static str,
        function: Function,
        handle: &tokio::runtime::Handle,
    ) where
        Function: FnOnce() -> Result<Ok, IntoTaskError> + Send + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = named(name, |builder| {
            builder.spawn_blocking_on(move || function().map_err(Into::into), handle)
        });

        self.track(name, task);
    }

    /// [`spawn`](Self::spawn) for a `!Send` `future`, on the current
    /// [`tokio::task::LocalSet`].
    ///
    /// # Panics
    ///
    /// Panics when called outside a local set, as
    /// [`tokio::task::spawn_local`] does.
    pub fn spawn_local<TaskFuture, Ok, IntoTaskError>(
        &mut self,
        name: &'static str,
        future: TaskFuture,
    ) where
        TaskFuture: Future<Output = Result<Ok, IntoTaskError>> + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = named(name, |builder| {
            builder.spawn_local(async move { future.await.map_err(Into::into) })
        });

        self.track(name, task);
    }

    /// [`spawn_local`](Self::spawn_local) on `local_set`.
    pub fn spawn_local_on<TaskFuture, Ok, IntoTaskError>(
        &mut self,
        name: &'static str,
        future: TaskFuture,
        local_set: &tokio::task::LocalSet,
    ) where
        TaskFuture: Future<Output = Result<Ok, IntoTaskError>> + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = named(name, |builder| {
            builder.spawn_local_on(async move { future.await.map_err(Into::into) }, local_set)
        });

        self.track(name, task);
    }
}

/// Spawn a task named `name` in tokio's eyes, for tokio-console and the
/// task metrics: `spawn_fn` gets the builder carrying the name and picks the
/// spawn variant.
///
/// The name is tokio's only under `tokio_unstable`, which this workspace
/// builds with. It is read at spawn time and not kept, hence any lifetime;
/// the `'static` the supervised spawns pass is the report's need, not
/// tokio's.
///
/// # Panics
///
/// Panics when the builder reported a failure to spawn. It never does on
/// this tokio: its result is reserved surface, and the builder panics
/// outside a runtime the way [`tokio::spawn`] does.
pub fn named<'name, Output, SpawnFn>(
    name: &'name str,
    spawn_fn: SpawnFn,
) -> tokio::task::JoinHandle<Output>
where
    SpawnFn:
        FnOnce(tokio::task::Builder<'name>) -> std::io::Result<tokio::task::JoinHandle<Output>>,
{
    spawn_fn(tokio::task::Builder::new().name(name))
        .expect("tokio's task builder always spawns; its result is reserved surface")
}

#[cfg(test)]
mod tests;
