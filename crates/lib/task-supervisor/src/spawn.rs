//! Spawning supervised tasks: every [`tokio::task::Builder`] variant,
//! each naming the task in tokio's eyes and handing it to
//! [`track`](crate::Supervisor::track).

impl<TaskError> crate::Supervisor<TaskError>
where
    TaskError: Send + 'static,
{
    /// Spawn `future` on the current runtime as a task named `name`, in
    /// tokio's eyes too, and supervise it ([`track`](Self::track)),
    /// converting its error into the supervisor's.
    ///
    /// The variants below mirror [`tokio::task::Builder`]: `_on` takes the
    /// runtime or local set to spawn on instead of the current one,
    /// `_blocking` runs a closure on the blocking pool, `_local` spawns a
    /// `!Send` future on the current local set.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, as [`tokio::spawn`] does.
    pub fn spawn<TaskFuture, Ok, IntoTaskError>(&mut self, name: &'static str, future: TaskFuture)
    where
        TaskFuture: Future<Output = Result<Ok, IntoTaskError>> + Send + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = tokio::task::Builder::new()
            .name(name)
            .spawn(async move { future.await.map_err(Into::into) })
            .expect(ASYNC_BUILDERS_ALWAYS_SPAWN);

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
        let task = tokio::task::Builder::new()
            .name(name)
            .spawn_on(async move { future.await.map_err(Into::into) }, handle)
            .expect(ASYNC_BUILDERS_ALWAYS_SPAWN);

        self.track(name, task);
    }

    /// [`spawn`](Self::spawn) for a blocking `function`, on the current
    /// runtime's blocking pool.
    ///
    /// The blocking pool can refuse to spawn the task, when it is shutting
    /// down or the OS refuses it a thread. Then nothing is supervised, and
    /// the error is returned to the caller.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, as
    /// [`tokio::task::spawn_blocking`] does.
    pub fn spawn_blocking<Function, Ok, IntoTaskError>(
        &mut self,
        name: &'static str,
        function: Function,
    ) -> Result<(), std::io::Error>
    where
        Function: FnOnce() -> Result<Ok, IntoTaskError> + Send + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = tokio::task::Builder::new()
            .name(name)
            .spawn_blocking(move || function().map_err(Into::into))?;

        self.track(name, task);

        Ok(())
    }

    /// [`spawn_blocking`](Self::spawn_blocking) on the runtime behind
    /// `handle`.
    pub fn spawn_blocking_on<Function, Ok, IntoTaskError>(
        &mut self,
        name: &'static str,
        function: Function,
        handle: &tokio::runtime::Handle,
    ) -> Result<(), std::io::Error>
    where
        Function: FnOnce() -> Result<Ok, IntoTaskError> + Send + 'static,
        Ok: Send + 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = tokio::task::Builder::new()
            .name(name)
            .spawn_blocking_on(move || function().map_err(Into::into), handle)?;

        self.track(name, task);

        Ok(())
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
        Ok: 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = tokio::task::Builder::new()
            .name(name)
            .spawn_local(async move { future.await.map_err(Into::into) })
            .expect(ASYNC_BUILDERS_ALWAYS_SPAWN);

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
        Ok: 'static,
        tokio::task::JoinHandle<Result<Ok, TaskError>>: Into<crate::TaskJoinHandle<TaskError>>,
        IntoTaskError: Into<TaskError>,
    {
        let task = tokio::task::Builder::new()
            .name(name)
            .spawn_local_on(async move { future.await.map_err(Into::into) }, local_set)
            .expect(ASYNC_BUILDERS_ALWAYS_SPAWN);

        self.track(name, task);
    }
}

/// Why the async spawns unwrap the builder's result: on this tokio it is
/// reserved surface, never an error, and the builder panics outside a
/// runtime the way [`tokio::spawn`] does. The blocking builders can
/// refuse, so the blocking spawns take their result themselves.
pub const ASYNC_BUILDERS_ALWAYS_SPAWN: &str =
    "tokio's async task builders always spawn; their result is reserved surface";

#[cfg(test)]
mod tests;
