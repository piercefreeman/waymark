//! The task supervisor as a managed spawner.

#![warn(missing_docs)]

pub use waymark_task_supervisor as supervisor;

/// Conversion of any task error into one unified error.
pub trait UnifyAnyError {
    /// The error every task error converts into.
    type UnifiedError;

    /// `error` as the unified error.
    fn from_any_error<Error>(error: Error) -> Self::UnifiedError
    where
        Error: core::error::Error + Send + Sync + 'static;
}

/// A task supervisor as a managed spawner: the tasks spawned through it
/// are supervised, their errors unified by `Converter`.
pub struct Spawner<'a, Converter>
where
    Converter: UnifyAnyError,
{
    /// The supervisor the tasks are spawned under.
    pub supervisor: &'a mut waymark_task_supervisor::Supervisor<Converter::UnifiedError>,

    /// The converter of the tasks' errors into the supervisor's.
    pub converter: Converter,
}

impl<Converter> Spawner<'_, Converter>
where
    Converter: UnifyAnyError,
{
    /// Spawn `future` as a task named `name`: [`Spawn::spawn`] for
    /// whichever shape `future` has.
    ///
    /// [`Spawn::spawn`]: waymark_managed_spawner::Spawn::spawn
    pub fn spawn<Fut, TaskOutput>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = TaskOutput> + Send + 'static,
        TaskOutput: waymark_managed_spawner::TaskOutput,
        TaskOutput::Error: core::error::Error + Send + Sync + 'static,
        Self: waymark_managed_spawner::Spawn<TaskOutput::Ok>,
    {
        waymark_managed_spawner::Spawn::spawn(self, name, future);
    }
}

/// A supervisor as a managed spawner.
pub trait SupervisorExt<TaskError> {
    /// This supervisor as a managed spawner, with `converter` unifying
    /// the tasks' errors into its own.
    fn spawner<Converter>(&mut self, converter: Converter) -> Spawner<'_, Converter>
    where
        Converter: UnifyAnyError<UnifiedError = TaskError>;
}

impl<TaskError> SupervisorExt<TaskError> for waymark_task_supervisor::Supervisor<TaskError> {
    fn spawner<Converter>(&mut self, converter: Converter) -> Spawner<'_, Converter>
    where
        Converter: UnifyAnyError<UnifiedError = TaskError>,
    {
        Spawner {
            supervisor: self,
            converter,
        }
    }
}

impl<Converter> waymark_managed_spawner::Spawn<()> for Spawner<'_, Converter>
where
    Converter: UnifyAnyError,
    Converter::UnifiedError: Send + 'static,
{
    fn spawn<Fut, TaskOutput>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = TaskOutput> + Send + 'static,
        TaskOutput: waymark_managed_spawner::TaskOutput<Ok = ()>,
        TaskOutput::Error: core::error::Error + Send + Sync + 'static,
    {
        self.supervisor.spawn(name, async move {
            waymark_managed_spawner::TaskOutput::into_task_result(future.await)
                .map_err(Converter::from_any_error)
        });
    }
}

impl<Converter> waymark_managed_spawner::Spawn<std::convert::Infallible> for Spawner<'_, Converter>
where
    Converter: UnifyAnyError,
    Converter::UnifiedError: Send + 'static,
{
    fn spawn<Fut, TaskOutput>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = TaskOutput> + Send + 'static,
        TaskOutput: waymark_managed_spawner::TaskOutput<Ok = std::convert::Infallible>,
        TaskOutput::Error: core::error::Error + Send + Sync + 'static,
    {
        self.supervisor.spawn(name, async move {
            waymark_managed_spawner::TaskOutput::into_task_result(future.await)
                .map_err(Converter::from_any_error)
        });
    }
}

#[cfg(test)]
mod tests;
