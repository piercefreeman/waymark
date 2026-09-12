//! The supervisor as a managed spawner.

impl<TaskError> waymark_managed_spawner::SpawnsTasksWith for crate::Supervisor<TaskError> {
    type TaskError = TaskError;
}

impl<TaskError> waymark_managed_spawner::Spawn<()> for crate::Supervisor<TaskError>
where
    TaskError: Send + 'static,
{
    fn spawn<Fut, IntoError>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = Result<(), IntoError>> + Send + 'static,
        IntoError: Into<Self::TaskError>,
    {
        crate::Supervisor::spawn(self, name, future);
    }
}

impl<TaskError> waymark_managed_spawner::Spawn<std::convert::Infallible>
    for crate::Supervisor<TaskError>
where
    TaskError: Send + 'static,
{
    fn spawn<Fut, IntoError>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = Result<std::convert::Infallible, IntoError>> + Send + 'static,
        IntoError: Into<Self::TaskError>,
    {
        crate::Supervisor::spawn(self, name, future);
    }
}

#[cfg(test)]
mod tests;
