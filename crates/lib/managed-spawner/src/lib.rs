//! The managed spawner: named tasks, spawned through whatever the caller
//! hands in.

#![warn(missing_docs)]

/// Spawning a task whose success type is `Ok`.
///
/// Don't use in bounds, use [`Spawner`] instead.
pub trait Spawn<Ok>
where
    Ok: AllowedOkShape,
{
    /// Spawn `future` as a task named `name`.
    fn spawn<Fut, TaskOutput>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = TaskOutput> + Send + 'static,
        TaskOutput: crate::TaskOutput<Ok = Ok>,
        TaskOutput::Error: core::error::Error + Send + Sync + 'static;
}

/// Something that runs futures as named tasks: a [`Spawn`]er of both
/// shapes.
pub trait Spawner: Spawn<()> + Spawn<std::convert::Infallible> {}

impl<T> crate::Spawner for T where T: Spawn<()> + Spawn<std::convert::Infallible> {}

impl<Ok, Spawner> crate::Spawn<Ok> for &mut Spawner
where
    Ok: AllowedOkShape,
    Spawner: crate::Spawn<Ok>,
{
    fn spawn<Fut, TaskOutput>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = TaskOutput> + Send + 'static,
        TaskOutput: crate::TaskOutput<Ok = Ok>,
        TaskOutput::Error: core::error::Error + Send + Sync + 'static,
    {
        (**self).spawn(name, future);
    }
}

/// The output of a spawned future, as the result its task ends with: a
/// `Result` as it is, and `()` as a success with no error.
///
/// Sealed: `()`, `Result<(), Error>` and `Result<Infallible, Error>` are
/// the shapes.
pub trait TaskOutput: sealed::TaskOutput {
    /// The task's success type.
    type Ok: AllowedOkShape;

    /// The task's error.
    type Error;

    /// The result the task ends with.
    fn into_task_result(self) -> Result<Self::Ok, Self::Error>;
}

impl TaskOutput for () {
    type Ok = ();

    type Error = std::convert::Infallible;

    fn into_task_result(self) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

impl<Error> TaskOutput for Result<(), Error> {
    type Ok = ();

    type Error = Error;

    fn into_task_result(self) -> Self {
        self
    }
}

impl<Error> TaskOutput for Result<std::convert::Infallible, Error> {
    type Ok = std::convert::Infallible;

    type Error = Error;

    fn into_task_result(self) -> Self {
        self
    }
}

/// The success types a spawned future may have: `()`, or
/// [`Infallible`](std::convert::Infallible) for a task that never succeeds:
/// it ends only with an error.
///
/// Sealed: those two are the shapes.
pub trait AllowedOkShape: sealed::AllowedOkShape {}

impl AllowedOkShape for () {}

impl AllowedOkShape for std::convert::Infallible {}

mod sealed {
    pub trait AllowedOkShape {}

    impl AllowedOkShape for () {}

    impl AllowedOkShape for std::convert::Infallible {}

    pub trait TaskOutput {}

    impl TaskOutput for () {}

    impl<Error> TaskOutput for Result<(), Error> {}

    impl<Error> TaskOutput for Result<std::convert::Infallible, Error> {}
}
