//! The seam between what spawns tasks and what asks for them to be
//! spawned.

#![warn(missing_docs)]

/// What a spawner spawns its tasks with: the error their own errors are
/// converted into.
pub trait SpawnsTasksWith {
    /// That error.
    type TaskError;
}

/// Spawning a task whose success type is `Ok`.
pub trait Spawn<Ok>: SpawnsTasksWith
where
    Ok: AllowedOkShape,
{
    /// Spawn `future` as a task named `name`.
    fn spawn<Fut, IntoError>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = Result<Ok, IntoError>> + Send + 'static,
        IntoError: Into<Self::TaskError>;
}

/// Something that runs futures as named tasks: a [`Spawn`]er of both
/// shapes.
pub trait Spawner: Spawn<()> + Spawn<std::convert::Infallible> {}

impl<T> crate::Spawner for T where T: Spawn<()> + Spawn<std::convert::Infallible> {}

impl<SpawnsTasksWith> crate::SpawnsTasksWith for &mut SpawnsTasksWith
where
    SpawnsTasksWith: crate::SpawnsTasksWith,
{
    type TaskError = SpawnsTasksWith::TaskError;
}

impl<Ok, Spawner> crate::Spawn<Ok> for &mut Spawner
where
    Ok: AllowedOkShape,
    Spawner: crate::Spawn<Ok>,
{
    fn spawn<Fut, IntoError>(&mut self, name: &'static str, future: Fut)
    where
        Fut: Future<Output = Result<Ok, IntoError>> + Send + 'static,
        IntoError: Into<Self::TaskError>,
    {
        (**self).spawn(name, future);
    }
}

/// The success types a spawned future may have: `()`, or
/// [`Infallible`](std::convert::Infallible) for a task that never succeeds:
/// it ends only with an error.
///
/// Sealed: those two are the shapes.
pub trait AllowedOkShape: sealed::Sealed {}

impl AllowedOkShape for () {}

impl AllowedOkShape for std::convert::Infallible {}

mod sealed {
    pub trait Sealed {}

    impl Sealed for () {}

    impl Sealed for std::convert::Infallible {}
}
