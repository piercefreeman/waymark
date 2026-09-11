//! A spawned task as the supervisor takes it.

/// A spawned task as the supervisor takes it: by the shape of its success
/// type, which is what a join handle fixes.
#[derive(Debug)]
pub enum TaskJoinHandle<TaskError> {
    /// A task whose success is `()`.
    Unit(tokio::task::JoinHandle<Result<(), TaskError>>),

    /// A task whose success is never: it ends only with an error.
    Infallible(tokio::task::JoinHandle<Result<std::convert::Infallible, TaskError>>),

    /// A task whose success is `()` and that has no error of its own.
    BareUnit(tokio::task::JoinHandle<()>),

    /// A task whose success is never and that has no error of its own: it
    /// cannot end on its own.
    BareInfallible(tokio::task::JoinHandle<std::convert::Infallible>),
}

impl<TaskError> From<tokio::task::JoinHandle<Result<(), TaskError>>> for TaskJoinHandle<TaskError> {
    fn from(task: tokio::task::JoinHandle<Result<(), TaskError>>) -> Self {
        Self::Unit(task)
    }
}

impl<TaskError> From<tokio::task::JoinHandle<Result<std::convert::Infallible, TaskError>>>
    for TaskJoinHandle<TaskError>
{
    fn from(task: tokio::task::JoinHandle<Result<std::convert::Infallible, TaskError>>) -> Self {
        Self::Infallible(task)
    }
}

impl<TaskError> From<tokio::task::JoinHandle<()>> for TaskJoinHandle<TaskError> {
    fn from(task: tokio::task::JoinHandle<()>) -> Self {
        Self::BareUnit(task)
    }
}

impl<TaskError> From<tokio::task::JoinHandle<std::convert::Infallible>>
    for TaskJoinHandle<TaskError>
{
    fn from(task: tokio::task::JoinHandle<std::convert::Infallible>) -> Self {
        Self::BareInfallible(task)
    }
}
