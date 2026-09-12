use super::*;

#[derive(Debug, thiserror::Error)]
#[error("shutdown observed")]
struct ShutdownError;

/// The tests' unified task error: the task error's message.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct TaskError(String);

struct TaskErrorConverter;

impl UnifyAnyError for TaskErrorConverter {
    type UnifiedError = TaskError;

    fn from_any_error<Error>(error: Error) -> TaskError
    where
        Error: core::error::Error + Send + Sync + 'static,
    {
        TaskError(error.to_string())
    }
}

async fn until_shutdown(
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<std::convert::Infallible, ShutdownError> {
    shutdown_token.cancelled().await;
    Err(ShutdownError)
}

fn spawn_infallible_through_the_spawner<Spawner>(
    mut spawner: Spawner,
    shutdown_token: tokio_util::sync::CancellationToken,
) where
    Spawner: waymark_managed_spawner::Spawner,
{
    spawner.spawn("loop", until_shutdown(shutdown_token));
}

fn spawn_unit_through_the_spawner<Spawner>(mut spawner: Spawner)
where
    Spawner: waymark_managed_spawner::Spawner,
{
    spawner.spawn("early", async { Ok::<(), ShutdownError>(()) });
}

fn spawn_unit_output_through_the_spawner<Spawner>(mut spawner: Spawner)
where
    Spawner: waymark_managed_spawner::Spawner,
{
    spawner.spawn("early", async {});
}

#[tokio::test]
async fn infallible_task_through_the_spawner_ends_after_shutdown() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = waymark_task_supervisor::start::<TaskError>(shutdown_token.clone());

    spawn_infallible_through_the_spawner(
        supervisor.spawner(TaskErrorConverter),
        shutdown_token.clone(),
    );

    shutdown_token.cancel();

    let report = supervisor.drain().await;
    assert!(!report.any_before_shutdown());
    assert_eq!(report.ended[0].name, "loop");
    assert!(matches!(
        report.ended[0].result,
        Err(waymark_task_supervisor::report::Cause::Error(TaskError(ref message))) if message == "shutdown observed"
    ));
}

#[tokio::test]
async fn unit_task_through_the_spawner_ends_ok() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = waymark_task_supervisor::start::<TaskError>(shutdown_token.clone());

    spawn_unit_through_the_spawner(supervisor.spawner(TaskErrorConverter));

    let report = supervisor.drain().await;
    assert!(report.ended[0].before_shutdown);
    assert!(report.ended[0].result.is_ok());
}

#[tokio::test]
async fn unit_output_task_through_the_spawner_ends_ok() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = waymark_task_supervisor::start::<TaskError>(shutdown_token.clone());

    spawn_unit_output_through_the_spawner(supervisor.spawner(TaskErrorConverter));

    let report = supervisor.drain().await;
    assert!(report.ended[0].before_shutdown);
    assert!(report.ended[0].result.is_ok());
}
