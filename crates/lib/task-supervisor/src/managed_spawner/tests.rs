use crate::test_helpers::{TaskError, until_shutdown};
use crate::{BoxedError, start};

fn spawn_infallible_through_the_seam<Spawner>(
    mut spawner: Spawner,
    shutdown_token: tokio_util::sync::CancellationToken,
) where
    Spawner: waymark_managed_spawner::Spawn<std::convert::Infallible>,
    TaskError: Into<Spawner::TaskError>,
{
    spawner.spawn("loop", until_shutdown(shutdown_token));
}

fn spawn_unit_through_the_seam<Spawner>(mut spawner: Spawner)
where
    Spawner: waymark_managed_spawner::Spawn<()>,
    TaskError: Into<Spawner::TaskError>,
{
    spawner.spawn("early", async { Ok::<(), TaskError>(()) });
}

#[tokio::test]
async fn infallible_task_through_the_seam_ends_after_shutdown() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<BoxedError>(shutdown_token.clone());

    spawn_infallible_through_the_seam(&mut supervisor, shutdown_token.clone());

    shutdown_token.cancel();

    let report = supervisor.drain().await;
    assert!(!report.any_before_shutdown());
    assert_eq!(report.ended[0].name, "loop");
}

#[tokio::test]
async fn unit_task_through_the_seam_ends_ok() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<BoxedError>(shutdown_token.clone());

    spawn_unit_through_the_seam(&mut supervisor);

    let report = supervisor.drain().await;
    assert!(report.ended[0].before_shutdown);
    assert!(report.ended[0].result.is_ok());
}
