use super::*;
use crate::test_helpers::{MessageError, StartupError};

/// A supervisor over a fresh shutdown token, that token, and the stop
/// token the checkpoints observe.
fn setup() -> (
    waymark_task_supervisor::Supervisor<MessageError>,
    tokio_util::sync::CancellationToken,
    tokio_util::sync::CancellationToken,
) {
    let stop_startup_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let supervisor = waymark_task_supervisor::start(shutdown_token.clone());
    (supervisor, stop_startup_token, shutdown_token)
}

#[test]
fn a_stopped_is_told_apart_from_a_failure() {
    let stopped = StartupError::from(StoppedError(()));
    assert!(stopped.as_stopped().is_some());

    let failure = StartupError::from(MessageError("step failed"));
    assert!(failure.as_stopped().is_none());
}

#[tokio::test]
async fn a_startup_that_returns_succeeds() {
    let (mut supervisor, stop_startup_token, shutdown_token) = setup();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
        &shutdown_token,
        async |_supervisor, checkpoint| {
            checkpoint()?;
            Ok::<_, StartupError>(5)
        },
    )
    .await;

    assert!(matches!(outcome, Outcome::Success(5)), "{outcome:?}");
}

#[tokio::test]
async fn a_startup_stops_at_the_checkpoint_after_the_stop() {
    let (mut supervisor, stop_startup_token, shutdown_token) = setup();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
        &shutdown_token,
        async |_supervisor, checkpoint| -> Result<(), StartupError> {
            checkpoint()?;
            stop_startup_token.cancel();
            checkpoint()?;
            panic!("the next step must not start")
        },
    )
    .await;

    assert!(matches!(outcome, Outcome::Stopped), "{outcome:?}");
}

#[tokio::test]
async fn the_checkpoint_that_stops_requests_the_shutdown_before_returning() {
    let (mut supervisor, stop_startup_token, shutdown_token) = setup();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
        &shutdown_token,
        async |_supervisor, checkpoint| -> Result<(), StartupError> {
            checkpoint()?;
            assert!(!shutdown_token.is_cancelled());

            stop_startup_token.cancel();

            let stopped = checkpoint();
            assert!(shutdown_token.is_cancelled());

            stopped?;
            panic!("the next step must not start")
        },
    )
    .await;

    assert!(matches!(outcome, Outcome::Stopped), "{outcome:?}");
}

#[tokio::test]
async fn a_startup_that_errors_fails() {
    let (mut supervisor, stop_startup_token, shutdown_token) = setup();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
        &shutdown_token,
        async |_supervisor, checkpoint| {
            checkpoint()?;
            Err::<(), StartupError>(MessageError("step failed").into())
        },
    )
    .await;

    let Outcome::Failed(error) = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(error.to_string(), "step failed");
}

#[tokio::test]
async fn a_startup_without_checkpoints_returns() {
    let (mut supervisor, stop_startup_token, shutdown_token) = setup();
    stop_startup_token.cancel();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
        &shutdown_token,
        async |_supervisor, _checkpoint| Ok::<_, StartupError>(7),
    )
    .await;

    assert!(matches!(outcome, Outcome::Success(7)), "{outcome:?}");
}
