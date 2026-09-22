use super::*;

/// The tests' task error and startup error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Message(&'static str);

/// The startup error the tests run with: its own error, as any crate's
/// would be, with the `Error` form being itself.
#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error(transparent)]
    Stopped(#[from] Stopped),

    #[error(transparent)]
    Message(#[from] Message),
}

impl waymark_into_error::IntoError for StartupError {
    type Error = Self;

    fn as_dyn_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        match self {
            Self::Stopped(stopped) => stopped,
            Self::Message(message) => message,
        }
    }

    fn into_error(self) -> Self {
        self
    }
}

fn setup() -> (
    waymark_task_supervisor::Supervisor<Message>,
    tokio_util::sync::CancellationToken,
) {
    let stop_startup_token = tokio_util::sync::CancellationToken::new();
    let supervisor = waymark_task_supervisor::start(stop_startup_token.child_token());
    (supervisor, stop_startup_token)
}

#[tokio::test]
async fn a_startup_that_returns_succeeds() {
    let (mut supervisor, stop_startup_token) = setup();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
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
    let (mut supervisor, stop_startup_token) = setup();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
        async |_supervisor, checkpoint| {
            checkpoint()?;
            stop_startup_token.cancel();
            checkpoint()?;
            panic!("the next step must not start");
            #[allow(unreachable_code)]
            Ok::<(), StartupError>(())
        },
    )
    .await;

    assert!(matches!(outcome, Outcome::Stopped), "{outcome:?}");
}

#[tokio::test]
async fn a_startup_that_errors_fails() {
    let (mut supervisor, stop_startup_token) = setup();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
        async |_supervisor, checkpoint| {
            checkpoint()?;
            Err::<(), StartupError>(Message("step failed").into())
        },
    )
    .await;

    let Outcome::Failed(error) = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(error.to_string(), "step failed");
}

#[tokio::test]
async fn a_startup_without_checkpoints_runs_to_its_end() {
    let (mut supervisor, stop_startup_token) = setup();
    stop_startup_token.cancel();

    let outcome = run(
        &mut supervisor,
        &stop_startup_token,
        async |_supervisor, _checkpoint| Ok::<_, StartupError>(7),
    )
    .await;

    assert!(matches!(outcome, Outcome::Success(7)), "{outcome:?}");
}
