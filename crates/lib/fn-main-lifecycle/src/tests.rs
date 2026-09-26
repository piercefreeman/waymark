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
    Stopped(#[from] startup::Stopped),

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

#[derive(Debug, thiserror::Error)]
#[error("shutdown observed")]
struct Shutdown;

impl From<Shutdown> for Message {
    fn from(_: Shutdown) -> Self {
        Message("shutdown observed")
    }
}

async fn until_shutdown(
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), Shutdown> {
    shutdown_token.cancelled().await;
    Err(Shutdown)
}

/// The params over a fresh stop token, and that token: cancelling it
/// stops the startup, and the shutdown follows.
fn setup() -> (Params<Message>, tokio_util::sync::CancellationToken) {
    let stop_startup_then_shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let params = Params {
        supervisor: waymark_task_supervisor::start(shutdown_token.clone()),
        stop_startup_then_shutdown_token: stop_startup_then_shutdown_token.clone(),
        shutdown_token,
    };
    (params, stop_startup_then_shutdown_token)
}

#[tokio::test]
async fn completes_and_drains() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        checkpoint()?;
        // The work is done: cancel the stop-startup-then-shutdown token,
        // then end.
        stop_startup_then_shutdown_token.cancel();
        Ok::<_, StartupError>(5)
    })
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::AfterFullLifecycle(5))),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_failed_startup_requests_the_shutdown_and_is_the_result() {
    let (params, _stop_startup_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run(params, async |supervisor, _checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        Err::<(), StartupError>(Message("step failed").into())
    })
    .await;

    assert!(shutdown_token.is_cancelled());
    let Err(Error::Startup {
        error,
        supervisor_report,
    }) = outcome
    else {
        panic!("{outcome:?}");
    };
    assert_eq!(error.to_string(), "step failed");
    assert_eq!(supervisor_report.ended.len(), 1);
    assert!(!supervisor_report.any_before_shutdown());
}

#[tokio::test]
async fn a_checkpoint_after_the_cancel_stops_the_startup() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        checkpoint()?;
        // The stop-startup-then-shutdown token is cancelled between two
        // steps.
        stop_startup_then_shutdown_token.cancel();
        checkpoint()?;
        panic!("the next step must not start");
        #[allow(unreachable_code)]
        Ok::<(), StartupError>(())
    })
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::DuringStartup)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn an_early_task_end_is_the_result_over_the_startup_end() {
    let (params, _stop_startup_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run(params, async |supervisor, checkpoint| {
        // Ends at once, before any shutdown was requested.
        supervisor.spawn("short", async { Ok::<(), Shutdown>(()) });
        shutdown_token.cancelled().await;
        checkpoint()?;
        Ok::<(), StartupError>(())
    })
    .await;

    let Err(Error::Task {
        supervisor_report,
        startup_outcome,
    }) = outcome
    else {
        panic!("{outcome:?}");
    };
    assert!(supervisor_report.any_before_shutdown());
    assert_eq!(supervisor_report.ended[0].name, "short");
    assert!(matches!(startup_outcome, startup::Outcome::Success(())));
}

#[tokio::test]
async fn an_early_task_end_keeps_the_startup_outcome() {
    let (params, _stop_startup_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run(params, async |supervisor, _checkpoint| {
        // Ends at once, before any shutdown was requested; the startup
        // then fails on its own account.
        supervisor.spawn("short", async { Ok::<(), Shutdown>(()) });
        shutdown_token.cancelled().await;
        Err::<(), StartupError>(Message("step failed").into())
    })
    .await;

    let Err(Error::Task {
        supervisor_report,
        startup_outcome,
    }) = outcome
    else {
        panic!("{outcome:?}");
    };
    assert!(supervisor_report.any_before_shutdown());
    let startup::Outcome::Failed(error) = startup_outcome else {
        panic!("{startup_outcome:?}");
    };
    assert_eq!(error.to_string(), "step failed");
}

#[tokio::test]
async fn a_step_failing_after_the_cancel_is_still_the_failure() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        checkpoint()?;
        // The stop-startup-then-shutdown token is cancelled while this
        // step runs; the step fails on its own.
        stop_startup_then_shutdown_token.cancel();
        Err::<(), StartupError>(Message("step failed").into())
    })
    .await;

    let Err(Error::Startup {
        error,
        supervisor_report,
    }) = outcome
    else {
        panic!("{outcome:?}");
    };
    assert_eq!(error.to_string(), "step failed");
    assert!(!supervisor_report.any_before_shutdown());
}

#[tokio::test]
async fn a_stopped_startup_requests_the_shutdown() {
    let stop_startup_then_shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let params = Params {
        supervisor: waymark_task_supervisor::start::<Message>(shutdown_token.clone()),
        stop_startup_then_shutdown_token: stop_startup_then_shutdown_token.clone(),
        shutdown_token: shutdown_token.clone(),
    };

    let outcome = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        // Only the stop-startup-then-shutdown token is cancelled; the
        // shutdown is requested once the startup has stopped, so the task
        // ends and the drain does.
        stop_startup_then_shutdown_token.cancel();
        checkpoint()?;
        panic!("the next step must not start");
        #[allow(unreachable_code)]
        Ok::<(), StartupError>(())
    })
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::DuringStartup)),
        "{outcome:?}"
    );
    assert!(shutdown_token.is_cancelled());
}

#[tokio::test]
async fn a_cancel_after_the_startup_is_the_shutdown() {
    let stop_startup_then_shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let params = Params {
        supervisor: waymark_task_supervisor::start::<Message>(shutdown_token.clone()),
        stop_startup_then_shutdown_token: stop_startup_then_shutdown_token.clone(),
        shutdown_token: shutdown_token.clone(),
    };

    // The stop-startup-then-shutdown token is cancelled once the startup
    // is over and the drain is on; the shutdown is requested right away,
    // and the drain ends.
    tokio::spawn({
        let stop_startup_then_shutdown_token = stop_startup_then_shutdown_token.clone();
        async move {
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            stop_startup_then_shutdown_token.cancel();
        }
    });

    let outcome = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        checkpoint()?;
        Ok::<(), StartupError>(())
    })
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::AfterFullLifecycle(()))),
        "{outcome:?}"
    );
    assert!(shutdown_token.is_cancelled());
}

#[tokio::test]
async fn a_shutdown_during_the_startup_lets_it_run_to_its_end() {
    let (params, _stop_startup_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        checkpoint()?;
        // The shutdown, not the lifecycle one, comes between two
        // steps: the checkpoints do not observe it.
        shutdown_token.cancel();
        checkpoint()?;
        Ok::<_, StartupError>(3)
    })
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::AfterFullLifecycle(3))),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_startup_without_checkpoints_runs_to_its_end_and_then_shuts_down() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run(params, async |supervisor, _checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        // The stop-startup-then-shutdown token is cancelled, and no
        // checkpoint observes it: the startup completes and requests the
        // shutdown itself.
        stop_startup_then_shutdown_token.cancel();
        assert!(!shutdown_token.is_cancelled());
        Ok::<_, StartupError>(7)
    })
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::AfterFullLifecycle(7))),
        "{outcome:?}"
    );
    assert!(shutdown_token.is_cancelled());
}

#[test]
fn outcomes_report_exit_codes() {
    use std::process::{ExitCode, Termination as _};

    let success = CleanShutdown::AfterFullLifecycle(()).report();
    let during_startup = CleanShutdown::<()>::DuringStartup.report();
    let failure = CleanShutdown::AfterFullLifecycle(ExitCode::from(3)).report();

    assert_eq!(format!("{success:?}"), format!("{:?}", ExitCode::SUCCESS));
    assert_eq!(
        format!("{during_startup:?}"),
        format!("{:?}", ExitCode::SUCCESS)
    );
    assert_eq!(format!("{failure:?}"), format!("{:?}", ExitCode::from(3)));
}
