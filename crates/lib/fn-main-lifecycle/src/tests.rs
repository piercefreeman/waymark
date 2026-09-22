use super::*;
use crate::test_helpers::{MessageError, ShutdownError, StartupError, until_shutdown};

/// A supervised task that requests the stop, then waits for the shutdown
/// token so its end comes after the shutdown request.
async fn stop_then_until_shutdown(
    stop_startup_then_shutdown_token: tokio_util::sync::CancellationToken,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), ShutdownError> {
    stop_startup_then_shutdown_token.cancel();
    shutdown_token.cancelled().await;
    Err(ShutdownError)
}

/// The params over a fresh stop token, and that token: cancelling it
/// stops the startup, and the shutdown follows.
fn setup() -> (Params<MessageError>, tokio_util::sync::CancellationToken) {
    let stop_startup_then_shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let params = Params {
        supervisor: waymark_task_supervisor::start(shutdown_token.clone()),
        stop_startup_then_shutdown_token: stop_startup_then_shutdown_token.clone(),
        shutdown_token,
    };
    (params, stop_startup_then_shutdown_token)
}

/// The lifecycle run under a bound: a lifecycle that never finishes, in
/// its startup or its drain, fails the test instead of hanging it.
async fn run_bounded<StartupOk, StartupError>(
    params: Params<MessageError>,
    startup_fn: impl AsyncFnOnce(
        &mut waymark_task_supervisor::Supervisor<MessageError>,
        startup::CheckpointFn<'_>,
    ) -> Result<StartupOk, StartupError>,
) -> Result<CleanShutdown<StartupOk>, Error<StartupOk, StartupError::Error, MessageError>>
where
    StartupError: startup::Error,
    StartupError: waymark_error_coercion::IntoError,
    StartupError: std::fmt::Debug,
{
    tokio::time::timeout(std::time::Duration::from_secs(5), run(params, startup_fn))
        .await
        .expect("the lifecycle must finish within the bound")
}

#[tokio::test]
async fn completes_and_drains() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        checkpoint()?;
        // The work is done: cancel the stop-startup-then-shutdown token,
        // then return.
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

    let outcome = run_bounded(params, async |supervisor, _checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        Err::<(), StartupError>(MessageError("step failed").into())
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
async fn a_panicking_startup_requests_the_shutdown_on_the_way_out() {
    let (params, _stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    // The panic unwinds through the lifecycle: no drain, no result, only
    // the guard on the way out.
    let lifecycle = run(
        params,
        async |_supervisor, _checkpoint| -> Result<(), StartupError> { panic!("a step panicked") },
    );
    let unwound = {
        use futures_util::FutureExt as _;
        std::panic::AssertUnwindSafe(lifecycle).catch_unwind().await
    };

    assert!(unwound.is_err());
    assert!(shutdown_token.is_cancelled());
}

#[tokio::test]
async fn a_checkpoint_after_the_cancel_stops_the_startup() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(
        params,
        async |supervisor, checkpoint| -> Result<(), StartupError> {
            supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
            checkpoint()?;
            // The stop-startup-then-shutdown token is cancelled between two
            // steps.
            stop_startup_then_shutdown_token.cancel();
            checkpoint()?;
            panic!("the next step must not start")
        },
    )
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::DuringStartup)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn an_early_task_end_is_the_result_over_the_startup_outcome() {
    let (params, _stop_startup_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(params, async |supervisor, checkpoint| {
        // Ends at once, before any shutdown was requested.
        supervisor.spawn("short", async { Ok::<(), ShutdownError>(()) });
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

    let outcome = run_bounded(params, async |supervisor, _checkpoint| {
        // Ends at once, before any shutdown was requested; the startup
        // then fails on its own account.
        supervisor.spawn("short", async { Ok::<(), ShutdownError>(()) });
        shutdown_token.cancelled().await;
        Err::<(), StartupError>(MessageError("step failed").into())
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

    let outcome = run_bounded(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        checkpoint()?;
        // The stop-startup-then-shutdown token is cancelled while this
        // step runs; the step fails on its own.
        stop_startup_then_shutdown_token.cancel();
        Err::<(), StartupError>(MessageError("step failed").into())
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
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(
        params,
        async |supervisor, checkpoint| -> Result<(), StartupError> {
            supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
            // Only the stop-startup-then-shutdown token is cancelled; the
            // shutdown is requested once the startup has stopped, so the task
            // finishes and the drain does.
            stop_startup_then_shutdown_token.cancel();
            checkpoint()?;
            panic!("the next step must not start")
        },
    )
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::DuringStartup)),
        "{outcome:?}"
    );
    assert!(shutdown_token.is_cancelled());
}

#[tokio::test]
async fn a_stop_requests_the_shutdown_before_the_startup_drops_what_it_holds() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(
        params,
        async |supervisor, checkpoint| -> Result<(), StartupError> {
            // A task that ends once the startup drops its handle, the way a
            // batcher ends on its last handle; it observes no token.
            let (handle, until_handle_dropped) = tokio::sync::oneshot::channel::<()>();
            supervisor.spawn("last handle", async move {
                let _ = until_handle_dropped.await;
                Ok::<(), MessageError>(())
            });
            checkpoint()?;

            stop_startup_then_shutdown_token.cancel();

            // The stop requests the shutdown at the checkpoint, so when the
            // handle drops with this closure the task's end is after it.
            let stopped = checkpoint();
            assert!(shutdown_token.is_cancelled());

            drop(handle);
            stopped?;
            panic!("the next step must not start")
        },
    )
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::DuringStartup)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_cancel_after_the_startup_is_the_shutdown() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    // The stop-startup-then-shutdown token is cancelled once the startup
    // is over and the drain is on; the shutdown is requested right away,
    // and the drain finishes.
    tokio::spawn({
        let stop_startup_then_shutdown_token = stop_startup_then_shutdown_token.clone();
        async move {
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            stop_startup_then_shutdown_token.cancel();
        }
    });

    let outcome = run_bounded(params, async |supervisor, checkpoint| {
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

/// A supervised task requesting the stop during the startup: the startup
/// stops at its next checkpoint, and the task's end, after the shutdown
/// request it waited for, is not an early end.
#[tokio::test]
async fn a_supervised_stop_during_the_startup_is_a_clean_shutdown() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(params, async |supervisor, checkpoint| {
        supervisor.spawn(
            "canceller",
            stop_then_until_shutdown(
                stop_startup_then_shutdown_token.clone(),
                shutdown_token.clone(),
            ),
        );
        stop_startup_then_shutdown_token.cancelled().await;
        checkpoint()?;
        Ok::<(), StartupError>(())
    })
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::DuringStartup)),
        "{outcome:?}"
    );
}

/// A supervised task requesting the stop after the startup: the shutdown
/// follows right away, and the task's end, after it, is not an early end.
#[tokio::test]
async fn a_supervised_stop_after_the_startup_is_a_clean_shutdown() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(params, async |supervisor, checkpoint| {
        supervisor.spawn("canceller", {
            let stop_startup_then_shutdown_token = stop_startup_then_shutdown_token.clone();
            let shutdown_token = shutdown_token.clone();
            async move {
                // Past the startup: it has no checkpoint after this spawn.
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                stop_then_until_shutdown(stop_startup_then_shutdown_token, shutdown_token).await
            }
        });
        checkpoint()?;
        Ok::<_, StartupError>(4)
    })
    .await;

    assert!(
        matches!(outcome, Ok(CleanShutdown::AfterFullLifecycle(4))),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_shutdown_during_the_startup_lets_it_return() {
    let (params, _stop_startup_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(params, async |supervisor, checkpoint| {
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
async fn a_startup_without_checkpoints_returns_and_then_shuts_down() {
    let (params, stop_startup_then_shutdown_token) = setup();
    let shutdown_token = params.shutdown_token.clone();

    let outcome = run_bounded(params, async |supervisor, _checkpoint| {
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
