//! How the errors read once a `main` returns them: the report [`run`]
//! ends with, printed the way std prints a returned error.

use std::process::Termination as _;

use waymark_fn_main_lifecycle::{CleanShutdown, Params, run};

/// The tests' task error.
#[derive(Debug, thiserror::Error)]
#[error("shutdown observed")]
struct Shutdown;

async fn until_shutdown(
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), Shutdown> {
    shutdown_token.cancelled().await;
    Err(Shutdown)
}

/// The params over fresh tokens; the shutdown token is read back from them.
fn params() -> Params<Shutdown> {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    Params {
        supervisor: waymark_task_supervisor::start(shutdown_token.clone()),
        stop_startup_then_shutdown_token: tokio_util::sync::CancellationToken::new(),
        shutdown_token,
    }
}

/// Install the report hook once, with nothing environment-dependent in
/// its output: no colours, no location, no environment section, no span
/// trace. The backtrace section is suppressed per report in
/// [`printed_by_main`].
fn install_plain_report_hook() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        color_eyre::config::HookBuilder::default()
            .theme(color_eyre::config::Theme::new())
            .display_env_section(false)
            .display_location_section(false)
            .capture_span_trace_by_default(false)
            .install()
            .expect("install the report hook");
    });
}

/// What std prints on stderr for a `main` that returns `Err(report)`; the
/// report is what `?` makes of the lifecycle's error. The backtrace
/// section is suppressed, so the environment variables do not reach the
/// snapshots; trailing spaces are trimmed off every line, so they carry
/// none.
fn printed_by_main(report: color_eyre::eyre::Report) -> String {
    use color_eyre::Section as _;

    let report = report.suppress_backtrace(true);
    let printed = format!("Error: {report:?}");

    printed
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn a_failed_startup_prints_its_own_report() {
    install_plain_report_hook();
    let params = params();
    let shutdown_token = params.shutdown_token.clone();

    let lifecycle = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        checkpoint()?;
        Err::<(), color_eyre::eyre::Report>(
            color_eyre::eyre::Report::msg("connection refused")
                .wrap_err("connect the database")
                .wrap_err("start the backend"),
        )
    })
    .await;

    let error = lifecycle.expect_err("the startup failed");
    insta::assert_snapshot!(error.to_string(), @"startup failed: start the backend");
    insta::assert_snapshot!(printed_by_main(color_eyre::eyre::Report::from(error)), @"
    Error:
       0: startup failed: start the backend
       1: start the backend
       2: connect the database
       3: connection refused
    ");
}

#[tokio::test]
async fn an_early_task_end_prints_the_report() {
    install_plain_report_hook();
    let params = params();
    let shutdown_token = params.shutdown_token.clone();

    let lifecycle = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("short", async { Ok::<(), Shutdown>(()) });
        shutdown_token.cancelled().await;
        checkpoint()?;
        Ok::<(), color_eyre::eyre::Report>(())
    })
    .await;

    let error = lifecycle.expect_err("a task ended early");
    insta::assert_snapshot!(error.to_string(), @"
    a task ended before the shutdown was requested: 1 task(s) ended, 1 of them before shutdown was requested
      short (before shutdown): returned
    ");
    insta::assert_snapshot!(printed_by_main(color_eyre::eyre::Report::from(error)), @"
    Error:
       0: a task ended before the shutdown was requested: 1 task(s) ended, 1 of them before shutdown was requested
            short (before shutdown): returned

       1: 1 task(s) ended, 1 of them before shutdown was requested
            short (before shutdown): returned
    ");
}

#[tokio::test]
async fn a_stop_through_an_eyre_report_is_a_clean_shutdown() {
    install_plain_report_hook();
    let params = params();
    let stop_startup_token = params.stop_startup_then_shutdown_token.clone();
    let shutdown_token = params.shutdown_token.clone();

    // The checkpoint's `Stopped` travels through a `Report`, the binaries'
    // startup error, and is still told apart from a failure.
    let lifecycle = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("loop", until_shutdown(shutdown_token.clone()));
        stop_startup_token.cancel();
        checkpoint()?;
        Ok::<(), color_eyre::eyre::Report>(())
    })
    .await;

    assert!(matches!(lifecycle, Ok(CleanShutdown::DuringStartup)));
}

#[tokio::test]
async fn an_early_task_end_outranks_a_stopped_startup() {
    install_plain_report_hook();
    let params = params();
    let stop_startup_token = params.stop_startup_then_shutdown_token.clone();
    let shutdown_token = params.shutdown_token.clone();

    let lifecycle = run(params, async |supervisor, checkpoint| {
        supervisor.spawn("short", async { Ok::<(), Shutdown>(()) });
        // The early end must be observed before the stop: the supervisor
        // requests the shutdown on it, and only then is the startup stopped.
        shutdown_token.cancelled().await;
        stop_startup_token.cancel();
        checkpoint()?;
        Ok::<(), color_eyre::eyre::Report>(())
    })
    .await;

    let error = lifecycle.expect_err("a task ended early");
    assert!(matches!(
        error,
        waymark_fn_main_lifecycle::Error::Task {
            startup_outcome: waymark_fn_main_lifecycle::startup::Outcome::Stopped,
            ..
        }
    ));
}

#[test]
fn a_clean_shutdown_exits_with_success() {
    let exit_code = Ok::<_, color_eyre::eyre::Report>(CleanShutdown::<()>::DuringStartup).report();
    assert_eq!(
        format!("{exit_code:?}"),
        format!("{:?}", std::process::ExitCode::SUCCESS)
    );
}
