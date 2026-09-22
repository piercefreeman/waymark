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
/// trace. A backtrace section would come from the environment variables,
/// so the tests refuse to run under them.
fn install_plain_report_hook() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        assert!(
            std::env::var_os("RUST_BACKTRACE").is_none()
                && std::env::var_os("RUST_LIB_BACKTRACE").is_none(),
            "unset RUST_BACKTRACE and RUST_LIB_BACKTRACE: the snapshots have no backtrace section"
        );
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
/// report is what `?` makes of the lifecycle's error.
fn printed_by_main(report: &color_eyre::eyre::Report) -> String {
    format!("Error: {report:?}")
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
    insta::assert_snapshot!(error.to_string(), @"startup failed");
    insta::assert_snapshot!(printed_by_main(&color_eyre::eyre::Report::from(error)), @"
    Error: 
       0: startup failed
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
    insta::assert_snapshot!(error.to_string(), @"a task ended before the shutdown was requested");
    insta::assert_snapshot!(printed_by_main(&color_eyre::eyre::Report::from(error)), @"
    Error: 
       0: a task ended before the shutdown was requested
       1: 1 task(s) ended, 1 of them before shutdown was requested
            short (before shutdown): returned
    ");
}

#[test]
fn a_clean_shutdown_exits_with_success() {
    let exit_code = Ok::<_, color_eyre::eyre::Report>(CleanShutdown::<()>::DuringStartup).report();
    assert_eq!(
        format!("{exit_code:?}"),
        format!("{:?}", std::process::ExitCode::SUCCESS)
    );
}
