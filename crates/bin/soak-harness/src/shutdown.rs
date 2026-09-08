//! The OS's shutdown requests as the run's tokens.

/// Error from [`managed`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The Ctrl+C listener could not be installed.
    #[error("failed to install the Ctrl+C listener")]
    InstallCtrlC(#[source] waymark_os_shutdown_requests::InstallError),

    /// The termination listener could not be installed.
    #[error("failed to install the termination listener")]
    InstallTermination(#[source] waymark_os_shutdown_requests::InstallError),
}

/// Run `run` with the tokens the OS's shutdown requests cancel, the
/// requests watched for as long as it runs.
///
/// `run` gets `(stop_token, abort_token)`: the first Ctrl+C or termination
/// request cancels `stop_token`, the next Ctrl+C cancels `abort_token`.
pub async fn managed<Run, Fut, Output>(run: Run) -> Result<Output, Error>
where
    Run: FnOnce(tokio_util::sync::CancellationToken, tokio_util::sync::CancellationToken) -> Fut,
    Fut: Future<Output = Output>,
{
    let ctrl_c = waymark_os_shutdown_requests::ctrl_c::install().map_err(Error::InstallCtrlC)?;
    let termination =
        waymark_os_shutdown_requests::termination::install().map_err(Error::InstallTermination)?;

    let stop_token = tokio_util::sync::CancellationToken::new();
    let abort_token = tokio_util::sync::CancellationToken::new();
    let run_over = tokio_util::sync::CancellationToken::new();

    let watcher = tokio::spawn(drive_cancellation_tokens(
        ctrl_c,
        termination,
        stop_token.clone(),
        abort_token.clone(),
        run_over.clone(),
    ));

    let output = run(stop_token, abort_token).await;

    // The run is over, whichever way it ended: nothing is left for the
    // watcher to guard.
    run_over.cancel();
    watcher.await.unwrap();

    Ok(output)
}

/// Turn the OS's shutdown requests into the run's tokens: the first
/// Ctrl+C or termination request cancels `stop_token`, the next Ctrl+C
/// cancels `abort_token`. `run_over` ends the watch at either step with
/// the tokens untouched; any other end cancels the token being guarded.
async fn drive_cancellation_tokens(
    mut ctrl_c: waymark_os_shutdown_requests::ctrl_c::Receiver,
    mut termination: waymark_os_shutdown_requests::termination::Receiver,
    stop_token: tokio_util::sync::CancellationToken,
    abort_token: tokio_util::sync::CancellationToken,
    run_over: tokio_util::sync::CancellationToken,
) {
    {
        let stop_guard = stop_token.drop_guard();
        tokio::select! {
            () = ctrl_c.recv() => {
                tracing::info!(
                    "Ctrl+C received; stopping the soak run; press Ctrl+C again to abort the teardown"
                );
            }
            () = termination.recv() => {
                tracing::info!("termination requested; stopping the soak run");
            }
            () = run_over.cancelled() => {
                stop_guard.disarm();
                return;
            }
        }
    }

    {
        let abort_guard = abort_token.drop_guard();
        tokio::select! {
            () = ctrl_c.recv() => {
                tracing::warn!("Ctrl+C received again; aborting the teardown");
            }
            () = run_over.cancelled() => {
                abort_guard.disarm();
            }
        }
    }
}
