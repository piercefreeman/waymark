use super::*;

#[tokio::test]
async fn a_shutdown_requested_some_other_way_cancels_nothing() {
    let receivers = install().expect("install the listeners");
    let stop_startup_then_shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = tokio_util::sync::CancellationToken::new();

    shutdown_token.cancel();

    let Ok(()) = listen(
        receivers,
        stop_startup_then_shutdown_token.clone(),
        shutdown_token,
    )
    .await;

    assert!(!stop_startup_then_shutdown_token.is_cancelled());
}
