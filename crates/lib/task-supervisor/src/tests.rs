use super::*;
use crate::test_helpers::{
    TaskError, bare_infallible_panic_now, bare_unit_panic_now, break_now, panic_now, until_shutdown,
};

#[tokio::test]
async fn early_end_cancels_the_token_and_fails_the_drain() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("breaker", tokio::spawn(break_now()));

    let report = supervisor.drain().await;
    assert!(report.any_before_shutdown());

    assert!(shutdown_token.is_cancelled());
    assert_eq!(report.ended.len(), 1);
    assert_eq!(report.ended[0].name, "breaker");
    assert!(report.ended[0].before_shutdown);
    assert_eq!(
        report.ended[0].result.as_ref().unwrap_err().to_string(),
        "broke"
    );
}

#[tokio::test]
async fn end_after_shutdown_is_recorded_and_the_drain_succeeds() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("loop", tokio::spawn(until_shutdown(shutdown_token.clone())));

    shutdown_token.cancel();

    let report = supervisor.drain().await;
    assert!(!report.any_before_shutdown());

    assert_eq!(report.ended.len(), 1);
    assert_eq!(report.ended[0].name, "loop");
    assert!(!report.ended[0].before_shutdown);
    assert_eq!(
        report.ended[0].result.as_ref().unwrap_err().to_string(),
        "shutdown observed"
    );
}

#[tokio::test]
async fn panic_is_recorded_under_the_task_name() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("panicker", tokio::spawn(panic_now()));

    let report = supervisor.drain().await;
    assert!(report.any_before_shutdown());

    assert!(shutdown_token.is_cancelled());
    assert_eq!(report.ended.len(), 1);
    assert_eq!(report.ended[0].name, "panicker");
    assert!(report.ended[0].before_shutdown);
    assert!(
        matches!(report.ended[0].result, Err(report::Cause::Join(ref join_error)) if join_error.is_panic()),
        "{:?}",
        report.ended[0].result
    );
}

#[tokio::test]
async fn early_end_of_one_task_shuts_the_others_down() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("loop", tokio::spawn(until_shutdown(shutdown_token.clone())));
    supervisor.track("breaker", tokio::spawn(break_now()));

    let report = supervisor.drain().await;
    assert!(report.any_before_shutdown());

    assert!(shutdown_token.is_cancelled());
    assert_eq!(report.ended.len(), 2);

    let breaker = report
        .ended
        .iter()
        .find(|ended| ended.name == "breaker")
        .unwrap();
    assert!(breaker.before_shutdown);

    let looper = report
        .ended
        .iter()
        .find(|ended| ended.name == "loop")
        .unwrap();
    assert!(!looper.before_shutdown);
}

#[tokio::test]
async fn typed_supervisor_tracks_with_its_error_type() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("loop", tokio::spawn(until_shutdown(shutdown_token.clone())));
    supervisor.track("breaker", tokio::spawn(break_now()));

    let report = supervisor.drain().await;
    assert!(report.any_before_shutdown());

    assert!(shutdown_token.is_cancelled());
    assert_eq!(report.ended.len(), 2);

    let breaker = report
        .ended
        .iter()
        .find(|ended| ended.name == "breaker")
        .unwrap();
    assert!(breaker.before_shutdown);
    assert!(matches!(
        breaker.result,
        Err(report::Cause::Error(TaskError::Broke))
    ));

    let looper = report
        .ended
        .iter()
        .find(|ended| ended.name == "loop")
        .unwrap();
    assert!(!looper.before_shutdown);
    assert!(matches!(
        looper.result,
        Err(report::Cause::Error(TaskError::Shutdown))
    ));
}

#[tokio::test]
async fn unit_return_is_an_end_like_any_other() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("early", tokio::spawn(async { Ok::<(), TaskError>(()) }));

    let report = supervisor.drain().await;

    assert!(shutdown_token.is_cancelled());
    assert!(report.ended[0].before_shutdown);
    assert!(report.ended[0].result.is_ok());
    assert!(
        report
            .to_string()
            .contains("early (before shutdown): returned")
    );
}

#[tokio::test]
async fn bare_unit_return_is_a_return() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("early", tokio::spawn(async {}));

    let report = supervisor.drain().await;

    assert!(shutdown_token.is_cancelled());
    assert!(report.ended[0].before_shutdown);
    assert!(report.ended[0].result.is_ok());
}

#[tokio::test]
async fn bare_unit_panic_is_a_join_end() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("panicker", tokio::spawn(bare_unit_panic_now()));

    let report = supervisor.drain().await;

    assert!(shutdown_token.is_cancelled());
    assert!(report.ended[0].before_shutdown);
    assert!(
        matches!(report.ended[0].result, Err(report::Cause::Join(ref join_error)) if join_error.is_panic()),
        "{:?}",
        report.ended[0].result
    );
}

#[tokio::test]
async fn bare_infallible_panic_is_a_join_end() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<TaskError>(shutdown_token.clone());

    supervisor.track("panicker", tokio::spawn(bare_infallible_panic_now()));

    let report = supervisor.drain().await;

    assert!(shutdown_token.is_cancelled());
    assert!(report.ended[0].before_shutdown);
    assert!(
        matches!(report.ended[0].result, Err(report::Cause::Join(ref join_error)) if join_error.is_panic()),
        "{:?}",
        report.ended[0].result
    );
}
