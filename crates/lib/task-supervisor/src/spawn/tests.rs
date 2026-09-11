use crate::start;
use crate::test_helpers::{AnyError, TaskError, break_now, until_shutdown};

#[tokio::test]
async fn blocking_task_is_supervised() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<AnyError>(shutdown_token.clone());

    supervisor
        .spawn_blocking("blocking breaker", || {
            Err::<std::convert::Infallible, _>(TaskError::Broke)
        })
        .expect("spawn the blocking task");

    let report = supervisor.drain().await;
    assert!(report.any_before_shutdown());

    assert!(shutdown_token.is_cancelled());
    assert_eq!(report.ended[0].name, "blocking breaker");
    assert!(report.ended[0].before_shutdown);
}

#[tokio::test]
async fn a_refused_blocking_spawn_is_the_callers_error() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<AnyError>(shutdown_token.clone());
    // A runtime that is shutting down refuses to spawn on its blocking
    // pool; its handle outlives the shutdown.
    let refusing_runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build a runtime");
    let handle = refusing_runtime.handle().clone();
    refusing_runtime.shutdown_background();

    let spawned = supervisor.spawn_blocking_on("refused", || Ok::<(), TaskError>(()), &handle);
    assert!(spawned.is_err(), "{spawned:?}");

    // Nothing was spawned, so nothing is supervised: the drain is empty
    // and the shutdown was never requested.
    let report = supervisor.drain().await;
    assert!(report.ended.is_empty(), "{report}");
    assert!(!shutdown_token.is_cancelled());
}

#[tokio::test]
async fn on_variants_spawn_and_are_supervised() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<AnyError>(shutdown_token.clone());
    let handle = tokio::runtime::Handle::current();

    supervisor.spawn_on("loop", until_shutdown(shutdown_token.clone()), &handle);
    supervisor
        .spawn_blocking_on(
            "blocking breaker",
            || Err::<std::convert::Infallible, _>(TaskError::Broke),
            &handle,
        )
        .expect("spawn the blocking task");

    let report = supervisor.drain().await;
    assert!(report.any_before_shutdown());

    assert!(shutdown_token.is_cancelled());
    assert_eq!(report.ended.len(), 2);
}

#[tokio::test]
async fn local_task_is_supervised() {
    let local_set = tokio::task::LocalSet::new();

    local_set
        .run_until(async {
            let shutdown_token = tokio_util::sync::CancellationToken::new();
            let mut supervisor = start::<AnyError>(shutdown_token.clone());

            let not_send = std::rc::Rc::new(());
            supervisor.spawn_local("local breaker", async move {
                let _held = not_send;
                break_now().await
            });

            let report = supervisor.drain().await;
            assert!(report.any_before_shutdown());

            assert!(shutdown_token.is_cancelled());
            assert_eq!(report.ended[0].name, "local breaker");
        })
        .await;
}
