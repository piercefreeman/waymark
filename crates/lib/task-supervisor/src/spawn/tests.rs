use crate::test_helpers::{TaskError, break_now, until_shutdown};
use crate::{BoxedError, start};

#[tokio::test]
async fn blocking_task_is_supervised() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<BoxedError>(shutdown_token.clone());

    supervisor.spawn_blocking("blocking breaker", || {
        Err::<std::convert::Infallible, _>(TaskError::Broke)
    });

    let report = supervisor.drain().await;
    assert!(report.any_before_shutdown());

    assert!(shutdown_token.is_cancelled());
    assert_eq!(report.ended[0].name, "blocking breaker");
    assert!(report.ended[0].before_shutdown);
}

#[tokio::test]
async fn on_variants_spawn_on_the_given_runtime() {
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = start::<BoxedError>(shutdown_token.clone());
    let handle = tokio::runtime::Handle::current();

    supervisor.spawn_on("loop", until_shutdown(shutdown_token.clone()), &handle);
    supervisor.spawn_blocking_on(
        "blocking breaker",
        || Err::<std::convert::Infallible, _>(TaskError::Broke),
        &handle,
    );

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
            let mut supervisor = start::<BoxedError>(shutdown_token.clone());

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
