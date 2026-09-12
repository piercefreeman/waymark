use super::*;

use waymark_managed_spawner::Spawn as _;

#[derive(Debug, thiserror::Error)]
#[error("broke")]
struct Broke;

#[tokio::test]
async fn current_runtime_runs_a_task_that_ends_with_an_error() {
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let mut spawner = CurrentRuntime;

    spawner.spawn("breaker", async move {
        let _ = done_tx.send(());
        Err::<std::convert::Infallible, _>(Broke)
    });

    done_rx.await.expect("the task ran");
}

#[tokio::test]
async fn handle_runs_a_unit_task() {
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let mut spawner = Handle(tokio::runtime::Handle::current());

    spawner.spawn("worker", async move {
        let _ = done_tx.send(());
        Ok::<(), Broke>(())
    });

    done_rx.await.expect("the task ran");
}

#[tokio::test]
async fn current_runtime_runs_a_unit_output_task() {
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let mut spawner = CurrentRuntime;

    spawner.spawn("worker", async move {
        let _ = done_tx.send(());
    });

    done_rx.await.expect("the task ran");
}
