use std::future::poll_fn;
use std::pin::Pin;
use std::time::Duration;

use futures_core::Stream as _;

use super::*;

/// An [`ExecuteStream`] over `tasks`, with the sender of its output
/// channel and the token its drop guard cancels.
fn stream(
    tasks: tokio::task::JoinSet<()>,
) -> (
    ExecuteStream,
    mpsc::Sender<Result<proto::WorkflowStreamResponse, Status>>,
    tokio_util::sync::CancellationToken,
) {
    let (out_tx, out_rx) = mpsc::channel(1);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let stream = ExecuteStream {
        out_rx,
        tasks,
        _cancel_driver: cancellation.clone().drop_guard(),
    };

    (stream, out_tx, cancellation)
}

#[tokio::test]
async fn a_panicking_task_ends_the_stream_with_an_internal_status() {
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async { panic!("the task's own panic") });
    let (mut stream, _out_tx, _cancellation) = stream(tasks);

    let item = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await;

    let status = item.expect("an item").expect_err("an error item");
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(
        status.message().starts_with("execution task failed: "),
        "{}",
        status.message()
    );
}

#[tokio::test]
async fn dropping_the_stream_aborts_its_tasks_and_cancels_the_driver() {
    // The sender goes away with the task's future; the receiver sees it.
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        let _dropped_tx = dropped_tx;
        std::future::pending::<()>().await;
    });
    let (stream, _out_tx, cancellation) = stream(tasks);

    drop(stream);

    assert!(cancellation.is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), dropped_rx)
        .await
        .expect("the task is aborted with the stream")
        .expect_err("the sender is dropped with the task");
}
