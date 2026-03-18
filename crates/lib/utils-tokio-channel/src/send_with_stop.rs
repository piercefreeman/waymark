use std::time::Duration;

use waymark_utils_futures::{with_cancellation, with_race_callback};

/// Sends an item into a bounded channel while racing against a cancellation token.
///
/// Tasks already check their cancellation token at the top of each loop iteration and
/// in `select!` branches when awaiting input. However, if shutdown is signaled while a
/// task is blocked on a *full channel send* (because the consumer has already exited or
/// stopped draining), the task would deadlock — it never returns to the loop head to
/// re-check the token. This helper breaks that deadlock by selecting over the send and
/// the cancellation future, guaranteeing the task can exit cleanly. It also warns if the
/// send is pending for more than 2 seconds, surfacing backpressure during normal operation.
#[tracing::instrument(skip_all, fields(kind))]
pub async fn send_with_stop<T, CancellationSignalFut>(
    tx: &tokio::sync::mpsc::Sender<T>,
    item: T,
    stop: CancellationSignalFut,
    #[allow(unused_variables)] kind: &'static str, // used in tracing span
) -> Result<(), Error<T>>
where
    CancellationSignalFut: Future<Output = ()>,
{
    let fut = tx.send(item);

    let fut = std::pin::pin!(fut);
    let fut = with_race_callback(fut, tokio::time::sleep(Duration::from_secs(2)), || {
        tracing::warn!("send pending >2s")
    });

    let fut = std::pin::pin!(fut);
    let fut = with_cancellation(fut, stop);

    let result = fut.await;

    match result {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(send_error)) => {
            tracing::warn!("receiver dropped");
            Err(Error::ReceiverDropped(send_error))
        }
        Err(with_cancellation::CancelledError { .. }) => {
            tracing::info!("sender stop notified during send");
            return Err(Error::Stop);
        }
    }
}

pub enum Error<T> {
    ReceiverDropped(tokio::sync::mpsc::error::SendError<T>),
    Stop,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn send_unblocks_on_stop_notification() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
        tx.send(1).await.expect("seed channel");

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let send_task = tokio::spawn({
            let tx = tx.clone();
            let shutdown_token = shutdown_token.clone();
            async move { send_with_stop(&tx, 2, shutdown_token.cancelled(), "test message").await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        shutdown_token.cancel();
        let sent = tokio::time::timeout(Duration::from_millis(300), send_task)
            .await
            .expect("send task should complete")
            .expect("send task should not panic");
        assert!(
            matches!(sent, Err(Error::Stop)),
            "send should abort when stop is notified"
        );

        let _ = rx.recv().await;
    }

    #[tokio::test]
    async fn send_fails_when_receiver_dropped() {
        let (tx, rx) = tokio::sync::mpsc::channel::<u32>(1);
        drop(rx); // close receiver

        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let sent = send_with_stop(&tx, 99, shutdown_token.cancelled(), "test message").await;
        assert!(
            matches!(sent, Err(Error::ReceiverDropped(_))),
            "send should fail when receiver is dropped"
        );
    }
}
