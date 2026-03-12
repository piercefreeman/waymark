use std::time::Duration;

use tracing::{info, warn};

/// Sends an item into a bounded channel while racing against a cancellation token.
///
/// Tasks already check their cancellation token at the top of each loop iteration and
/// in `select!` branches when awaiting input. However, if shutdown is signaled while a
/// task is blocked on a *full channel send* (because the consumer has already exited or
/// stopped draining), the task would deadlock — it never returns to the loop head to
/// re-check the token. This helper breaks that deadlock by selecting over the send and
/// the cancellation future, guaranteeing the task can exit cleanly. It also warns if the
/// send is pending for more than 2 seconds, surfacing backpressure during normal operation.
pub async fn send_with_stop<T>(
    tx: &tokio::sync::mpsc::Sender<T>,
    item: T,
    stop: tokio_util::sync::WaitForCancellationFuture<'_>,
    kind: &'static str,
) -> bool {
    let send_fut = tx.send(item);
    tokio::pin!(send_fut);

    let mut stop = std::pin::pin!(stop);

    let mut warned = false;
    loop {
        tokio::select! {
            res = &mut send_fut => {
                if res.is_err() {
                    warn!(%kind, "receiver dropped");
                    return false;
                }
                return true;
            }
            _ = &mut stop => {
                info!(%kind, "sender stop notified during send");
                return false;
            }
            _ = tokio::time::sleep(Duration::from_secs(2)), if !warned => {
                warn!(%kind, "send pending >2s");
                warned = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::send_with_stop;
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
        assert!(!sent, "send should abort when stop is notified");

        let _ = rx.recv().await;
    }

    #[tokio::test]
    async fn send_succeeds_when_channel_has_capacity() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let sent = send_with_stop(&tx, 42, shutdown_token.cancelled(), "test message").await;
        assert!(sent);

        let received = rx.recv().await.expect("queued message");
        assert_eq!(received, 42);
    }
}
