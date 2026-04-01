use std::time::Duration;
use waymark_timed_channel as timed_channel;

#[tokio::test]
async fn tokio_mpsc_message_age_includes_blocked_send_await_time() {
    let (tx, mut rx) = timed_channel::tokio::mpsc::channel::<u8>(1);

    tx.send(1).await.expect("first send should succeed");

    let send_blocked = tokio::spawn(async move {
        tx.send(2).await.expect("second send should succeed");
    });

    tokio::time::sleep(Duration::from_millis(80)).await;

    let first = rx
        .channel
        .recv()
        .await
        .expect("first opaque message should exist");
    assert_eq!(first.into_inner(), 1);

    send_blocked
        .await
        .expect("blocked sender task should complete");

    let second = rx
        .channel
        .recv()
        .await
        .expect("second opaque message should exist");
    let age = second.since_creation();
    assert_eq!(second.into_inner(), 2);

    assert!(
        age >= Duration::from_millis(70),
        "expected >= 70ms, got {}ms",
        age.as_millis()
    );
}
