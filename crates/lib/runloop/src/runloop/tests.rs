use crate::channel_utils::send_with_stop;

use super::*;

fn default_test_config(lock_uuid: Uuid) -> RunLoopConfig {
    RunLoopConfig {
        max_concurrent_instances: 25,
        executor_shards: 1,
        instance_done_batch_size: None,
        poll_interval: Duration::from_millis(10),
        persistence_interval: Duration::from_millis(10),
        lock_uuid,
        lock_ttl: Duration::from_secs(15),
        lock_heartbeat: Duration::from_secs(5),
        evict_sleep_threshold: Duration::from_secs(10),
        skip_sleep: false,
        active_instance_gauge: None,
    }
}

#[tokio::test]
async fn test_instance_poller_send_succeeds_when_channel_has_capacity() {
    let (instance_tx, mut instance_rx) = mpsc::channel::<InstanceMessage>(1);
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let sent = send_with_stop(
        &instance_tx,
        InstanceMessage::Batch {
            instances: Vec::new(),
        },
        shutdown_token.cancelled(),
        "instance message",
    )
    .await;
    assert!(sent);

    let received = instance_rx.recv().await.expect("queued message");
    match received {
        InstanceMessage::Batch { instances } => assert!(instances.is_empty()),
        InstanceMessage::Error(err) => panic!("unexpected error message: {err}"),
    }
}

#[tokio::test]
async fn test_instance_poller_send_unblocks_on_stop_notification() {
    let (instance_tx, mut instance_rx) = mpsc::channel::<InstanceMessage>(1);
    instance_tx
        .send(InstanceMessage::Batch {
            instances: Vec::new(),
        })
        .await
        .expect("seed channel");

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let send_task = tokio::spawn({
        let instance_tx = instance_tx.clone();
        let shutdown_token = shutdown_token.clone();
        async move {
            send_with_stop(
                &instance_tx,
                InstanceMessage::Batch {
                    instances: Vec::new(),
                },
                shutdown_token.cancelled(),
                "instance message",
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    shutdown_token.cancel();
    let sent = tokio::time::timeout(Duration::from_millis(300), send_task)
        .await
        .expect("send task should complete")
        .expect("send task should not panic");
    assert!(!sent, "send should abort when stop is notified");

    let _ = instance_rx.recv().await;
}

#[test]
fn test_lock_mismatches_ignores_expired_lock_with_matching_owner() {
    let backend = waymark_backend_memory::MemoryBackend::new();
    let worker_pool = waymark_worker_inline::InlineWorkerPool::new(HashMap::new());
    let lock_uuid = Uuid::new_v4();
    let runloop = RunLoop::new(worker_pool, backend, default_test_config(lock_uuid));

    let instance_id = Uuid::new_v4();
    let statuses = vec![InstanceLockStatus {
        instance_id,
        lock_uuid: Some(lock_uuid),
        lock_expires_at: Some(Utc::now() - chrono::Duration::seconds(60)),
    }];
    assert!(
        runloop.lock_mismatches(&statuses).is_empty(),
        "matching lock UUID should not evict solely due to stale expiry"
    );

    let mismatched = vec![InstanceLockStatus {
        instance_id,
        lock_uuid: Some(Uuid::new_v4()),
        lock_expires_at: Some(Utc::now() + chrono::Duration::seconds(60)),
    }];
    let evict_ids = runloop.lock_mismatches(&mismatched);
    assert_eq!(evict_ids, HashSet::from([instance_id]));
}
