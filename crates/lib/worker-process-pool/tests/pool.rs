use std::{
    num::{NonZeroU64, NonZeroUsize},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use waymark_worker_process_pool::Pool;
use waymark_worker_status_core::WorkerPoolStats;

type Registry = waymark_worker_reservation::Registry<waymark_worker_message_protocol::Channels>;

#[derive(Debug)]
struct DummySpec {
    reservation_id_tx: tokio::sync::mpsc::UnboundedSender<waymark_worker_reservation::Id>,
}

impl waymark_worker_process_spec::Spec for DummySpec {
    fn prepare_spawn_params(
        &self,
        reservation_id: waymark_worker_reservation::Id,
    ) -> waymark_worker_process::SpawnParams {
        self.reservation_id_tx
            .send(reservation_id)
            .expect("send reservation id");

        waymark_worker_process::SpawnParams {
            command: make_stub_command(),
            wait_for_playload_timeout: Duration::from_secs(5),
            shutdown_params: waymark_worker_process::ShutdownParams {
                tasks_graceful_shutdown_timeout: Duration::from_secs(1),
                process_graceful_shutdown_timeout: Duration::from_secs(1),
                process_kill_timeout: Duration::from_secs(1),
            },
        }
    }
}

fn make_stub_command() -> tokio::process::Command {
    let (program, args) = cfg_select! {
        windows => ("cmd", ["/C", "timeout", "/T", "60", "/NOBREAK"]),
        _ =>  ("sleep", ["60"]),
    };

    let mut command = tokio::process::Command::new(program);
    command.args(args);
    command.stdin(Stdio::null());
    command.stdout(Stdio::null());
    command.stderr(Stdio::null());
    command
}

fn make_worker_channels() -> waymark_worker_message_protocol::Channels {
    let (to_worker, _) = tokio::sync::mpsc::channel(4);
    let (_, from_worker) = tokio::sync::mpsc::channel(4);
    waymark_worker_message_protocol::Channels {
        to_worker,
        from_worker,
    }
}

async fn make_pool(
    worker_count: usize,
    max_concurrent_per_worker: usize,
    max_action_lifecycle: Option<u64>,
) -> Pool<DummySpec> {
    let registry = Arc::new(Registry::default());
    let (reservation_id_tx, mut reservation_id_rx) = tokio::sync::mpsc::unbounded_channel();
    let spec = DummySpec { reservation_id_tx };
    let worker_count_non_zero =
        NonZeroUsize::new(worker_count).expect("worker count must be non-zero");
    let max_concurrent_per_worker = NonZeroUsize::new(max_concurrent_per_worker)
        .expect("max concurrent per worker must be non-zero");

    let register_registry = Arc::clone(&registry);
    let register_task = tokio::spawn(async move {
        for _ in 0..worker_count {
            let reservation_id = reservation_id_rx.recv().await.expect("reservation id");
            let register_result =
                register_registry.register(reservation_id, make_worker_channels());
            assert!(register_result.is_ok(), "register worker channels");
        }
    });

    let pool = Pool::new_with_concurrency(
        registry,
        spec,
        worker_count_non_zero,
        max_action_lifecycle
            .map(|value| NonZeroU64::new(value).expect("max action lifecycle must be non-zero")),
        max_concurrent_per_worker,
    )
    .await
    .expect("initialize worker pool");

    register_task.await.expect("registration task");
    pool
}

#[tokio::test(flavor = "multi_thread")]
async fn try_acquire_slot_for_worker_enforces_capacity_and_wraps_indices() {
    let pool = make_pool(2, 2, None).await;

    assert_eq!(pool.len().get(), 2);
    assert_eq!(pool.max_concurrent_per_worker().get(), 2);
    assert_eq!(pool.total_capacity().get(), 4);
    assert!(pool.try_acquire_slot_for_worker(0));
    assert!(pool.try_acquire_slot_for_worker(0));
    assert!(!pool.try_acquire_slot_for_worker(0));
    assert!(pool.try_acquire_slot_for_worker(3));

    assert_eq!(pool.in_flight_for_worker(0), 2);
    assert_eq!(pool.in_flight_for_worker(1), 1);
    assert_eq!(pool.total_in_flight(), 3);
    assert_eq!(pool.available_capacity(), 1);

    pool.release_slot(2);

    assert_eq!(pool.in_flight_for_worker(0), 1);
    assert_eq!(pool.available_capacity(), 2);

    pool.shutdown().await.expect("shutdown pool");
}

#[tokio::test(flavor = "multi_thread")]
async fn try_acquire_slot_uses_round_robin_and_skips_saturated_workers() {
    let pool = make_pool(2, 1, None).await;

    assert!(pool.try_acquire_slot_for_worker(0));
    assert_eq!(pool.try_acquire_slot(), Some(1));
    assert_eq!(pool.try_acquire_slot(), None);

    pool.release_slot(0);

    assert_eq!(pool.try_acquire_slot(), Some(0));
    assert_eq!(pool.total_in_flight(), 2);

    pool.shutdown().await.expect("shutdown pool");
}

#[tokio::test(flavor = "multi_thread")]
async fn release_slot_saturates_at_zero() {
    let pool = make_pool(1, 2, None).await;

    assert!(pool.try_acquire_slot_for_worker(0));
    pool.release_slot(0);
    pool.release_slot(0);

    assert_eq!(pool.in_flight_for_worker(0), 0);
    assert_eq!(pool.total_in_flight(), 0);
    assert_eq!(pool.available_capacity(), 2);

    pool.shutdown().await.expect("shutdown pool");
}

#[tokio::test(flavor = "multi_thread")]
async fn record_completion_updates_public_metrics_and_stats() {
    let pool = Arc::new(make_pool(2, 2, None).await);

    assert!(pool.try_acquire_slot_for_worker(1));
    pool.record_latency(Duration::from_millis(15), Duration::from_millis(25));
    pool.record_completion(1, Arc::clone(&pool));

    assert_eq!(pool.in_flight_for_worker(1), 0);

    let snapshots = pool.throughput_snapshots();
    assert_eq!(snapshots.len(), 2);

    let worker_snapshot = snapshots
        .iter()
        .find(|snapshot| snapshot.worker_id == 1)
        .expect("worker 1 snapshot");
    assert_eq!(worker_snapshot.total_completed, 1);
    assert!(worker_snapshot.throughput_per_min > 0.0);
    assert!(worker_snapshot.last_action_at.is_some());

    let stats = pool.stats_snapshot();
    assert_eq!(stats.active_workers, 2);
    assert!(stats.throughput_per_min > 0.0);
    assert_eq!(stats.total_completed, 1);
    assert!(stats.last_action_at.is_some());
    assert_eq!(stats.dispatch_queue_size, 0);
    assert_eq!(stats.total_in_flight, 0);
    assert_eq!(stats.median_dequeue_ms, Some(15));
    assert_eq!(stats.median_handling_ms, Some(25));

    pool.shutdown_arc().await.expect("shutdown pool");
}
