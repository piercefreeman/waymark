//! The worker pool loop's two ends: every requests handle dropped, or the
//! shutdown future resolved.

use std::{num::NonZeroUsize, process::Stdio, sync::Arc, time::Duration};

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

/// The worker's ends of its channels: what a connected worker would hold.
/// Dropped, a dispatch to the worker fails at once; held and never
/// answered, a dispatch stays in flight.
struct WorkerEnds {
    to_worker: tokio::sync::mpsc::Receiver<waymark_proto::messages::Envelope>,
    _from_worker: tokio::sync::mpsc::Sender<waymark_proto::messages::Envelope>,
}

fn make_worker_channels() -> (waymark_worker_message_protocol::Channels, WorkerEnds) {
    let (to_worker_tx, to_worker_rx) = tokio::sync::mpsc::channel(4);
    let (from_worker_tx, from_worker_rx) = tokio::sync::mpsc::channel(4);
    let channels = waymark_worker_message_protocol::Channels {
        to_worker: to_worker_tx,
        from_worker: from_worker_rx,
    };
    let worker_ends = WorkerEnds {
        to_worker: to_worker_rx,
        _from_worker: from_worker_tx,
    };
    (channels, worker_ends)
}

/// A one-worker process pool over the stub command, its worker registered
/// as a connected one would be, and the worker's ends of its channels.
async fn make_pool_with_worker_ends() -> (waymark_worker_process_pool::Pool<DummySpec>, WorkerEnds)
{
    let registry = Arc::new(Registry::default());
    let (reservation_id_tx, mut reservation_id_rx) = tokio::sync::mpsc::unbounded_channel();
    let spec = DummySpec { reservation_id_tx };
    let one = NonZeroUsize::new(1).expect("one is non-zero");

    let pool_init = waymark_worker_process_pool::Pool::new_with_concurrency(
        Arc::clone(&registry),
        spec,
        one,
        None,
        one,
    );
    let registration = async {
        let reservation_id = reservation_id_rx.recv().await.expect("reservation id");
        let (channels, worker_ends) = make_worker_channels();
        let register_result = registry.register(reservation_id, channels);
        assert!(register_result.is_ok(), "register worker channels");
        worker_ends
    };
    let (pool, worker_ends) = tokio::join!(pool_init, registration);

    (pool.expect("initialize worker pool"), worker_ends)
}

/// [`make_pool_with_worker_ends`] with the worker's ends dropped: a
/// dispatch to the worker fails at once.
async fn make_pool() -> waymark_worker_process_pool::Pool<DummySpec> {
    let (pool, worker_ends) = make_pool_with_worker_ends().await;
    drop(worker_ends);
    pool
}

/// The bound every loop end is awaited under: a loop that never ends
/// fails the test instead of hanging it.
const END_BOUND: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn the_loop_ends_once_every_requests_handle_is_dropped() {
    let pool = make_pool().await;
    let (requests, _completions, pool_loop) =
        waymark_worker_remote_pool::run(pool, std::future::pending());

    drop(requests);

    let ended = tokio::time::timeout(END_BOUND, pool_loop)
        .await
        .expect("the loop must end once its last requests handle is dropped");
    assert!(ended.is_ok(), "{ended:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_loop_ends_on_the_shutdown_future_with_a_requests_handle_held() {
    let pool = make_pool().await;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (requests, _completions, pool_loop) = waymark_worker_remote_pool::run(pool, async move {
        let _ = shutdown_rx.await;
    });

    shutdown_tx.send(()).expect("the loop holds the receiver");

    let ended = tokio::time::timeout(END_BOUND, pool_loop)
        .await
        .expect("the loop must end once the shutdown future resolves");
    assert!(ended.is_ok(), "{ended:?}");

    drop(requests);
}

/// The drop-driven end serves the request in flight first: with the
/// worker's ends gone it is served as a loss, and that report is the one
/// completion before the completion queue is shut down.
#[tokio::test(flavor = "multi_thread")]
async fn the_drop_driven_end_serves_the_request_in_flight_first() {
    use waymark_worker_core::{PollActionResults as _, QueueActionDispatch as _};

    let pool = make_pool().await;
    let (requests, completions, pool_loop) =
        waymark_worker_remote_pool::run(pool, std::future::pending());

    requests
        .queue(waymark_proto::messages::ActionDispatch::default())
        .await
        .expect("the queue has room");
    drop(requests);

    let ended = tokio::time::timeout(END_BOUND, pool_loop)
        .await
        .expect("the loop must end once the request in flight is served");
    assert!(ended.is_ok(), "{ended:?}");

    let reports = completions
        .poll_complete()
        .await
        .expect("polling the completions is infallible")
        .expect("the request in flight was served");
    assert_eq!(reports.len().get(), 1);
    assert!(
        matches!(
            reports.first(),
            waymark_worker_core::ActionExecutionReport::Lost(_)
        ),
        "{reports:?}"
    );
    let shut_down = completions
        .poll_complete()
        .await
        .expect("polling the completions is infallible");
    assert!(shut_down.is_none(), "{shut_down:?}");
}

/// The shutdown future abandons the request in flight: the loop ends with
/// the dispatch at the worker unanswered, and no completion for it.
#[tokio::test(flavor = "multi_thread")]
async fn the_shutdown_future_abandons_the_request_in_flight() {
    use waymark_worker_core::{PollActionResults as _, QueueActionDispatch as _};

    let (pool, mut worker_ends) = make_pool_with_worker_ends().await;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (requests, completions, pool_loop) = waymark_worker_remote_pool::run(pool, async move {
        let _ = shutdown_rx.await;
    });
    let pool_loop = tokio::spawn(pool_loop);

    requests
        .queue(waymark_proto::messages::ActionDispatch::default())
        .await
        .expect("the queue has room");

    // In flight for real: the dispatch has reached the worker's end, which
    // never answers it.
    let envelope = tokio::time::timeout(END_BOUND, worker_ends.to_worker.recv())
        .await
        .expect("the dispatch must reach the worker")
        .expect("the worker's sender is held by the pool");
    assert_eq!(
        envelope.kind,
        waymark_proto::messages::MessageKind::ActionDispatch as i32
    );

    shutdown_tx.send(()).expect("the loop holds the receiver");

    let ended = tokio::time::timeout(END_BOUND, pool_loop)
        .await
        .expect("the loop must end once the shutdown future resolves")
        .expect("the loop task must not panic");
    assert!(ended.is_ok(), "{ended:?}");

    let shut_down = completions
        .poll_complete()
        .await
        .expect("polling the completions is infallible");
    assert!(shut_down.is_none(), "{shut_down:?}");
    drop(requests);
    drop(worker_ends);
}
