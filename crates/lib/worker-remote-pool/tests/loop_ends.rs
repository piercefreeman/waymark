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

fn make_worker_channels() -> waymark_worker_message_protocol::Channels {
    let (to_worker, _) = tokio::sync::mpsc::channel(4);
    let (_, from_worker) = tokio::sync::mpsc::channel(4);
    waymark_worker_message_protocol::Channels {
        to_worker,
        from_worker,
    }
}

/// A one-worker process pool over the stub command, its worker registered
/// as a connected one would be.
async fn make_pool() -> waymark_worker_process_pool::Pool<DummySpec> {
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
        let register_result = registry.register(reservation_id, make_worker_channels());
        assert!(register_result.is_ok(), "register worker channels");
    };
    let (pool, ()) = tokio::join!(pool_init, registration);

    pool.expect("initialize worker pool")
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
