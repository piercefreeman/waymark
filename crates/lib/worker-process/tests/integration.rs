use std::{sync::Arc, time::Duration};

use waymark_worker_message_protocol::Channels;
use waymark_worker_process::{Handle, ShutdownParams, SpawnError, SpawnParams, spawn};
use waymark_worker_reservation::Registry;

fn sleeping_command() -> tokio::process::Command {
    let mut command = tokio::process::Command::new("sh");
    command.arg("-c").arg("sleep 60");
    command
}

async fn spawn_test_worker(
    command: tokio::process::Command,
    tasks_graceful_shutdown_timeout: Duration,
) -> Handle {
    let registry = Arc::new(Registry::<Channels>::default());
    let reservation = registry.reserve();
    let reservation_id = reservation.id();

    let spawn_task = tokio::spawn(spawn(
        reservation,
        SpawnParams {
            command,
            wait_for_playload_timeout: Duration::from_secs(1),
            shutdown_params: ShutdownParams {
                tasks_graceful_shutdown_timeout,
                process_graceful_shutdown_timeout: Duration::from_millis(50),
                process_kill_timeout: Duration::from_secs(1),
            },
        },
    ));

    let (to_worker, _to_worker_rx) = tokio::sync::mpsc::channel(1);
    let (_from_worker_tx, from_worker) = tokio::sync::mpsc::channel(1);

    let registration_result = registry.register(
        reservation_id,
        Channels {
            to_worker,
            from_worker,
        },
    );
    assert!(
        registration_result.is_ok(),
        "reservation registration should succeed"
    );

    let (handle, _sender) = spawn_task
        .await
        .expect("spawn task should not panic")
        .expect("worker spawn should succeed");

    handle
}

#[tokio::test]
async fn spawn_returns_spawn_error_for_missing_command() {
    let registry = Arc::new(Registry::<Channels>::default());
    let reservation = registry.reserve();

    let command = tokio::process::Command::new("/definitely/missing/waymark-worker-process-test");

    let result = spawn(
        reservation,
        SpawnParams {
            command,
            wait_for_playload_timeout: Duration::from_millis(25),
            shutdown_params: ShutdownParams {
                tasks_graceful_shutdown_timeout: Duration::from_millis(25),
                process_graceful_shutdown_timeout: Duration::from_millis(25),
                process_kill_timeout: Duration::from_millis(25),
            },
        },
    )
    .await;

    assert!(
        matches!(result, Err(SpawnError::Spawn(_))),
        "missing executable should return SpawnError::Spawn"
    );
}

#[tokio::test]
async fn spawn_returns_timeout_when_worker_never_registers() {
    let registry = Arc::new(Registry::<Channels>::default());
    let reservation = registry.reserve();

    let result = tokio::time::timeout(
        Duration::from_millis(200),
        spawn(
            reservation,
            SpawnParams {
                command: sleeping_command(),
                wait_for_playload_timeout: Duration::from_millis(25),
                shutdown_params: ShutdownParams {
                    tasks_graceful_shutdown_timeout: Duration::from_millis(25),
                    process_graceful_shutdown_timeout: Duration::from_millis(25),
                    process_kill_timeout: Duration::from_secs(1),
                },
            },
        ),
    )
    .await
    .expect("spawn should time out instead of hanging");

    assert!(
        matches!(
            result,
            Err(SpawnError::ReservationWaitTimeout { timeout })
                if timeout == Duration::from_millis(25)
        ),
        "missing worker registration should return SpawnError::ReservationWaitTimeout"
    );
}

#[tokio::test]
async fn shutdown_notifies_associated_tasks_via_cancellation_signal() {
    let mut handle = spawn_test_worker(sleeping_command(), Duration::from_millis(50)).await;
    let signal = handle.task_shutdown_signal();
    let (tx, rx) = tokio::sync::oneshot::channel();

    handle.spawn_associated_task(async move {
        signal.await;
        tx.send(()).expect("test receiver should still be alive");
    });

    handle.shutdown().await.expect("shutdown should succeed");

    tokio::time::timeout(Duration::from_millis(100), rx)
        .await
        .expect("associated task should observe shutdown")
        .expect("associated task should send completion signal");
}

#[tokio::test]
async fn shutdown_waits_for_cooperative_associated_task_cleanup() {
    let mut handle = spawn_test_worker(sleeping_command(), Duration::from_millis(50)).await;
    let signal = handle.task_shutdown_signal();
    let (tx, rx) = tokio::sync::oneshot::channel();

    handle.spawn_associated_task(async move {
        signal.await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        tx.send(()).expect("test receiver should still be alive");
    });

    handle.shutdown().await.expect("shutdown should succeed");

    rx.await
        .expect("shutdown should wait for cooperative task cleanup");
}

#[tokio::test]
async fn shutdown_aborts_non_cooperative_tasks_after_timeout() {
    let mut handle = spawn_test_worker(sleeping_command(), Duration::from_millis(20)).await;

    handle.spawn_associated_task(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });

    tokio::time::timeout(Duration::from_millis(200), handle.shutdown())
        .await
        .expect("shutdown should complete after aborting stuck tasks")
        .expect("shutdown should succeed");
}

#[tokio::test]
async fn shutdown_falls_back_to_kill_when_child_ignores_sigterm() {
    let mut command = tokio::process::Command::new("sh");
    command.arg("-c").arg("trap '' TERM; sleep 60");

    let handle = spawn_test_worker(command, Duration::from_millis(25)).await;

    tokio::time::timeout(Duration::from_secs(2), handle.shutdown())
        .await
        .expect("shutdown should finish after kill fallback")
        .expect("shutdown should succeed after kill fallback");
}

#[tokio::test]
async fn shutdown_propagates_panic_from_associated_task() {
    let mut handle = spawn_test_worker(sleeping_command(), Duration::from_millis(50)).await;

    handle.spawn_associated_task(async move {
        panic!("associated task panicked");
    });
    tokio::task::yield_now().await;

    let join_error = tokio::spawn(handle.shutdown())
        .await
        .expect_err("shutdown should propagate the task panic");

    assert!(join_error.is_panic(), "expected a panic JoinError");

    let panic_message = join_error
        .into_panic()
        .downcast_ref::<&str>()
        .copied()
        .expect("panic payload should be a &str");

    assert_eq!(panic_message, "associated task panicked");
}
