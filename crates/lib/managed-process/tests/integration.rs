use std::time::Duration;

use waymark_managed_process::{self as managed_process, ShutdownOutcome, WaitWithTimeoutError};

fn spawn_sleeping_shell() -> managed_process::Child {
    let mut command = tokio::process::Command::new("sh");
    command.arg("-c").arg("sleep 60");

    managed_process::spawn(command).expect("spawn sleeping shell")
}

/// Spawns a shell that runs `setup` and then `body`, and returns once
/// `setup` has run, so the test's signals reach a shell that is ready for
/// them. The shell reports readiness by creating a marker file.
async fn spawn_shell_after_setup(setup: &str, body: &str) -> managed_process::Child {
    let marker = std::env::temp_dir().join(format!(
        "waymark-managed-process-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&marker);

    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        .arg(format!("{setup}; : > '{}'; {body}", marker.display()));
    let child = managed_process::spawn(command).expect("spawn shell");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "shell did not report readiness in time"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    std::fs::remove_file(&marker).expect("remove readiness marker");

    child
}

#[tokio::test]
async fn wait_with_timeout_reports_timeout_for_running_child() {
    let mut child = spawn_sleeping_shell();

    let result = child.wait_with_timeout(Duration::from_millis(25)).await;
    assert!(matches!(
        result,
        Err(WaitWithTimeoutError::Timeout { elapsed }) if elapsed == Duration::from_millis(25)
    ));

    let _ = child.kill_and_wait(Some(Duration::from_secs(1))).await;
}

#[tokio::test]
async fn kill_and_wait_terminates_running_child() {
    let child = spawn_sleeping_shell();

    let exit_status = child
        .kill_and_wait(Some(Duration::from_secs(1)))
        .await
        .expect("kill and wait should terminate child");

    assert!(!exit_status.success());
}

#[tokio::test]
async fn shutdown_falls_back_to_kill_after_graceful_timeout() {
    // Ignore SIGTERM so `shutdown` must use the kill fallback.
    let child = spawn_shell_after_setup("trap '' TERM", "sleep 60").await;

    let outcome = child
        .shutdown(
            Some(Duration::from_millis(25)),
            Some(Duration::from_secs(1)),
        )
        .await
        .expect("shutdown should terminate child");

    assert!(matches!(outcome, ShutdownOutcome::KillSent(status) if !status.success()));
}

#[tokio::test]
async fn shutdown_reports_a_graceful_exit() {
    // Exit successfully on SIGTERM, so `shutdown` ends in the graceful wait.
    let child = spawn_shell_after_setup("trap 'exit 0' TERM", "while :; do sleep 1; done").await;

    let outcome = child
        .shutdown(Some(Duration::from_secs(5)), Some(Duration::from_secs(1)))
        .await
        .expect("shutdown should stop child");

    assert!(matches!(outcome, ShutdownOutcome::Exited(status) if status.success()));
}
