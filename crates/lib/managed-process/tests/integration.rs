use std::time::Duration;

use waymark_managed_process::{self as managed_process, WaitWithTimeoutError};

fn spawn_sleeping_shell() -> managed_process::Child {
    let mut command = tokio::process::Command::new("sh");
    command.arg("-c").arg("sleep 60");

    managed_process::spawn(command).expect("spawn sleeping shell")
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
    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        // Ignore SIGTERM so `shutdown` must use the kill fallback.
        .arg("trap '' TERM; sleep 60");

    let child = managed_process::spawn(command).expect("spawn term-trapping shell");

    let exit_status = child
        .shutdown(
            Some(Duration::from_millis(25)),
            Some(Duration::from_secs(1)),
        )
        .await
        .expect("shutdown should terminate child");

    assert!(!exit_status.success());
}
