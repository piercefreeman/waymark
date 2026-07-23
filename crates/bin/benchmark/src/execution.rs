//! Execution-subsystem configuration and shutdown for the benchmark.

use std::num::NonZeroUsize;
use std::time::Duration;

pub fn durable_execution_config(
    max_pinned: NonZeroUsize,
) -> waymark_execution_bringup::Config<uuid::Uuid> {
    waymark_execution_bringup::Config {
        node_id: uuid::Uuid::new_v4(),
        // Generous lock/pinning windows: at benchmark load the renewal and
        // refresh heartbeats lag behind their default 15s TTLs, and a lapsed
        // lock gets relocked at revive — the renewal loop then breaches its
        // fence (`HeldElsewhere`) and shuts the whole subsystem down.
        action_effect_reconciler_lock_ttl: Duration::from_secs(120).try_into().unwrap(),
        action_effect_reconciler_lock_heartbeat: Duration::from_secs(15).try_into().unwrap(),
        max_pinned,
        pinning_ttl: Duration::from_secs(120).try_into().unwrap(),
        pinning_heartbeat: Duration::from_secs(15).try_into().unwrap(),
        pinning_fencing_margin: Duration::from_secs(5).try_into().unwrap(),
        sleep_poll_interval: Duration::from_millis(250).try_into().unwrap(),
        vm_retention: Duration::from_secs(60).try_into().unwrap(),
        vm_sweep_interval: Duration::from_secs(10).try_into().unwrap(),
        executable_retention: Duration::from_secs(300).try_into().unwrap(),
        executable_sweep_interval: Duration::from_secs(60).try_into().unwrap(),
    }
}

pub async fn shutdown_execution(handles: waymark_execution_bringup::Handles) {
    let waymark_execution_bringup::Handles {
        pinning_manager,
        execution_driver,
        executable_sweeper,
        vm_sweeper,
        durable_action_completions_writer,
        durable_action_completions_poller,
        durable_action_completions_acker,
        durable_sleeps_poller,
        durable_sleeps_acker,
        action_effect_reconciler_lock_renewal,
    } = handles;

    let _ = tokio::time::timeout(Duration::from_secs(5), pinning_manager).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), execution_driver).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), executable_sweeper).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), vm_sweeper).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_writer).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_poller).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_action_completions_acker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_sleeps_poller).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), durable_sleeps_acker).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        action_effect_reconciler_lock_renewal,
    )
    .await;
}
