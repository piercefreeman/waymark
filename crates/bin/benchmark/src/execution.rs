//! Execution-subsystem configuration and shutdown for the benchmark.

use std::num::NonZeroUsize;
use std::time::Duration;

use waymark_nonzero_duration::NonZeroDuration;

/// Max snapshots coalesced per batched write (env `WAYMARK_SNAPSHOT_BATCH_MAX`).
fn snapshot_batch_max() -> NonZeroUsize {
    std::env::var("WAYMARK_SNAPSHOT_BATCH_MAX")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(NonZeroUsize::new(256).unwrap())
}

/// Snapshot batch flush window (env `WAYMARK_SNAPSHOT_BATCH_DELAY_MS`).
fn snapshot_batch_delay() -> NonZeroDuration {
    std::env::var("WAYMARK_SNAPSHOT_BATCH_DELAY_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .and_then(NonZeroDuration::from_millis)
        .unwrap_or(NonZeroDuration::from_millis(5).unwrap())
}

/// Max action-call requests coalesced per batched insert (env
/// `WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_MAX`).
fn action_effect_reconciler_request_batch_max() -> NonZeroUsize {
    std::env::var("WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_MAX")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(NonZeroUsize::new(256).unwrap())
}

/// Action-call request batch flush window (env
/// `WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_DELAY_MS`).
fn action_effect_reconciler_request_batch_delay() -> NonZeroDuration {
    std::env::var("WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_DELAY_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .and_then(NonZeroDuration::from_millis)
        .unwrap_or(NonZeroDuration::from_millis(5).unwrap())
}

/// Max workflow terminal outcomes coalesced per batched upsert (env
/// `WAYMARK_WORKFLOW_COMPLETION_BATCH_MAX`).
fn workflow_completion_batch_max() -> NonZeroUsize {
    std::env::var("WAYMARK_WORKFLOW_COMPLETION_BATCH_MAX")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(NonZeroUsize::new(256).unwrap())
}

/// Workflow terminal-outcome batch flush window (env
/// `WAYMARK_WORKFLOW_COMPLETION_BATCH_DELAY_MS`).
fn workflow_completion_batch_delay() -> NonZeroDuration {
    std::env::var("WAYMARK_WORKFLOW_COMPLETION_BATCH_DELAY_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .and_then(NonZeroDuration::from_millis)
        .unwrap_or(NonZeroDuration::from_millis(5).unwrap())
}

/// Max revival-reconcile locks coalesced per batched statement pair (env
/// `WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_MAX`).
fn action_effect_reconciler_lock_batch_max() -> NonZeroUsize {
    std::env::var("WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_MAX")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(NonZeroUsize::new(256).unwrap())
}

/// Revival-reconcile lock batch flush window (env
/// `WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_DELAY_MS`).
fn action_effect_reconciler_lock_batch_delay() -> NonZeroDuration {
    std::env::var("WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_DELAY_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .and_then(NonZeroDuration::from_millis)
        .unwrap_or(NonZeroDuration::from_millis(5).unwrap())
}

pub fn durable_execution_config(
    max_pinned: NonZeroUsize,
) -> waymark_execution_bringup::Config<uuid::Uuid> {
    waymark_execution_bringup::Config {
        node_id: uuid::Uuid::new_v4(),
        action_effect_reconciler_lock_ttl: Duration::from_secs(15).try_into().unwrap(),
        action_effect_reconciler_lock_heartbeat: Duration::from_secs(5).try_into().unwrap(),
        max_pinned,
        pinning_ttl: Duration::from_secs(15).try_into().unwrap(),
        pinning_heartbeat: Duration::from_secs(5).try_into().unwrap(),
        pinning_fencing_margin: Duration::from_secs(1).try_into().unwrap(),
        workload_poll_rate_limit: 1000.try_into().unwrap(),
        snapshot_batch_max: snapshot_batch_max(),
        snapshot_batch_delay: snapshot_batch_delay(),
        action_effect_reconciler_request_batch_max: action_effect_reconciler_request_batch_max(),
        action_effect_reconciler_request_batch_delay: action_effect_reconciler_request_batch_delay(
        ),
        workflow_completion_batch_max: workflow_completion_batch_max(),
        workflow_completion_batch_delay: workflow_completion_batch_delay(),
        action_effect_reconciler_lock_batch_max: action_effect_reconciler_lock_batch_max(),
        action_effect_reconciler_lock_batch_delay: action_effect_reconciler_lock_batch_delay(),
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
        snapshot_batcher,
        action_effect_reconciler_request_batcher,
        workflow_completion_batcher,
        action_effect_reconciler_lock_batcher,
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
    let _ = tokio::time::timeout(Duration::from_secs(5), snapshot_batcher).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        action_effect_reconciler_request_batcher,
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(5), workflow_completion_batcher).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        action_effect_reconciler_lock_batcher,
    )
    .await;
}
