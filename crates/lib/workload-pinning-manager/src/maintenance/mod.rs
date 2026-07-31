//! Maintenance loop — handles evictions, pinnings refresh, batch
//! registration, and the pinning fences.
//!
//! # Exit contract
//!
//! The maintenance loop keeps running as long as there is work to do.
//! Work exists whenever **any** condition holds:
//!
//! - `batch_rx` is still open — the poll loop is alive and may dispatch
//!   newly-pinned workload IDs at any time.
//! - `active` is non-empty — there are in-flight workloads whose
//!   pinnings must be refreshed, whose liveness must be watched, and
//!   whose evictions must be processed.
//!
//! The **terminal state** is reached when **both** conditions are
//! false: `batch_rx` has been closed (the poll loop has exited) **and**
//! `active` is empty (all in-flight work has been evicted). At that
//! point no future work can arrive and the loop exits cleanly.
//!
//! The maintenance loop listens to its own shutdown token for emergency
//! cancellation — this is separate from the poll loop's
//! graceful-shutdown token. Under normal operation the loop exits
//! naturally as above.
//!
//! # Pinning liveness and the fence
//!
//! Each active workload's pinning has a tracked liveness: a lapse
//! deadline on the local monotonic clock — the instant captured
//! **before** the (re-)pinning call was sent, plus the ttl, minus the
//! configured fencing margin. The pre-send anchor makes the local
//! deadline conservative with respect to the store-authoritative
//! expiry regardless of the call's latency. A confirmed refresh pushes
//! the deadline out; liveness ends when the pinning **lapses** (the
//! deadline passes without a confirmed refresh) or is **lost** (a
//! refresh reports it held by another node). Either way the workload
//! is fenced: its fence token is cancelled, signalling the holder of
//! the matching [`PinnedHandle`](crate::PinnedHandle) that the pinning
//! can no longer be proven held. What the holder does about that is its
//! own concern — the loop only raises the signal. A fenced workload
//! stays tracked (though no longer refreshed) until its eviction flows
//! back through the normal channel; the loop's responsibility ends at
//! the signal.
//!
//! The converse is an invariant of
//! [`ActiveSet`](self::active_set::ActiveSet): because tracking is what
//! drives the refresh, a workload that stops being tracked is always
//! fenced on the way out — including when the loop exits with an error
//! and hands the remaining ids to the caller to release. Releasing a
//! pinning whose holder was never told to stop is precisely the
//! double-drive the fence exists to prevent.
//!
//! # Heartbeat retry policy
//!
//! Heartbeat refreshes are critical for keeping pinnings alive.
//! If a refresh fails the loop immediately retries once — this handles
//! transient database errors without waiting for the next tick interval.
//! If the retry also fails the loop exits with [`Error::Refresh`] so the
//! caller can release the pinnings cleanly before they expire.

mod active_set;
mod error;

pub use self::error::*;

use std::collections::HashSet;
use std::sync::Arc;

use nonempty_collections::NEVec;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use waymark_nonzero_duration::NonZeroDuration;

use self::active_set::ActiveSet;
use crate::pinned_batch::PinnedBatch;

pub(super) struct MaintainParams<Backend>
where
    Backend: waymark_workload_pinning_backend::HasNodeId,
    Backend: waymark_workload_pinning_backend::HasWorkloadId,
{
    pub backend: Arc<Backend>,
    pub node_id: Backend::NodeId,
    pub batch_rx: tokio::sync::mpsc::Receiver<PinnedBatch<Backend::WorkloadId>>,
    pub evict_rx: tokio::sync::mpsc::UnboundedReceiver<(
        Backend::WorkloadId,
        waymark_workload_pinning_core::UnpinMode,
    )>,
    pub count_tx: tokio::sync::mpsc::UnboundedSender<usize>,
    pub shutdown_token: CancellationToken,
    pub pinning_heartbeat: NonZeroDuration,
    pub pinning_ttl: NonZeroDuration,
    pub pinning_fencing_margin: NonZeroDuration,
}

pub(super) async fn run_maintenance_loop<Backend>(
    params: MaintainParams<Backend>,
) -> Result<(), (MaintenanceErrorFor<Backend>, HashSet<Backend::WorkloadId>)>
where
    Backend: waymark_workload_pinning_backend::PollUnpinnedWorkloads,
    Backend: waymark_workload_pinning_backend::KeepalivePinnings<
            Timestamp = chrono::DateTime<chrono::Utc>,
        >,
    Backend: waymark_workload_pinning_backend::UnpinWorkloads,
    Backend::NodeId: Clone,
    Backend::WorkloadId: Clone + std::hash::Hash + Eq + std::fmt::Debug,
{
    let MaintainParams {
        backend,
        node_id,
        mut batch_rx,
        mut evict_rx,
        count_tx,
        shutdown_token,
        pinning_heartbeat,
        pinning_ttl,
        pinning_fencing_margin,
    } = params;

    // How long past its pre-send anchor a pinning stays locally
    // trusted.  A margin at or above the ttl degenerates to zero:
    // workloads fence immediately, which such a configuration is
    // asking for.
    let lapse_after = pinning_ttl
        .get()
        .saturating_sub(pinning_fencing_margin.get());

    let mut active: ActiveSet<Backend::WorkloadId> = ActiveSet::new();
    let mut poll_exited = false;
    let shutdown = shutdown_token.child_token().cancelled_owned();
    let mut shutdown = std::pin::pin!(shutdown);
    let mut heartbeat_tick = tokio::time::interval(pinning_heartbeat.get());
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        if poll_exited && active.tracked_count() == 0 {
            break Ok(());
        }

        let next_lapse = active.earliest_lapse_deadline();

        tokio::select! {
            result = batch_rx.recv(), if !poll_exited => {
                match result {
                    Some(PinnedBatch { pinned_at, pinned, reply }) => {
                        let lapses_at = pinned_at + lapse_after;
                        for (id, fence) in pinned {
                            active.track_newly_pinned(id, lapses_at, fence);
                        }
                        let _ = reply.send(active.tracked_count());
                    }
                    None => {
                        poll_exited = true;
                        if active.tracked_count() > 0 {
                            info!("poll loop exited, {} active IDs remain", active.tracked_count());
                        }
                    }
                }
            }
            Some(eviction) = evict_rx.recv() => {
                // Coalesce everything already queued into one batch.
                let mut unpins = NEVec::new(eviction);
                while let Ok(eviction) = evict_rx.try_recv() {
                    unpins.push(eviction);
                }
                if let Err(error) = (*backend).unpin_workloads(node_id.clone(), unpins.clone()).await {
                    break Err((MaintenanceError::Unpin(error), active.fence_all_and_into_ids()));
                }
                for (id, _mode) in unpins {
                    active.fence_and_stop_tracking(&id);
                }
                let _ = count_tx.send(active.tracked_count());
            }
            _ = heartbeat_tick.tick() => {
                // Both nows are captured before issuing the refresh:
                // the store stamps the new expiry after this instant,
                // so `fencing_now + lapse_after` is a conservative
                // local deadline.  The immediate retry re-sends the
                // same refresh, so it reuses them.
                let fencing_now = tokio::time::Instant::now();
                let timestamp_now = chrono::Utc::now();

                // The ids borrow `active`, so the refresh is scoped:
                // applying the statuses below needs it back, mutably.
                let refreshed = {
                    let Some(ids) = active.ids_needing_refresh() else {
                        continue;
                    };
                    match refresh_active_pinnings(&*backend, timestamp_now, node_id.clone(), ids.clone(), pinning_ttl).await {
                        Ok(statuses) => Ok(statuses),
                        Err(error) => {
                            warn!(?error, "heartbeat refresh failed, retrying immediately");
                            // Retry once immediately — must complete before pins expire.
                            refresh_active_pinnings(&*backend, timestamp_now, node_id.clone(), ids, pinning_ttl).await
                        }
                    }
                };

                let statuses = match refreshed {
                    Ok(statuses) => statuses,
                    Err(error) => {
                        break Err((MaintenanceError::Refresh(error), active.fence_all_and_into_ids()));
                    }
                };

                let lapses_at = fencing_now + lapse_after;
                for status in statuses {
                    if status.pinning.is_some() {
                        active.extend_after_confirmed_refresh(&status.workload_id, lapses_at);
                    } else {
                        active.fence_lost_pinning(&status.workload_id);
                    }
                }
            }
            _ = async { tokio::time::sleep_until(next_lapse.unwrap()).await }, if next_lapse.is_some() => {
                active.fence_lapsed_pinnings(tokio::time::Instant::now());
            }
            _ = &mut shutdown => {
                warn!("maintenance loop force shutdown");
                break Err((MaintenanceError::ForceShutdown, active.fence_all_and_into_ids()));
            }
        }
    };

    debug!("maintenance loop exiting");
    result
}

/// Refresh pinnings on all active workloads, returning the per-workload
/// statuses.
pub(super) async fn refresh_active_pinnings<Backend>(
    backend: &Backend,
    now: chrono::DateTime<chrono::Utc>,
    node_id: Backend::NodeId,
    ids: impl nonempty_collections::IntoNonEmptyIterator<Item = Backend::WorkloadId>,
    pinning_ttl: NonZeroDuration,
) -> Result<
    NEVec<waymark_workload_pinning_backend::PinningStatusFor<Backend>>,
    <Backend as waymark_workload_pinning_backend::KeepalivePinnings>::Error,
>
where
    Backend: waymark_workload_pinning_backend::KeepalivePinnings<
            Timestamp = chrono::DateTime<chrono::Utc>,
        >,
{
    let expires_at = now + chrono::Duration::from_std(pinning_ttl.get()).unwrap();

    let pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at,
    };

    let statuses = backend.refresh_pinnings(now, pinning, ids).await?;

    debug!("refreshed workload pinnings");

    Ok(statuses)
}

#[cfg(test)]
mod tests;
