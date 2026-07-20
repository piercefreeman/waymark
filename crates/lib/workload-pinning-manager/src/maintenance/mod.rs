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
//! # Heartbeat retry policy
//!
//! Heartbeat refreshes are critical for keeping pinnings alive.
//! If a refresh fails the loop immediately retries once — this handles
//! transient database errors without waiting for the next tick interval.
//! If the retry also fails the loop exits with [`Error::Refresh`] so the
//! caller can release the pinnings cleanly before they expire.

mod error;

pub use self::error::*;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nonempty_collections::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use waymark_nonzero_duration::NonZeroDuration;

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

/// The tracked liveness of one active workload's pinning.
struct PinningLiveness {
    /// The local monotonic deadline by which a refresh must confirm the
    /// pinning, or it lapses.
    lapses_at: tokio::time::Instant,

    /// Cancelled when the pinning lapses or is lost — the fence: the
    /// matching [`PinnedHandle`](crate::PinnedHandle) holds a clone.
    fence: CancellationToken,
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

    let mut active: HashMap<Backend::WorkloadId, PinningLiveness> = HashMap::new();
    let mut poll_exited = false;
    let shutdown = shutdown_token.child_token().cancelled_owned();
    let mut shutdown = std::pin::pin!(shutdown);
    let mut heartbeat_tick = tokio::time::interval(pinning_heartbeat.get());
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        if poll_exited && active.is_empty() {
            break Ok(());
        }

        // The earliest lapse deadline still standing; already-fenced
        // workloads are excluded — their signal has fired and only the
        // eviction flow removes them.
        let next_lapse = active
            .values()
            .filter(|state| !state.fence.is_cancelled())
            .map(|state| state.lapses_at)
            .min();

        tokio::select! {
            result = batch_rx.recv(), if !poll_exited => {
                match result {
                    Some(PinnedBatch { pinned_at, pinned, reply }) => {
                        let lapses_at = pinned_at + lapse_after;
                        for (id, fence) in pinned {
                            active.insert(id, PinningLiveness { lapses_at, fence });
                        }
                        let _ = reply.send(active.len());
                    }
                    None => {
                        poll_exited = true;
                        if !active.is_empty() {
                            info!("poll loop exited, {} active IDs remain", active.len());
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
                    break Err((MaintenanceError::Unpin(error), active.into_keys().collect()));
                }
                for (id, _mode) in unpins {
                    active.remove(&id);
                }
                let _ = count_tx.send(active.len());
            }
            _ = heartbeat_tick.tick() => {
                // Refresh every pinning still standing — a fenced
                // workload's pinning is deliberately left to lapse.
                let Some(ids) = active
                    .iter()
                    .filter(|(_, state)| !state.fence.is_cancelled())
                    .map(|(id, _)| id.clone())
                    .try_into_nonempty_iter()
                else {
                    continue;
                };
                let ids: NEVec<_> = ids.collect();

                // Both nows are captured before issuing the refresh:
                // the store stamps the new expiry after this instant,
                // so `fencing_now + lapse_after` is a conservative
                // local deadline.  The immediate retry re-sends the
                // same refresh, so it reuses them.
                let fencing_now = tokio::time::Instant::now();
                let timestamp_now = chrono::Utc::now();
                let statuses = match refresh_active_pinnings(&*backend, timestamp_now, node_id.clone(), ids.clone(), pinning_ttl).await {
                    Ok(statuses) => statuses,
                    Err(error) => {
                        warn!(?error, "heartbeat refresh failed, retrying immediately");
                        // Retry once immediately — must complete before pins expire.
                        match refresh_active_pinnings(&*backend, timestamp_now, node_id.clone(), ids, pinning_ttl).await {
                            Ok(statuses) => statuses,
                            Err(error) => {
                                break Err((MaintenanceError::Refresh(error), active.into_keys().collect()));
                            }
                        }
                    }
                };

                {
                    let lapses_at = fencing_now + lapse_after;
                    for status in statuses {
                        let Some(state) = active.get_mut(&status.workload_id) else {
                            // Evicted while the refresh was in flight.
                            continue;
                        };
                        if status.pinning.is_some() {
                            state.lapses_at = lapses_at;
                        } else if !state.fence.is_cancelled() {
                            warn!(
                                workload_id = ?status.workload_id,
                                "pinning lost to another node; fencing workload"
                            );
                            state.fence.cancel();
                        }
                    }
                }
            }
            _ = async { tokio::time::sleep_until(next_lapse.unwrap()).await }, if next_lapse.is_some() => {
                let now = tokio::time::Instant::now();
                for (id, state) in active.iter() {
                    if !state.fence.is_cancelled() && state.lapses_at <= now {
                        warn!(
                            workload_id = ?id,
                            "pinning lapsed without a confirmed refresh; fencing workload"
                        );
                        state.fence.cancel();
                    }
                }
            }
            _ = &mut shutdown => {
                warn!("maintenance loop force shutdown");
                break Err((MaintenanceError::ForceShutdown, active.into_keys().collect()));
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
