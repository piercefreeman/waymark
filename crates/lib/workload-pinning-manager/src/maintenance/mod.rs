//! Maintenance loop — handles evictions, pinnings refresh, and batch registration.
//!
//! # Exit contract
//!
//! The maintenance loop keeps running as long as there is work to do.
//! Work exists whenever **either** condition holds:
//!
//! - `batch_rx` is still open — the poll loop is alive and may dispatch
//!   newly-pinned workload IDs at any time.
//! - `active_ids` is non-empty — there are in-flight workloads whose
//!   pinnings must be refreshed and whose evictions must be processed.
//!
//! The **terminal state** is reached when **both** conditions are false:
//! `batch_rx` has been closed (the poll loop has exited) **and**
//! `active_ids` is empty (all in-flight work has been evicted). At that
//! point no future work can arrive and the loop exits cleanly.
//!
//! On exit (whether clean or errored) the remaining `active_ids` set is
//! sent through `cleanup_tx` so the caller can release any pinnings
//! that were still held.
//!
//! The maintenance loop listens to its own shutdown token for emergency
//! cancellation — this is separate from the poll loop's graceful-shutdown
//! token. Under normal operation the loop exits naturally when the poll
//! loop has stopped and all active IDs have been evicted.
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

use std::collections::HashSet;
use std::sync::Arc;

use nonempty_collections::{IntoIteratorExt as _, NEVec};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use waymark_nonzero_duration::NonZeroDuration;

pub(super) struct MaintainParams<Backend>
where
    Backend: waymark_workload_pinning_backend::HasNodeId,
    Backend: waymark_workload_pinning_backend::HasWorkloadId,
{
    pub backend: Arc<Backend>,
    pub node_id: Backend::NodeId,
    pub batch_rx: tokio::sync::mpsc::Receiver<(
        NEVec<Backend::WorkloadId>,
        tokio::sync::oneshot::Sender<usize>,
    )>,
    pub evict_rx: tokio::sync::mpsc::UnboundedReceiver<Backend::WorkloadId>,
    pub count_tx: tokio::sync::mpsc::UnboundedSender<usize>,
    pub shutdown_token: CancellationToken,
    pub pinning_heartbeat: NonZeroDuration,
    pub pinning_ttl: NonZeroDuration,
}

pub(super) async fn run_maintenance_loop<Backend>(
    params: MaintainParams<Backend>,
) -> Result<(), (MaintenanceErrorFor<Backend>, HashSet<Backend::WorkloadId>)>
where
    Backend: waymark_workload_pinning_backend::PollUnpinnedWorkloads,
    Backend: waymark_workload_pinning_backend::KeepalivePinnings<
            Timestamp = chrono::DateTime<chrono::Utc>,
        >,
    Backend: waymark_workload_pinning_backend::ReleasePinnings,
    Backend::NodeId: Clone,
    Backend::WorkloadId: Clone + std::hash::Hash + Eq,
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
    } = params;

    let mut active_ids: HashSet<Backend::WorkloadId> = HashSet::new();
    let mut poll_exited = false;
    let shutdown = shutdown_token.child_token().cancelled_owned();
    let mut shutdown = std::pin::pin!(shutdown);
    let mut heartbeat_tick = tokio::time::interval(pinning_heartbeat.get());
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        if poll_exited && active_ids.is_empty() {
            break Ok(());
        }

        tokio::select! {
            result = batch_rx.recv(), if !poll_exited => {
                match result {
                    Some((ids, reply)) => {
                        for id in ids.iter() {
                            active_ids.insert(id.clone());
                        }
                        let _ = reply.send(active_ids.len());
                    }
                    None => {
                        poll_exited = true;
                        if !active_ids.is_empty() {
                            info!("poll loop exited, {} active IDs remain", active_ids.len());
                        }
                    }
                }
            }
            Some(id) = evict_rx.recv() => {
                if let Err(error) = (*backend).release_pinnings(node_id.clone(), NEVec::new(id.clone())).await {
                    break Err((MaintenanceError::Release(error), active_ids));
                }
                active_ids.remove(&id);
                let _ = count_tx.send(active_ids.len());
            }
            _ = heartbeat_tick.tick() => {
                let Some(ids) = active_ids.iter().cloned().try_into_nonempty_iter() else {
                    continue;
                };
                match refresh_active_pinnings(&*backend, chrono::Utc::now(), node_id.clone(), ids.clone(), pinning_ttl).await {
                    Ok(()) => {}
                    Err(error) => {
                        warn!(?error, "heartbeat refresh failed, retrying immediately");
                        // Retry once immediately — must complete before pins expire.
                        if let Err(error) = refresh_active_pinnings(&*backend, chrono::Utc::now(), node_id.clone(), ids, pinning_ttl).await {
                            break Err((MaintenanceError::Refresh(error), active_ids));
                        }
                    }
                }
            }
            _ = &mut shutdown => {
                warn!("maintenance loop force shutdown");
                break Err((MaintenanceError::ForceShutdown, active_ids));
            }
        }
    };

    debug!("maintenance loop exiting");
    result
}

/// Refresh pinnings on all active workloads.
pub(super) async fn refresh_active_pinnings<Backend>(
    backend: &Backend,
    now: chrono::DateTime<chrono::Utc>,
    node_id: Backend::NodeId,
    ids: impl nonempty_collections::IntoNonEmptyIterator<Item = Backend::WorkloadId>,
    pinning_ttl: NonZeroDuration,
) -> Result<(), <Backend as waymark_workload_pinning_backend::KeepalivePinnings>::Error>
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

    backend.refresh_pinnings(now, pinning, ids).await?;

    debug!("refreshed workload pinnings");

    Ok(())
}

#[cfg(test)]
mod tests;
