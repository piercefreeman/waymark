//! Workload pinning manager routines.

#![warn(missing_docs)]

mod maintenance;
mod outcome;
mod pinned_batch;
mod pinned_handle;
mod poll;

#[cfg(test)]
mod test_utils {
    pub mod helpers;
    pub mod mock;
}

pub use self::maintenance::*;
pub use self::outcome::*;
pub use self::pinned_handle::*;
pub use self::poll::*;

use std::num::NonZeroUsize;
use std::sync::Arc;

use nonempty_collections::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use waymark_nonzero_duration::NonZeroDuration;

/// Parameters for the workload management loop.
pub struct Params<Backend>
where
    Backend: waymark_workload_pinning_backend::HasNodeId,
    Backend: waymark_workload_pinning_backend::HasWorkloadId,
{
    /// Token to signal graceful shutdown of the poll loop.
    ///
    /// Cancelling this token stops accepting new workloads (polling ceases)
    /// while in-flight work is allowed to complete naturally — the
    /// maintenance loop keeps heartbeating and processing evictions
    /// until all active workloads drain.
    ///
    /// For an immediate stop of everything, use
    /// [`force_shutdown_token`](Params::force_shutdown_token).
    pub shutdown_token: CancellationToken,

    /// Token to signal force shutdown of the maintenance loop.
    ///
    /// Cancelling this token stops the maintenance loop immediately
    /// without waiting for in-flight work to drain.
    ///
    /// Use [`shutdown_token`](Params::shutdown_token) to stop accepting new
    /// workloads while letting in-flight work drain naturally.
    pub force_shutdown_token: CancellationToken,

    /// Backend for database operations (poll, pin, unpin).
    pub backend: Arc<Backend>,

    /// The identifier of this node.
    pub node_id: Backend::NodeId,

    /// Channel for sending newly pinned handles to external consumers.
    pub pinned_tx: tokio::sync::mpsc::Sender<NEVec<PinnedHandle<Backend::WorkloadId>>>,

    /// Maximum number of workloads to run concurrently.
    pub max_pinned: NonZeroUsize,

    /// How long a pinning lasts before it needs to be refreshed.
    pub pinning_ttl: NonZeroDuration,

    /// How often to refresh pinnings on active workloads.
    pub pinning_heartbeat: NonZeroDuration,

    /// How much earlier than the pinning ttl the local lapse deadline
    /// falls: a pinning not re-confirmed within `pinning_ttl -
    /// pinning_fencing_margin` of its (monotonic, pre-send) anchor
    /// lapses and the workload is fenced — the margin budgets the
    /// eviction latency between the fence signal and the workload
    /// actually stopping.
    pub pinning_fencing_margin: NonZeroDuration,
}

/// Run the workload management loop.
///
/// Returns a [`RunOutcomeFor`] that preserves the independent results of
/// the poll loop, maintenance loop, and cleanup phase so callers can
/// inspect every stage of the run lifecycle.
pub async fn run<Backend>(params: Params<Backend>) -> RunOutcomeFor<Backend>
where
    Backend:
        waymark_workload_pinning_backend::HasTimestamp<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: waymark_workload_pinning_backend::PollUnpinnedWorkloads,
    Backend: waymark_workload_pinning_backend::KeepalivePinnings,
    Backend: waymark_workload_pinning_backend::UnpinWorkloads,
    Backend: Send + Sync + 'static,
    <Backend as waymark_workload_pinning_backend::PollUnpinnedWorkloads>::Error: std::fmt::Debug,
    <Backend as waymark_workload_pinning_backend::KeepalivePinnings>::Error: std::fmt::Debug,
    <Backend as waymark_workload_pinning_backend::UnpinWorkloads>::Error: std::fmt::Debug,
    Backend::NodeId: Clone,
    Backend::WorkloadId: Clone + std::hash::Hash + Eq + std::fmt::Debug,
{
    let Params {
        shutdown_token,
        force_shutdown_token,
        backend,
        node_id,
        pinned_tx,
        max_pinned,
        pinning_ttl,
        pinning_heartbeat,
        pinning_fencing_margin,
    } = params;

    info!(
        max_pinned = max_pinned.get(),
        pinning_ttl_ms = pinning_ttl.get().as_millis(),
        pinning_heartbeat_ms = pinning_heartbeat.get().as_millis(),
        "workload manager starting"
    );

    let (evict_tx, evict_rx) = tokio::sync::mpsc::unbounded_channel();

    // Poll → Maintain: dispatch newly-pinned IDs with a oneshot to get back the count.
    let (batch_tx, batch_rx) =
        tokio::sync::mpsc::channel::<pinned_batch::PinnedBatch<Backend::WorkloadId>>(1);
    // Maintain → Poll: push updated count after evictions.
    let (count_tx, count_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();

    // Give the poll loop a child token so it can be cancelled internally
    // without affecting the caller's shutdown_token. The child is also
    // cancelled automatically when the caller cancels the parent.
    let poll_shutdown = shutdown_token.child_token();
    drop(shutdown_token);

    let poll_future = poll::run_poll_loop(poll::PollParams {
        backend: Arc::clone(&backend),
        node_id: node_id.clone(),
        pinned_tx,
        evict_tx,
        batch_tx,
        count_rx,
        shutdown_token: poll_shutdown.clone(),
        max_pinned,
        pinning_ttl,
    });

    let maintenance_future = maintenance::run_maintenance_loop(maintenance::MaintainParams {
        backend: Arc::clone(&backend),
        node_id: node_id.clone(),
        batch_rx,
        evict_rx,
        count_tx,
        shutdown_token: force_shutdown_token,
        pinning_heartbeat,
        pinning_ttl,
        pinning_fencing_margin,
    });

    // When the maintenance loop exits — for any reason — cancel the
    // poll loop's child token so it exits promptly. There is no point
    // accepting new work without maintenance.
    // (Poll exits never cancel maintenance; maintenance must always drain.)
    let maintenance_future = async move {
        let _cancel_poll = poll_shutdown.drop_guard();
        maintenance_future.await
    };

    let (poll_result, maintenance_result) = tokio::join!(poll_future, maintenance_future);

    // The maintenance loop returns active IDs alongside any error so
    // they can be released during cleanup.  On a clean exit the set
    // is always empty and there is nothing to release.
    let (maintenance_error, active_ids) = match maintenance_result {
        Ok(()) => (None, None),
        Err((error, ids)) => (Some(error), Some(ids)),
    };

    let mut cleanup_error = None;
    if let Some(ids) = active_ids
        && let Some(ids) = ids.try_into_nonempty_iter()
    {
        debug!("releasing pinnings on shutdown");
        // Cleanup always releases: no park decision was made for these
        // workloads, so they must remain runnable.
        let unpins = ids.map(|id| (id, waymark_workload_pinning_core::UnpinMode::Release));
        if let Err(error) = backend.unpin_workloads(node_id, unpins).await {
            warn!(?error, "failed to unpin workloads during cleanup");
            cleanup_error = Some(error);
        }
    }

    RunOutcome {
        poll_error: poll_result.err(),
        maintenance_error,
        cleanup_error,
    }
}

#[cfg(test)]
mod tests;
