//! Workload pinning manager routines.

#![warn(missing_docs)]

mod maintenance;
mod outcome;
mod pinned_batch;
mod pinned_handle;
mod poll;
mod unpin;

#[cfg(test)]
mod test_utils {
    pub mod helpers;
    pub mod mock;
}

pub use self::maintenance::*;
pub use self::outcome::*;
pub use self::pinned_handle::*;
pub use self::poll::*;

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use nonempty_collections::NEVec;
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

    /// Maximum number of unpinned-workload poll queries per second.
    pub poll_rate_limit: NonZeroU32,

    /// How often to refresh pinnings on active workloads.
    pub pinning_heartbeat: NonZeroDuration,

    /// How long to wait before retrying a failed unpin.
    ///
    /// Together with the loop's failure tolerance this is the budget for
    /// riding out a database blip before the pinnings of evicted
    /// workloads are abandoned and left to lapse.
    pub unpin_retry_interval: NonZeroDuration,

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
/// Runs the poll, maintenance and unpin loops concurrently. Returns a
/// [`RunOutcomeFor`] that preserves each loop's result independently so
/// callers can inspect every stage of the run lifecycle.
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
        poll_rate_limit,
        pinning_heartbeat,
        unpin_retry_interval,
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
    // Maintain → Unpin: the durable unpin of an evicted workload.
    let (unpin_tx, unpin_rx) = tokio::sync::mpsc::unbounded_channel();
    let unpin_tx = unpin::wrap_tx(unpin_tx);

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
        poll_rate_limit,
    });

    let maintenance_future = maintenance::run_maintenance_loop(maintenance::MaintainParams {
        backend: Arc::clone(&backend),
        node_id: node_id.clone(),
        batch_rx,
        evict_rx,
        unpin_tx: unpin_tx.clone(),
        count_tx,
        shutdown_token: force_shutdown_token,
        pinning_heartbeat,
        pinning_ttl,
        pinning_fencing_margin,
    });

    // The unpin loop outlives the maintenance loop by design: it closes
    // only once every sender is gone, so it still drains the evictions —
    // and the cleanup below — that maintenance forwards on its way out.
    let unpin_future = unpin::run_unpin_loop(unpin::UnpinParams {
        backend: Arc::clone(&backend),
        node_id: node_id.clone(),
        unpin_rx,
        retry_interval: unpin_retry_interval,
    });

    // When the maintenance loop exits — for any reason — cancel the
    // poll loop's child token so it exits promptly. There is no point
    // accepting new work without maintenance.
    // (Poll exits never cancel maintenance; maintenance must always drain.)
    //
    // Whatever pinnings it still held are routed into the unpin loop, so
    // cleanup goes through the same retrying machinery as every other
    // unpin.  They are always released: no park decision was made for
    // them, so they must remain runnable.
    let maintenance_future = async move {
        let _cancel_poll = poll_shutdown.drop_guard();
        let unpin_tx = unpin_tx;
        let result = match maintenance_future.await {
            Ok(()) => Ok(()),
            Err((error, ids)) => {
                if !ids.is_empty() {
                    debug!("releasing pinnings on shutdown");
                    for id in ids {
                        unpin_tx.request(id, waymark_workload_pinning_core::UnpinMode::Release);
                    }
                }
                Err(error)
            }
        };

        // Closing the input is what lets the unpin loop drain and exit,
        // so it is done explicitly rather than left to the drop order.
        drop(unpin_tx);
        result
    };

    let (poll_result, maintenance_result, unpin_result) =
        tokio::join!(poll_future, maintenance_future, unpin_future);

    RunOutcome {
        poll_error: poll_result.err(),
        maintenance_error: maintenance_result.err(),
        unpin_error: unpin_result.err(),
    }
}

#[cfg(test)]
mod tests;
