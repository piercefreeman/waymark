//! Workload pinning manager routines.

#![warn(missing_docs)]

mod error;
mod maintenance;
mod poll;

#[cfg(test)]
mod tests;

pub use self::error::*;

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use nonempty_collections::{IntoIteratorExt as _, NEVec};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workload_pinning_backend::{
    HasTimestamp, KeepaliveInstancePinnings, PollUnpinnedInstances, ReleasePinnings,
};

/// A handle to a pinned instance.
///
/// When dropped, the instance ID is sent back to the workload manager
/// for unpinning.
#[must_use]
pub struct PinnedHandle<InstanceId> {
    id: Option<InstanceId>,
    evict_tx: mpsc::UnboundedSender<InstanceId>,
}

impl<InstanceId> PinnedHandle<InstanceId> {
    /// Return a reference to the wrapped instance ID.
    pub fn id(&self) -> &InstanceId {
        // `PinnedHandle` is not dropped, so the `id` must be present.
        self.id.as_ref().unwrap()
    }
}

impl<InstanceId> Drop for PinnedHandle<InstanceId> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.evict_tx.send(id);
        }
    }
}

impl<InstanceId: std::fmt::Debug> std::fmt::Debug for PinnedHandle<InstanceId> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedHandle")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Parameters for the workload management loop.
pub struct Params<Backend>
where
    Backend: PollUnpinnedInstances,
{
    /// Token to signal graceful shutdown.
    pub shutdown_token: CancellationToken,

    /// Backend for database operations (poll, pin, unpin).
    pub backend: Arc<Backend>,

    /// The node identifier for this executor instance.
    pub node_id: Backend::NodeId,

    /// Channel for sending newly pinned handles to external consumers.
    pub pinned_tx: mpsc::Sender<NEVec<PinnedHandle<Backend::InstanceId>>>,

    /// Maximum number of workloads to run concurrently.
    pub max_pinned: NonZeroUsize,

    /// How long a pinning lasts before it needs to be refreshed.
    pub pinning_ttl: NonZeroDuration,

    /// How often to refresh pinnings on active workloads.
    pub pinning_heartbeat: NonZeroDuration,
}

/// Run the workload management loop.
pub async fn run<Backend>(
    params: Params<Backend>,
) -> Result<
    (),
    Error<
        <Backend as PollUnpinnedInstances>::Error,
        <Backend as KeepaliveInstancePinnings>::Error,
        <Backend as ReleasePinnings>::Error,
    >,
>
where
    Backend: HasTimestamp<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: PollUnpinnedInstances,
    Backend: KeepaliveInstancePinnings,
    Backend: ReleasePinnings,
    Backend: Send + Sync + 'static,
    <Backend as PollUnpinnedInstances>::Error: std::fmt::Debug,
    <Backend as KeepaliveInstancePinnings>::Error: std::fmt::Debug,
    <Backend as ReleasePinnings>::Error: std::fmt::Debug,
    Backend::NodeId: Clone,
    Backend::InstanceId: Clone + std::hash::Hash + Eq,
{
    let Params {
        shutdown_token,
        backend,
        node_id,
        pinned_tx,
        max_pinned,
        pinning_ttl,
        pinning_heartbeat,
    } = params;

    info!(
        max_pinned = max_pinned.get(),
        pinning_ttl_ms = pinning_ttl.get().as_millis(),
        pinning_heartbeat_ms = pinning_heartbeat.get().as_millis(),
        "workload manager starting"
    );

    let (evict_tx, evict_rx) = mpsc::unbounded_channel();

    // Poll → Maintain: dispatch newly-pinned IDs with a oneshot to get back the count.
    let (batch_tx, batch_rx) = mpsc::channel::<(
        NEVec<Backend::InstanceId>,
        tokio::sync::oneshot::Sender<usize>,
    )>(1);
    // Maintain → Poll: push updated count after evictions.
    let (count_tx, count_rx) = mpsc::unbounded_channel::<usize>();
    // Maintain → run: return remaining IDs for cleanup.
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel::<HashSet<Backend::InstanceId>>();

    let poll_loop = poll::run_poll_loop(poll::PollParams {
        backend: Arc::clone(&backend),
        node_id: node_id.clone(),
        pinned_tx: pinned_tx.clone(),
        evict_tx: evict_tx.clone(),
        batch_tx,
        count_rx,
        shutdown_token: shutdown_token.clone(),
        max_pinned,
        pinning_ttl,
    });

    let maintain_loop = maintenance::run_maintenance_loop(maintenance::MaintainParams {
        backend: Arc::clone(&backend),
        node_id: node_id.clone(),
        batch_rx,
        evict_rx,
        count_tx,
        cleanup_tx,
        shutdown_token: shutdown_token.clone(),
        pinning_heartbeat,
        pinning_ttl,
    });

    let (poll_result, maintain_result) = tokio::join!(poll_loop, maintain_loop);

    let mut result = poll_result;
    if result.is_ok() {
        result = maintain_result;
    }

    // Release remaining pinnings.
    if let Ok(active_ids) = cleanup_rx.await
        && let Some(ids) = active_ids.into_iter().try_into_nonempty_iter()
    {
        debug!("releasing pinnings on shutdown");
        if let Err(error) = backend.release_pinnings(node_id, ids).await {
            warn!(?error, "failed to release pinnings during cleanup");
            if result.is_ok() {
                result = Err(Error::Cleanup(error));
            }
        }
    }

    result
}
