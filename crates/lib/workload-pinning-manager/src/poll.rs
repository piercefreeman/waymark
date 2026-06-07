//! Poll loop — polls for unpinned instances and dispatches handles.

use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::Utc;
use nonempty_collections::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_workload_pinning_backend::{HasTimestamp, Pinning, PollUnpinnedInstances};

use crate::{Error, PinnedHandle};

pub(super) struct PollParams<Backend>
where
    Backend: PollUnpinnedInstances,
{
    pub backend: Arc<Backend>,
    pub node_id: Backend::NodeId,
    pub pinned_tx: mpsc::Sender<NEVec<PinnedHandle<Backend::InstanceId>>>,
    pub evict_tx: mpsc::UnboundedSender<Backend::InstanceId>,
    pub batch_tx: mpsc::Sender<(
        NEVec<Backend::InstanceId>,
        tokio::sync::oneshot::Sender<usize>,
    )>,
    pub count_rx: mpsc::UnboundedReceiver<usize>,
    pub shutdown_token: CancellationToken,
    pub max_pinned: NonZeroUsize,
    pub pinning_ttl: NonZeroDuration,
}

pub(super) async fn run_poll_loop<Backend>(
    params: PollParams<Backend>,
) -> Result<
    (),
    Error<
        <Backend as PollUnpinnedInstances>::Error,
        <Backend as waymark_workload_pinning_backend::KeepaliveInstancePinnings>::Error,
        <Backend as waymark_workload_pinning_backend::ReleasePinnings>::Error,
    >,
>
where
    Backend: PollUnpinnedInstances<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: waymark_workload_pinning_backend::KeepaliveInstancePinnings,
    Backend: waymark_workload_pinning_backend::ReleasePinnings,
    Backend: HasTimestamp,
    Backend::NodeId: Clone,
    Backend::InstanceId: Clone + std::hash::Hash + Eq,
{
    let PollParams {
        backend,
        node_id,
        pinned_tx,
        evict_tx,
        batch_tx,
        mut count_rx,
        shutdown_token,
        max_pinned,
        pinning_ttl,
    } = params;

    let mut current_count = 0usize;
    let shutdown = shutdown_token.child_token().cancelled_owned();
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let available = max_pinned.get().saturating_sub(current_count);
        if let Some(max_items) = NonZeroUsize::new(available) {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("poll loop shutting down");
                    break Ok(());
                }
                Some(updated) = count_rx.recv() => {
                    current_count = updated;
                }
                result = poll_and_dispatch(
                    &*backend,
                    node_id.clone(),
                    max_items,
                    pinning_ttl,
                    &batch_tx,
                    &pinned_tx,
                    &evict_tx,
                ) => {
                    match result {
                        Ok(count) => current_count = count,
                        Err(error) => break Err(error),
                    }
                }
            }
        } else {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("poll loop shutting down");
                    break Ok(());
                }
                Some(updated) = count_rx.recv() => {
                    current_count = updated;
                }
            }
        }
    }
}

/// Poll for new workloads and pin them.
///
/// Returns the newly pinned instance IDs.
pub(super) async fn poll_and_pin<Backend>(
    backend: &Backend,
    node_id: Backend::NodeId,
    max_items: NonZeroUsize,
    pinning_ttl: NonZeroDuration,
) -> Result<NEVec<Backend::InstanceId>, <Backend as PollUnpinnedInstances>::Error>
where
    Backend: PollUnpinnedInstances<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend::NodeId: Clone,
{
    let now = Utc::now();

    let expires_at = now
        + chrono::Duration::from_std(pinning_ttl.get())
            .unwrap_or_else(|_| chrono::Duration::seconds(0));

    let pinning = Pinning {
        node_id,
        expires_at,
    };

    backend.poll_unlocked(now, pinning, max_items).await
}

/// Poll, register with the maintain loop, and publish handles.
///
/// Returns the updated active count from the maintain loop.
async fn poll_and_dispatch<Backend>(
    backend: &Backend,
    node_id: Backend::NodeId,
    max_items: NonZeroUsize,
    pinning_ttl: NonZeroDuration,
    batch_tx: &mpsc::Sender<(
        NEVec<Backend::InstanceId>,
        tokio::sync::oneshot::Sender<usize>,
    )>,
    pinned_tx: &mpsc::Sender<NEVec<PinnedHandle<Backend::InstanceId>>>,
    evict_tx: &mpsc::UnboundedSender<Backend::InstanceId>,
) -> Result<
    usize,
    Error<
        <Backend as PollUnpinnedInstances>::Error,
        <Backend as waymark_workload_pinning_backend::KeepaliveInstancePinnings>::Error,
        <Backend as waymark_workload_pinning_backend::ReleasePinnings>::Error,
    >,
>
where
    Backend: PollUnpinnedInstances<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: waymark_workload_pinning_backend::KeepaliveInstancePinnings,
    Backend: waymark_workload_pinning_backend::ReleasePinnings,
    Backend: HasTimestamp,
    Backend::NodeId: Clone,
    Backend::InstanceId: Clone,
{
    let ids = poll_and_pin(backend, node_id, max_items, pinning_ttl)
        .await
        .map_err(Error::Poll)?;
    let handle_ids: Vec<Backend::InstanceId> = ids.iter().cloned().collect();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    batch_tx
        .send((ids, reply_tx))
        .await
        .map_err(|_| Error::PinnedReceiverClosed)?;
    let count = reply_rx.await.map_err(|_| Error::PinnedReceiverClosed)?;
    let handles: NEVec<_> = handle_ids
        .into_iter()
        .try_into_nonempty_iter()
        .expect("non-empty")
        .map(|id| PinnedHandle {
            id: Some(id),
            evict_tx: evict_tx.clone(),
        })
        .collect();
    pinned_tx
        .send(handles)
        .await
        .map_err(|_| Error::PinnedReceiverClosed)?;
    Ok(count)
}
