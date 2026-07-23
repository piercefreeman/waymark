//! Poll loop — polls for unpinned workloads and dispatches handles.

mod error;

pub use self::error::*;

use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::Utc;
use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use tokio_util::sync::CancellationToken;
use tracing::info;
use waymark_nonzero_duration::NonZeroDuration;

use crate::PinnedHandle;
use crate::pinned_batch::PinnedBatch;

pub(super) struct PollParams<Backend>
where
    Backend: waymark_workload_pinning_backend::HasNodeId,
    Backend: waymark_workload_pinning_backend::HasWorkloadId,
{
    pub backend: Arc<Backend>,
    pub node_id: Backend::NodeId,
    pub pinned_tx: tokio::sync::mpsc::Sender<NEVec<PinnedHandle<Backend::WorkloadId>>>,
    pub evict_tx: tokio::sync::mpsc::UnboundedSender<(
        Backend::WorkloadId,
        waymark_workload_pinning_core::UnpinMode,
    )>,
    pub batch_tx: tokio::sync::mpsc::Sender<PinnedBatch<Backend::WorkloadId>>,
    pub count_rx: tokio::sync::mpsc::UnboundedReceiver<usize>,
    pub shutdown_token: CancellationToken,
    pub max_pinned: NonZeroUsize,
    pub pinning_ttl: NonZeroDuration,
    pub poll_interval: NonZeroDuration,
}

pub(super) async fn run_poll_loop<Backend>(
    params: PollParams<Backend>,
) -> Result<(), PollLoopErrorFor<Backend>>
where
    Backend: waymark_workload_pinning_backend::PollUnpinnedWorkloads<
            Timestamp = chrono::DateTime<chrono::Utc>,
        >,
    Backend: waymark_workload_pinning_backend::HasTimestamp,
    Backend::NodeId: Clone,
    Backend::WorkloadId: Clone + std::hash::Hash + Eq,
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
        poll_interval,
    } = params;

    let mut current_count = 0usize;
    let shutdown = shutdown_token.child_token().cancelled_owned();
    let mut shutdown = std::pin::pin!(shutdown);

    // Caps the poll-query frequency: without it an empty poll result loops
    // straight back into the next query, hammering the store whenever every
    // runnable workload is already pinned.
    let mut poll_ticker = tokio::time::interval(poll_interval.get());
    poll_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

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
                _ = poll_ticker.tick() => {
                    // Count updates no longer race a poll that has begun:
                    // once the query pins rows, only shutdown may abandon
                    // it before the pinned batch is handed to maintenance.
                    // The trade: count updates queue while a poll runs, so
                    // a stale count can briefly inflate `available` past
                    // `max_pinned` — accepted as a soft, self-correcting
                    // target.
                    tokio::select! {
                        _ = &mut shutdown => {
                            info!("poll loop shutting down");
                            break Ok(());
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
                                Ok(Some(count)) => current_count = count,
                                Ok(None) => { /* no workloads — count unchanged */ }
                                Err(error) => break Err(error),
                            }
                        }
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
/// Returns the newly pinned workload IDs, or `None` if no workloads were available.
pub(super) async fn poll_and_pin<Backend>(
    backend: &Backend,
    node_id: Backend::NodeId,
    max_items: NonZeroUsize,
    pinning_ttl: NonZeroDuration,
) -> Result<
    Option<NEVec<Backend::WorkloadId>>,
    <Backend as waymark_workload_pinning_backend::PollUnpinnedWorkloads>::Error,
>
where
    Backend: waymark_workload_pinning_backend::PollUnpinnedWorkloads<
            Timestamp = chrono::DateTime<chrono::Utc>,
        >,
{
    let now = Utc::now();

    let expires_at =
        now + chrono::Duration::from_std(pinning_ttl.get()).unwrap_or(chrono::Duration::MAX);

    let pinning = waymark_workload_pinning_backend::Pinning {
        node_id,
        expires_at,
    };

    backend.poll_unpinned(now, pinning, max_items).await
}

/// Poll, register with the maintain loop, and publish handles.
///
/// Returns the updated active count from the maintain loop, or `None` if no
/// workloads were available to dispatch (count is unchanged).
async fn poll_and_dispatch<Backend>(
    backend: &Backend,
    node_id: Backend::NodeId,
    max_items: NonZeroUsize,
    pinning_ttl: NonZeroDuration,
    batch_tx: &tokio::sync::mpsc::Sender<PinnedBatch<Backend::WorkloadId>>,
    pinned_tx: &tokio::sync::mpsc::Sender<NEVec<PinnedHandle<Backend::WorkloadId>>>,
    evict_tx: &tokio::sync::mpsc::UnboundedSender<(
        Backend::WorkloadId,
        waymark_workload_pinning_core::UnpinMode,
    )>,
) -> Result<Option<usize>, PollLoopErrorFor<Backend>>
where
    Backend: waymark_workload_pinning_backend::PollUnpinnedWorkloads<
            Timestamp = chrono::DateTime<chrono::Utc>,
        >,
    Backend: waymark_workload_pinning_backend::HasTimestamp,
    Backend::WorkloadId: Clone,
{
    // The local anchor of these pinnings, captured before the pinning
    // call is sent: the store-side expiry cannot land earlier than
    // `pinned_at + ttl`, so the fence deadline derived from it in the
    // maintenance loop is conservative.
    let pinned_at = tokio::time::Instant::now();
    let ids = poll_and_pin(backend, node_id, max_items, pinning_ttl)
        .await
        .map_err(PollLoopError::Poll)?;

    let Some(ids) = ids else {
        // No workloads to dispatch — count unchanged.
        return Ok(None);
    };

    let pinned: NEVec<(Backend::WorkloadId, CancellationToken)> = ids
        .into_nonempty_iter()
        .map(|id| (id, CancellationToken::new()))
        .collect();

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    batch_tx
        .send(PinnedBatch {
            pinned_at,
            pinned: pinned.clone(),
            reply: reply_tx,
        })
        .await
        .map_err(|_| PollLoopError::MaintenanceClosed)?;

    let count = reply_rx
        .await
        .map_err(|_| PollLoopError::MaintenanceUnresponsive)?;

    let handles: NEVec<_> = pinned
        .into_nonempty_iter()
        .map(|(id, fence)| PinnedHandle::new(id, evict_tx.clone(), fence))
        .collect();
    pinned_tx
        .send(handles)
        .await
        .map_err(|_| PollLoopError::PinnedReceiverClosed)?;

    Ok(Some(count))
}

#[cfg(test)]
mod tests;
