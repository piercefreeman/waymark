//! Unpin loop — applies the durable unpins of evicted workloads.
//!
//! The loop runs alongside the poll and maintenance loops rather than
//! inside them: unpinning is a database round-trip, and doing it on the
//! maintenance loop would stall the heartbeat behind it. A stalled
//! heartbeat means pinnings lapse, which would fence healthy workloads
//! for no reason other than a slow unpin.
//!
//! # Exit contract
//!
//! The loop keeps running while either its input is open — the
//! maintenance loop may still forward evictions — or something is still
//! pending. It exits cleanly once the input has closed **and** every
//! unpin has landed.
//!
//! Because the maintenance loop routes the pinnings it still holds into
//! this loop before dropping its sender, the final cleanup goes through
//! the same machinery as every other unpin, retries included. This is
//! the only place that calls
//! [`UnpinWorkloads`](waymark_workload_pinning_backend::UnpinWorkloads).
//!
//! # Retry policy
//!
//! A failed unpin is not fatal: the batch stays queued and is retried on
//! the retry interval, so a transient database error does not abandon
//! the pinnings. Only after [`MAX_UNPIN_FAILURES`] consecutive failures
//! does the loop give up and return the error — the pinnings it could
//! not release are left to lapse on their own.

mod sender;

use std::collections::HashMap;
use std::sync::Arc;

use nonempty_collections::NEVec;
use tracing::{debug, warn};
use waymark_nonzero_duration::NonZeroDuration;

pub use self::sender::{UnpinSender, wrap_tx};

/// How many consecutive unpin failures the loop tolerates — keeping the
/// batch queued and retrying — before giving up.
const MAX_UNPIN_FAILURES: usize = 5;

pub(super) struct UnpinParams<Backend>
where
    Backend: waymark_workload_pinning_backend::HasNodeId,
    Backend: waymark_workload_pinning_backend::HasWorkloadId,
{
    pub backend: Arc<Backend>,
    pub node_id: Backend::NodeId,
    pub unpin_rx: tokio::sync::mpsc::UnboundedReceiver<(
        Backend::WorkloadId,
        waymark_workload_pinning_core::UnpinMode,
    )>,
    pub retry_interval: NonZeroDuration,
}

pub(super) async fn run_unpin_loop<Backend>(
    params: UnpinParams<Backend>,
) -> Result<(), <Backend as waymark_workload_pinning_backend::UnpinWorkloads>::Error>
where
    Backend: waymark_workload_pinning_backend::UnpinWorkloads,
    Backend::NodeId: Clone,
    Backend::WorkloadId: Clone + std::hash::Hash + Eq,
{
    let UnpinParams {
        backend,
        node_id,
        mut unpin_rx,
        retry_interval,
    } = params;

    // Keyed by workload id: a later eviction supersedes one still
    // queued for the same workload, since it carries the newer decision
    // about how that pinning should end.
    let mut pending: HashMap<Backend::WorkloadId, waymark_workload_pinning_core::UnpinMode> =
        HashMap::new();
    let mut failures = 0usize;
    let mut retry_at: Option<tokio::time::Instant> = None;
    let mut input_closed = false;

    let result = loop {
        if input_closed && pending.is_empty() {
            break Ok(());
        }

        tokio::select! {
            eviction = unpin_rx.recv(), if !input_closed => {
                match eviction {
                    Some(eviction) => {
                        // Coalesce everything already queued into one batch.
                        pending.insert(eviction.0, eviction.1);
                        while let Ok((id, mode)) = unpin_rx.try_recv() {
                            pending.insert(id, mode);
                        }
                    }
                    None => {
                        input_closed = true;
                        if !pending.is_empty() {
                            debug!(
                                "eviction input closed, {} unpins still pending",
                                pending.len()
                            );
                        }
                        continue;
                    }
                }
            }
            _ = async { tokio::time::sleep_until(retry_at.unwrap()).await }, if retry_at.is_some() => {}
        }

        if let Some(error) =
            flush_pending_unpins(&*backend, node_id.clone(), &mut pending, &mut failures).await
        {
            warn!(
                abandoned = pending.len(),
                "giving up on pending unpins; leaving those pinnings to lapse"
            );
            break Err(error);
        }

        // A still-pending batch means the flush failed; come back for it.
        retry_at =
            (!pending.is_empty()).then(|| tokio::time::Instant::now() + retry_interval.get());
    };

    debug!("unpin loop exiting");
    result
}

/// Attempt to unpin every pending workload in one batch.
///
/// On success the landed entries are dropped and the failure counter
/// reset. On failure the batch stays queued for a later retry and the
/// counter is bumped; once it reaches [`MAX_UNPIN_FAILURES`] the error is
/// returned so the caller can give up.
async fn flush_pending_unpins<Backend>(
    backend: &Backend,
    node_id: Backend::NodeId,
    pending: &mut HashMap<Backend::WorkloadId, waymark_workload_pinning_core::UnpinMode>,
    failures: &mut usize,
) -> Option<<Backend as waymark_workload_pinning_backend::UnpinWorkloads>::Error>
where
    Backend: waymark_workload_pinning_backend::UnpinWorkloads,
    Backend::WorkloadId: Clone + std::hash::Hash + Eq,
{
    // Nothing pending is not a failure — there is simply no batch.
    let batch = NEVec::try_from_vec(
        pending
            .iter()
            .map(|(id, mode)| (id.clone(), *mode))
            .collect(),
    )?;

    match backend.unpin_workloads(node_id, batch).await {
        Ok(()) => {
            // The loop records evictions only between flushes, never
            // during one, so everything queued is what just landed.
            pending.clear();
            *failures = 0;
            debug!("unpinned workloads");
            None
        }
        Err(error) => {
            // The batch stays queued for the next attempt.
            *failures += 1;
            warn!(
                ?error,
                failures = *failures,
                "unpin failed; keeping the batch queued for retry"
            );
            (*failures >= MAX_UNPIN_FAILURES).then_some(error)
        }
    }
}

#[cfg(test)]
mod tests;
